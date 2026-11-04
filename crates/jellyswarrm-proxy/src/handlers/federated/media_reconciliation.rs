use axum::Json;
use hyper::StatusCode;
use std::collections::BTreeSet;
use tracing::error;

use crate::{
    media_catalog::{MediaDedupPlan, TaggedMediaItem},
    media_identity::{MediaAlias, MediaKind, MediaObservation},
    media_storage_service::MediaCatalogSnapshot,
    request_preprocessing::PreprocessedRequest,
    AppState,
};

use super::{
    finalize_items_response,
    library_resolution::CatalogFetchTarget,
    postprocessing::{FederatedItems, Pagination, ServerItems},
    request_policy::{is_authoritative_media_inventory_request, watch_feed, WatchFeed},
    upstream::{
        estimate_merged_library_total, fetch_catalog, fetch_show_catalog, FetchMode, FetchedCatalog,
    },
};

async fn season_catalog_aliases(
    state: &AppState,
    items: &[crate::models::MediaItem],
    viewer: &str,
    series_group: Option<&str>,
) -> Result<Vec<BTreeSet<MediaAlias>>, StatusCode> {
    let mut parents = std::collections::HashMap::new();
    let mut result = Vec::with_capacity(items.len());
    for item in items {
        let mut aliases = MediaAlias::from_item(item);
        if item.item_type == crate::models::enums::BaseItemKind::Season {
            let group = if let Some(group) = series_group {
                Some(group.to_string())
            } else if let Some(parent) = item.series_id.as_ref().or(item.parent_id.as_ref()) {
                if !parents.contains_key(parent) {
                    let group = state
                        .media_storage
                        .get_media_parent_group_id(parent, viewer)
                        .await
                        .map_err(|error| {
                            error!("Failed to resolve season parent identity: {error}");
                            StatusCode::INTERNAL_SERVER_ERROR
                        })?;
                    parents.insert(parent.clone(), group);
                }
                parents[parent].clone()
            } else {
                None
            };
            if let Some(alias) = group
                .as_deref()
                .and_then(|group| MediaAlias::for_season(item, group))
            {
                // Some backends reuse the show's IDs on every season. Once
                // the parent and number are known, do not bridge different
                // season numbers via those unreliable IDs.
                aliases = BTreeSet::from([alias]);
            }
        }
        result.push(aliases);
    }
    Ok(result)
}

pub(super) async fn get_virtual_library_items(
    state: &AppState,
    preprocessed: PreprocessedRequest,
    catalog_scope_key: String,
    targets: Vec<CatalogFetchTarget>,
    skipped_targets: usize,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let deduplicate_media = state.deduplicate_media_enabled().await;
    let reconciliation_generation = if deduplicate_media {
        Some(
            state
                .media_storage
                .begin_media_reconciliation()
                .await
                .map_err(|error| {
                    error!("Failed to begin media reconciliation: {error}");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?,
        )
    } else {
        None
    };
    let original_request = preprocessed.original_request;
    let pagination = Pagination::from_url(original_request.url());
    let FetchedCatalog {
        server_items,
        failures,
        response_shape,
    } = fetch_catalog(
        state,
        &original_request,
        targets,
        FetchMode::VirtualLibrary { pagination },
        skipped_targets,
    )
    .await?;

    let mut upstream_total_sum = 0i32;
    let mut all_fully_fetched = true;
    let authoritative_inventory = is_authoritative_media_inventory_request(original_request.url());
    let mut snapshots = Vec::new();
    let mut tagged_items = Vec::new();
    let viewer = preprocessed
        .access_scope
        .as_ref()
        .map(|scope| scope.user_id().to_string())
        .unwrap_or_else(|| "anonymous".to_string());
    let mut catalog_aliases = Vec::new();
    // A cold recursive listing can contain both the parent series and its
    // seasons. Publish provider-matched parents before deriving season aliases,
    // rather than requiring another request to establish the parent identity.
    // This additive pass shares the request generation and never prunes an
    // inventory; the full snapshots below remain authoritative for removals.
    if deduplicate_media
        && server_items.iter().any(|fetch| {
            fetch
                .server_items
                .response
                .items()
                .iter()
                .any(|item| item.item_type == crate::models::enums::BaseItemKind::Season)
        })
    {
        let parents = server_items
            .iter()
            .map(|fetch| MediaCatalogSnapshot {
                source_key: format!(
                    "{}:{}",
                    fetch.server_items.server.id,
                    fetch.source_parent_id.as_deref().unwrap_or_default()
                ),
                server_id: fetch.server_items.server.id,
                complete: false,
                observations: fetch
                    .server_items
                    .response
                    .items()
                    .iter()
                    .filter(|item| item.item_type == crate::models::enums::BaseItemKind::Series)
                    .map(|item| MediaObservation {
                        virtual_media_id: item.id.clone(),
                        aliases: MediaAlias::from_item(item),
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        if parents
            .iter()
            .any(|snapshot| !snapshot.observations.is_empty())
        {
            state
                .media_storage
                .reconcile_media_catalog(
                    &catalog_scope_key,
                    reconciliation_generation.expect("enabled reconciliation has a generation"),
                    &parents,
                    false,
                )
                .await
                .map_err(|error| {
                    error!("Failed to reconcile parent series identities: {error}");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;
        }
    }
    for fetch in server_items {
        if let Some(total) = fetch.upstream_total {
            upstream_total_sum += total.max(0);
        }
        all_fully_fetched &= fetch.fully_fetched;
        let ServerItems { response, server } = fetch.server_items;
        let items = response.into_items();
        if deduplicate_media {
            let aliases = season_catalog_aliases(state, &items, &viewer, None).await?;
            snapshots.push(MediaCatalogSnapshot {
                source_key: format!(
                    "{}:{}",
                    server.id,
                    fetch.source_parent_id.as_deref().unwrap_or_default()
                ),
                server_id: server.id,
                complete: fetch.fully_fetched && authoritative_inventory,
                observations: items
                    .iter()
                    .zip(&aliases)
                    .filter(|(item, _)| MediaKind::from_item_kind(&item.item_type).is_some())
                    .map(|(item, aliases)| MediaObservation {
                        virtual_media_id: item.id.clone(),
                        aliases: aliases.clone(),
                    })
                    .collect(),
            });
            catalog_aliases.push(aliases);
        } else {
            catalog_aliases.push(vec![BTreeSet::new(); items.len()]);
        }
        tagged_items.push(
            items
                .into_iter()
                .map(|item| TaggedMediaItem {
                    item,
                    server: server.clone(),
                })
                .collect::<Vec<_>>(),
        );
    }
    let next_up = watch_feed(original_request.url()) == Some(WatchFeed::NextUp);
    // Next Up is ranked upstream by when the previous episode was watched, which
    // the returned episodes cannot reproduce. Interleave each server's ranking
    // (as unmerged Next Up does) instead of concatenating whole servers.
    let (tagged_items, catalog_aliases) = if next_up {
        interleave_server_lists(tagged_items, catalog_aliases)
    } else {
        (
            tagged_items.into_iter().flatten().collect(),
            catalog_aliases.into_iter().flatten().collect(),
        )
    };

    let items = if deduplicate_media {
        let plan = MediaDedupPlan::with_aliases(tagged_items, catalog_aliases);
        let stable_group_ids = state
            .media_storage
            .reconcile_media_catalog(
                &catalog_scope_key,
                reconciliation_generation.expect("enabled reconciliation has a generation"),
                &snapshots,
                authoritative_inventory && skipped_targets == 0 && failures == 0,
            )
            .await
            .map_err(|error| {
                error!("Failed to reconcile media version groups: {error}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        let mut merged_items = plan.collapse(&stable_group_ids);
        for item in &mut merged_items {
            crate::handlers::media_versions::preserve_media_parent_groups(state, item, &viewer)
                .await?;
        }
        if next_up {
            merged_items = keep_furthest_episode_per_series(merged_items);
        }
        FederatedItems::from_merged_items(merged_items)
    } else {
        FederatedItems::from_tagged_items(tagged_items)
    };
    let total_count =
        estimate_merged_library_total(items.len(), upstream_total_sum, all_fully_fetched);

    finalize_items_response(
        items.with_reported_total(total_count),
        original_request.url(),
        response_shape,
    )
}

/// Federates `/Shows/{aggregateId}/Seasons|Episodes` across the member
/// series of a collapsed show. Seasons and (Jellyfin v12+) episodes merge
/// using provider identities, with seasons identified by their matched parent
/// series and season number when available.
pub(super) async fn get_aggregate_show_items(
    state: &AppState,
    preprocessed: PreprocessedRequest,
    aggregate_id: String,
    targets: Vec<CatalogFetchTarget>,
    skipped_targets: usize,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let deduplicate = state.deduplicate_media_enabled().await;
    let reconciliation_generation = if deduplicate {
        Some(
            state
                .media_storage
                .begin_media_reconciliation()
                .await
                .map_err(|error| {
                    error!("Failed to begin show reconciliation: {error}");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?,
        )
    } else {
        None
    };
    let original_request = preprocessed.original_request;
    let viewer = preprocessed
        .access_scope
        .as_ref()
        .map(|scope| scope.user_id().to_string())
        .unwrap_or_else(|| "anonymous".to_string());
    let catalog_scope_key = format!("aggregate:{aggregate_id}:{viewer}");
    let FetchedCatalog {
        server_items,
        failures: _,
        response_shape,
    } = fetch_show_catalog(state, &original_request, targets, skipped_targets).await?;

    let mut upstream_total_sum = 0i32;
    let mut all_fully_fetched = true;
    let mut snapshots = Vec::new();
    let mut tagged_items = Vec::new();
    let mut catalog_aliases = Vec::new();
    for fetch in server_items {
        if let Some(total) = fetch.upstream_total {
            upstream_total_sum += total.max(0);
        }
        all_fully_fetched &= fetch.fully_fetched;
        let ServerItems { response, server } = fetch.server_items;
        let items = response.into_items();
        if deduplicate {
            let aliases =
                season_catalog_aliases(state, &items, &viewer, Some(&aggregate_id)).await?;
            snapshots.push(MediaCatalogSnapshot {
                source_key: format!(
                    "{}:{}",
                    server.id,
                    fetch.source_parent_id.as_deref().unwrap_or_default()
                ),
                server_id: server.id,
                // Seasons, episodes and filtered listings share this source.
                // A show-child response cannot replace the whole inventory.
                complete: false,
                observations: items
                    .iter()
                    .zip(&aliases)
                    .filter(|(item, _)| MediaKind::from_item_kind(&item.item_type).is_some())
                    .map(|(item, aliases)| MediaObservation {
                        virtual_media_id: item.id.clone(),
                        aliases: aliases.clone(),
                    })
                    .collect(),
            });
            catalog_aliases.extend(aliases);
        }
        tagged_items.extend(items.into_iter().map(|item| TaggedMediaItem {
            item,
            server: server.clone(),
        }));
    }

    for tagged in &mut tagged_items {
        crate::handlers::media_versions::preserve_media_parent_groups(
            state,
            &mut tagged.item,
            &viewer,
        )
        .await?;
        if matches!(
            tagged.item.item_type,
            crate::models::enums::BaseItemKind::Season
                | crate::models::enums::BaseItemKind::Episode
        ) {
            if tagged.item.series_id.is_some() && tagged.item.parent_id == tagged.item.series_id {
                tagged.item.parent_id = Some(aggregate_id.clone());
            }
            tagged.item.series_id = Some(aggregate_id.clone());
        }
    }

    let items = if deduplicate {
        let plan = MediaDedupPlan::with_aliases(tagged_items, catalog_aliases);
        let stable_group_ids = state
            .media_storage
            .reconcile_media_catalog(
                &catalog_scope_key,
                reconciliation_generation.expect("enabled reconciliation has a generation"),
                &snapshots,
                false,
            )
            .await
            .map_err(|error| {
                error!("Failed to reconcile show version groups: {error}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        FederatedItems::from_merged_items(plan.collapse(&stable_group_ids))
    } else {
        FederatedItems::from_tagged_items(tagged_items)
    };
    let total_count =
        estimate_merged_library_total(items.len(), upstream_total_sum, all_fully_fetched);

    finalize_items_response(
        items.with_reported_total(total_count),
        original_request.url(),
        response_shape,
    )
}

/// Round-robin each server's ranked list, keeping item/alias pairs aligned.
fn interleave_server_lists(
    items: Vec<Vec<TaggedMediaItem>>,
    aliases: Vec<Vec<BTreeSet<MediaAlias>>>,
) -> (Vec<TaggedMediaItem>, Vec<BTreeSet<MediaAlias>>) {
    let mut queues = items
        .into_iter()
        .zip(aliases)
        .map(|(items, aliases)| items.into_iter().zip(aliases))
        .collect::<Vec<_>>();
    let mut merged_items = Vec::new();
    let mut merged_aliases = Vec::new();
    loop {
        let mut advanced = false;
        for queue in &mut queues {
            if let Some((item, aliases)) = queue.next() {
                merged_items.push(item);
                merged_aliases.push(aliases);
                advanced = true;
            }
        }
        if !advanced {
            return (merged_items, merged_aliases);
        }
    }
}

/// Each backend computes Next Up from its own watch history, so a show
/// watched mostly on one server can surface a stale, earlier episode from
/// another. Once two entries are known to belong to the same matched show
/// (both carry its aggregate series ID), keep only the furthest episode, at the
/// rank of the show's first appearance. Entries whose series is not matched
/// across servers keep their own server-local series ID and are never hidden.
fn keep_furthest_episode_per_series(
    items: Vec<crate::models::MediaItem>,
) -> Vec<crate::models::MediaItem> {
    use std::collections::HashMap;

    let position = |item: &crate::models::MediaItem| {
        let number = |key: &str| {
            item.extra
                .iter()
                .find(|(field, _)| field.eq_ignore_ascii_case(key))
                .and_then(|(_, value)| value.as_i64())
        };
        (number("ParentIndexNumber"), number("IndexNumber"))
    };
    let mut slots: Vec<Option<crate::models::MediaItem>> = Vec::with_capacity(items.len());
    let mut series_slots = HashMap::<String, usize>::new();
    for item in items {
        let series = (item.item_type == crate::models::enums::BaseItemKind::Episode)
            .then(|| item.series_id.clone())
            .flatten();
        let Some(series) = series else {
            slots.push(Some(item));
            continue;
        };
        if let Some(&slot) = series_slots.get(&series) {
            let current = slots[slot].as_ref().expect("series slot is filled");
            // Specials (season 0) never outrank regular progress. Entries
            // without both numbers cannot be compared, so they stay visible.
            let rank = |item: &crate::models::MediaItem| match position(item) {
                (Some(season), Some(episode)) => Some((season != 0, season, episode)),
                _ => None,
            };
            match (rank(current), rank(&item)) {
                (Some(current), Some(candidate)) => {
                    if candidate > current {
                        slots[slot] = Some(item);
                    }
                }
                _ => slots.push(Some(item)),
            }
            continue;
        }
        series_slots.insert(series, slots.len());
        slots.push(Some(item));
    }
    slots.into_iter().flatten().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::MIGRATOR, media_storage_service::MediaStorageService, server_storage::Server,
    };

    fn next_up_episode(
        id: &str,
        series: &str,
        season: i64,
        episode: i64,
    ) -> crate::models::MediaItem {
        serde_json::from_value(serde_json::json!({
            "Id": id, "Type": "Episode", "SeriesId": series,
            "ParentIndexNumber": season, "IndexNumber": episode
        }))
        .unwrap()
    }

    fn ids(items: &[crate::models::MediaItem]) -> Vec<&str> {
        items.iter().map(|item| item.id.as_str()).collect()
    }

    #[test]
    fn next_up_keeps_the_furthest_episode_of_a_matched_show_at_its_first_rank() {
        let items = vec![
            next_up_episode("a-show-s1e3", "show", 1, 3),
            next_up_episode("a-other", "other-a", 2, 1),
            next_up_episode("b-show-s2e1", "show", 2, 1),
            next_up_episode("b-other", "other-b", 1, 1),
        ];
        assert_eq!(
            ids(&keep_furthest_episode_per_series(items)),
            vec!["b-show-s2e1", "a-other", "b-other"]
        );
    }

    #[test]
    fn next_up_never_hides_unnumbered_or_special_progress_ahead_of_regular_episodes() {
        let unnumbered: crate::models::MediaItem = serde_json::from_value(serde_json::json!({
            "Id": "unnumbered", "Type": "Episode", "SeriesId": "show"
        }))
        .unwrap();
        let items = vec![
            next_up_episode("regular", "show", 1, 5),
            next_up_episode("special", "show", 0, 9),
            unnumbered,
        ];
        assert_eq!(
            ids(&keep_furthest_episode_per_series(items)),
            vec!["regular", "unnumbered"]
        );
    }

    #[tokio::test]
    async fn partial_inventories_retain_sightings_until_a_full_inventory_removes_them() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        MIGRATOR.run(&pool).await.unwrap();
        let row = sqlx::query("INSERT INTO servers (name, url, priority, created_at, updated_at) VALUES ('test', 'http://localhost:8096', 100, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING *")
            .fetch_one(&pool).await.unwrap();
        let server = Server::from_row(row).unwrap();
        let storage = MediaStorageService::new(pool.clone());
        let mut observations = Vec::new();
        for (id, kind) in [
            ("movie", "Movie"),
            ("series", "Series"),
            ("season", "Season"),
            ("episode", "Episode"),
        ] {
            let mapping = storage
                .get_or_create_media_mapping(id, &server)
                .await
                .unwrap();
            let item = serde_json::from_value(serde_json::json!({
                "Id": mapping.virtual_media_id, "Type": kind, "ProviderIds": {"Tmdb": "42"}
            }))
            .unwrap();
            observations.push(MediaObservation {
                virtual_media_id: mapping.virtual_media_id,
                aliases: MediaAlias::from_item(&item),
            });
        }

        for (path, observed, expected) in [
            (
                "/Items?ParentId=show&Recursive=true",
                observations.clone(),
                4,
            ),
            (
                "/Items?ParentId=show&Recursive=true&IncludeItemTypes=Movie",
                vec![observations[0].clone()],
                4,
            ),
            (
                "/Items?ParentId=show&Recursive=true&IncludeItemTypes=Series",
                vec![observations[1].clone()],
                4,
            ),
            ("/Shows/show/Seasons", vec![observations[2].clone()], 4),
            ("/Shows/show/Episodes", vec![observations[3].clone()], 4),
            (
                "/Shows/show/Episodes?SeasonId=season&IsPlayed=true",
                vec![],
                4,
            ),
            (
                "/Items?ParentId=show&Recursive=true",
                vec![observations[0].clone()],
                1,
            ),
            ("/Items?ParentId=show&Recursive=true", vec![], 0),
        ] {
            let url = url::Url::parse(&format!("http://localhost{path}")).unwrap();
            let complete = is_authoritative_media_inventory_request(&url);
            let generation = storage.begin_media_reconciliation().await.unwrap();
            storage
                .reconcile_media_catalog(
                    "aggregate:show:user",
                    generation,
                    &[MediaCatalogSnapshot {
                        source_key: format!("{}:show", server.id),
                        server_id: server.id,
                        complete,
                        observations: observed,
                    }],
                    complete,
                )
                .await
                .unwrap();
            let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM movie_catalog_sightings")
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(count, expected, "{path}");
        }
    }
}
