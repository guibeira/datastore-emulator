# Aggregation bench: route count/sum/avg through query_candidates

Optimization: `run_aggregation_query` was always doing a full BTreeMap scan over `storage.entities` — checking `project_id`, kind, and the filter for every entity in the database — even when the filter could be served from the indexes used by regular queries. Refactored the matching-entities collection path: when the nested query targets a single kind, it now calls `storage.query_candidates(&project_id, kind, filter_type)`, which already understands the index lookups and the deferred project/kind filtering. The multi-kind branch keeps the original full scan since `query_candidates` is per-kind.

While there I dropped the redundant `aggregation_query.clone()` (only `query_type` was needed) and replaced the per-entity `&Arc<EntityWithMetadata>` collection with `Arc::clone` so the downstream sum/avg loops can iterate owned `Arc`s consistently.

| Case | Before | After | Change |
|---|---|---|---|
| aggregation_count_bucket | 397 µs | 38 µs | **−90.3%** (p=0.00) |
