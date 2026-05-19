# Query bench: bulk-clone index sets instead of per-key filter

Optimization: in `index_lookup_for_filter`, the EQ (op 5) and IN (op 6) paths were running `keys.iter().filter(|k| k.project_id == project_id).cloned().collect()` for every matched index bucket. `query_candidates` re-applies the same `project_id` and `kind_name` filter on the returned set, so the per-key check inside the index lookup was duplicating work. Replaced the filter+clone pipeline with a direct `BTreeSet::clone` / bulk `extend`, keeping the project/kind filter in `query_candidates` as the single source of truth.

| Case | Before | After | Change |
|---|---|---|---|
| indexed_equality_limit_20 | 61.9 µs | 59.4 µs | **−3.89%** (p=0.00) |
| indexed_array_in_limit_50 | 2.02 ms | 1.97 ms | +1.05% (no change) |
| nested_indexed_equality | 1.30 ms | 1.20 ms | **−6.55%** (p=0.00) |
