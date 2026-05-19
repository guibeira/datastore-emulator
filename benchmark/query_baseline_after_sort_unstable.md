# Query bench: sort_by → sort_unstable_by

Optimization: replaced `sort_by` with `sort_unstable_by` in `get_entities_from_candidates`. Datastore tie-breaking is implicit by `__key__`, so order between equal sort keys is not contractually guaranteed; using the unstable variant is safe and avoids the extra memory/stability overhead.

Measured with `DATASTORE_QUERY_BENCH_ENTITIES=10000` (default) on the Criterion bench.

| Case | Before | After | Change | Significant |
|---|---|---|---|---|
| indexed_equality_limit_20 | 62.6 µs | 62.9 µs | +0.48% | no |
| indexed_array_in_limit_50 | 1.903 ms | 1.886 ms | −0.89% | no |
| nested_indexed_equality | 1.226 ms | 1.214 ms | −0.98% | no |
| inequality_scan_sort_limit_50 | 1.154 ms | 1.098 ms | **−4.85%** | yes (p=0.00) |
| full_kind_ordered_limit_50 | 1.571 ms | 1.503 ms | −4.33% | no (p=0.08) |
| keys_only_offset_500 | 984.5 µs | 982.6 µs | −0.19% | no |

Both sort-driven cases trended faster; only the inequality+sort case crossed the 5% significance bar.
