# Query Baseline After Critical Index Optimizations

Commands:

```sh
DATASTORE_QUERY_BENCH_SAMPLES=20 \
DATASTORE_QUERY_BENCH_MEASUREMENT_SECONDS=3 \
DATASTORE_QUERY_BENCH_MIXED_SCOPE_ENTITIES_PER_KIND=1000 \
cargo bench --bench query_baseline -- --baseline before_all_perf mixed_scope

DATASTORE_QUERY_BENCH_SAMPLES=20 \
DATASTORE_QUERY_BENCH_MEASUREMENT_SECONDS=3 \
cargo bench --bench query_baseline -- --baseline before_all_perf update_one_field_many_indexes

DATASTORE_QUERY_BENCH_SAMPLES=20 \
DATASTORE_QUERY_BENCH_MEASUREMENT_SECONDS=3 \
cargo bench --bench query_baseline -- insert_entity_with_nested
```

Results:

| Benchmark | Before | After | Change |
| --- | ---: | ---: | ---: |
| `mixed_scope_full_kind_limit_50` | 193.78 us | 111.83 us | -41.73% |
| `mixed_scope_ordered_limit_50` | 518.56 us | 168.53 us | -67.78% |
| `mixed_scope_inequality_limit_50` | 293.97 us | 122.64 us | -58.30% |
| `update_one_field_many_indexes` | 24.473 us | 16.311 us | -32.80% |
| `insert_entity_with_nested` | 3.2401 us | 3.4409 us | +6.20% |

Notes:

- A range index prototype was evaluated and removed because it made `insert_entity_with_nested` roughly 2x slower while not improving the current inequality benchmark. The final inequality gain comes from scoped candidates plus bounded sorting.
- `insert_entity_with_nested` uses the prior post-direct-traversal baseline from `query_baseline_after_index_direct_traversal.md`.

