# Query Baseline Before Critical Index Optimizations

Command:

```sh
DATASTORE_QUERY_BENCH_SAMPLES=20 \
DATASTORE_QUERY_BENCH_MEASUREMENT_SECONDS=3 \
DATASTORE_QUERY_BENCH_MIXED_SCOPE_ENTITIES_PER_KIND=1000 \
cargo bench --locked --bench query_baseline -- --save-baseline before_all_perf mixed_scope

DATASTORE_QUERY_BENCH_SAMPLES=20 \
DATASTORE_QUERY_BENCH_MEASUREMENT_SECONDS=3 \
cargo bench --locked --bench query_baseline -- --save-baseline before_all_perf update_one_field_many_indexes
```

Results:

| Benchmark | Mean |
| --- | ---: |
| `mixed_scope_full_kind_limit_50` | 193.78 us |
| `mixed_scope_ordered_limit_50` | 518.56 us |
| `mixed_scope_inequality_limit_50` | 293.97 us |
| `update_one_field_many_indexes` | 24.473 us |
