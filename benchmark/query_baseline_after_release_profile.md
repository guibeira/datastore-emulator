# Query Baseline After Release Profile (LTO + mimalloc)

Two changes, measured one after the other on the same machine (Apple Silicon,
10k entities, `DATASTORE_QUERY_BENCH_SAMPLES=20`,
`DATASTORE_QUERY_BENCH_MEASUREMENT_SECONDS=3`):

1. `[profile.release] lto = "fat", codegen-units = 1` in `Cargo.toml`.
2. `mimalloc` as the global allocator (`src/main.rs` and the bench, so both
   use the same allocator).

Commands. The first two run on `11ece7d`, the last commit before the
profile and mimalloc landed; the bench binary at that revision uses the
system allocator. The third runs on `1519441`.

```sh
# Baseline (default profile, system allocator), at 11ece7d
DATASTORE_QUERY_BENCH_SAMPLES=20 DATASTORE_QUERY_BENCH_MEASUREMENT_SECONDS=3 \
cargo bench --bench query_baseline

# Step 1, LTO only, at 11ece7d
CARGO_PROFILE_BENCH_LTO=fat CARGO_PROFILE_BENCH_CODEGEN_UNITS=1 \
DATASTORE_QUERY_BENCH_SAMPLES=20 DATASTORE_QUERY_BENCH_MEASUREMENT_SECONDS=3 \
cargo bench --bench query_baseline

# Step 2, profile in Cargo.toml plus mimalloc, at 1519441
DATASTORE_QUERY_BENCH_SAMPLES=20 DATASTORE_QUERY_BENCH_MEASUREMENT_SECONDS=3 \
cargo bench --bench query_baseline
```

Results (median; each change column is relative to the previous column):

| Benchmark | Baseline | LTO | mimalloc |
| --- | ---: | ---: | ---: |
| `indexed_equality_limit_20` | 44.56 us | 41.69 us (-5.2%) | 36.27 us (-13.3%) |
| `indexed_array_in_limit_50` | 1.392 ms | 1.369 ms (-2.2%) | 1.369 ms (+1.2%) |
| `nested_indexed_equality` | 549.96 us | 559.16 us (+0.5%) | 537.01 us (-3.6%) |
| `inequality_scan_sort_limit_50` | 854.71 us | 864.32 us (+1.9%) | 924.64 us (+5.6%) |
| `full_kind_ordered_limit_50` | 1.322 ms | 1.174 ms (-9.4%) | 1.259 ms (+6.5%) |
| `keys_only_offset_500` | 165.09 us | 139.22 us (-16.5%) | 137.51 us (+1.2%) |
| `key_filter_greater_than` | 810.67 us | 741.69 us (-8.2%) | 626.18 us (-15.1%) |
| `distinct_on_region_bucket` | 9.796 ms | 9.056 ms (-5.9%) | 9.828 ms (+5.9%) |
| `mixed_scope_full_kind_limit_50` | 37.34 us | 31.40 us (-17.7%) | 23.28 us (-26.9%) |
| `mixed_scope_ordered_limit_50` | 111.10 us | 110.87 us (-0.7%) | 99.49 us (-10.1%) |
| `mixed_scope_inequality_limit_50` | 87.45 us | 89.17 us (+1.1%) | 83.85 us (-5.3%) |
| `missing_kind_keys_only_cleanup` | 322.54 ns | 269.71 ns (-17.0%) | 208.30 ns (-22.9%) |
| `ancestor_query_rows_20` | 11.76 us | 11.26 us (-5.2%) | 9.04 us (-19.4%) |
| `lookup_batch_100` | 108.53 us | 92.48 us (-17.5%) | 69.36 us (-25.2%) |
| `insert_entity_with_nested` | 3.529 us | 3.128 us (-11.7%) | 1.768 us (-43.7%) |
| `insert_entity_key_only` | 700.45 ns | 572.79 ns (-17.8%) | 299.64 ns (-48.0%) |
| `index_entity_with_nested` | 2.343 us | 2.135 us (-8.6%) | 1.207 us (-44.0%) |
| `update_entity_with_nested` | 3.636 us | 3.452 us (-4.3%) | 1.864 us (-50.2%) |
| `update_one_field_many_indexes` | 11.59 us | 11.27 us (-3.1%) | 5.53 us (-53.3%) |
| `aggregation_count_bucket` | 28.69 us | 28.60 us (-0.4%) | 28.11 us (-1.7%) |
| `delete_entity_with_nested` | 3.948 us | 3.707 us (-5.3%) | 1.834 us (-51.7%) |
| `upsert_existing_entity_with_nested` | 3.618 us | 3.431 us (-5.1%) | 1.677 us (-53.8%) |
| `commit_batch_10_inserts` | 37.92 us | 33.55 us (-12.1%) | 25.80 us (-23.2%) |
| `commit_transactional_batch_10` | 35.14 us | 31.71 us (-9.1%) | 24.20 us (-23.8%) |
| `read_only_transaction_lifecycle` | 281.12 ns | 276.03 ns (-3.4%) | 182.44 ns (-31.7%) |

Notes:

- The allocator dominates the write path: inserts, updates and deletes spend
  most of their time building index keys (three `String`s per index entry)
  and cloning `KeyStruct`s. mimalloc halves them.
- Sort-bound queries (`inequality_scan_sort_limit_50`,
  `full_kind_ordered_limit_50`, `distinct_on_region_bucket`) do not allocate
  much and move within noise.
- End to end, a single Python client sees about 1 ms per 50-entity query over
  gRPC, of which the server spends roughly 40 us. These gains show up under
  concurrency or with large result sets, not in single-client latency.
