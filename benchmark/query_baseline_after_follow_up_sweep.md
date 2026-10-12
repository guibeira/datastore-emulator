# Query Baseline After the Follow-up Sweep

Five changes, measured one after the other on the same machine (Apple
Silicon, 10k entities, `DATASTORE_QUERY_BENCH_SAMPLES=20`,
`DATASTORE_QUERY_BENCH_MEASUREMENT_SECONDS=3`). The baseline is `8021f3c`
(main after #35, release profile and mimalloc already in).

1. `1a30d4e` Lazy candidates: `query_candidates` returns an iterator and a
   query without an order (or ordered by `__key__` ascending) stops after
   `offset + limit + 1` candidates.
2. `7b21859` `SortView` looks up the sort values once per candidate;
   `distinct_on` keeps one representative per group before the sort.
3. `d9fecb6` One byte-string index key per entry, index sets of
   `Arc<KeyStruct>` shared with the entity map.
4. `ca117ba` `parking_lot::RwLock` instead of `tokio::sync::RwLock`; commit
   validates before locking.

Command, at each revision:

```sh
DATASTORE_QUERY_BENCH_SAMPLES=20 DATASTORE_QUERY_BENCH_MEASUREMENT_SECONDS=3 \
cargo bench --bench query_baseline
```

Results (median; each step column is relative to the previous one, the
last column to the baseline):

| Benchmark | Baseline | Lazy candidates | SortView + distinct | Index keys | parking_lot | Total |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `nested_indexed_equality` | 529.63 us | 30.86 us (-94%) | 36.61 us (+19%) | 31.93 us (-13%) | 31.79 us (-0%) | -94% |
| `keys_only_offset_500` | 72.04 us | 9.40 us (-87%) | 10.46 us (+11%) | 9.66 us (-8%) | 9.91 us (+3%) | -86% |
| `distinct_on_region_bucket` | 11.193 ms | 8.771 ms (-22%) | 2.481 ms (-72%) | 2.381 ms (-4%) | 2.524 ms (+6%) | -77% |
| `indexed_equality_limit_20` | 36.05 us | 12.64 us (-65%) | 13.33 us (+5%) | 13.21 us (-1%) | 14.06 us (+6%) | -61% |
| `key_filter_greater_than` | 251.93 us | 130.60 us (-48%) | 136.23 us (+4%) | 124.82 us (-8%) | 130.92 us (+5%) | -48% |
| `indexed_array_in_limit_50` | 1.363 ms | 737.73 us (-46%) | 878.48 us (+19%) | 767.99 us (-13%) | 741.11 us (-4%) | -46% |
| `index_entity_with_nested` | 1.21 us | 1.19 us (-2%) | 1.21 us (+2%) | 689.5 ns (-43%) | 659.3 ns (-4%) | -45% |
| `update_one_field_many_indexes` | 6.48 us | 5.34 us (-18%) | 5.54 us (+4%) | 4.27 us (-23%) | 3.75 us (-12%) | -42% |
| `delete_entity_with_nested` | 2.15 us | 1.99 us (-7%) | 1.99 us (-0%) | 1.37 us (-31%) | 1.25 us (-9%) | -42% |
| `mixed_scope_full_kind_limit_50` | 16.65 us | 10.46 us (-37%) | 10.36 us (-1%) | 10.36 us (-0%) | 10.38 us (+0%) | -38% |
| `inequality_scan_sort_limit_50` | 881.56 us | 788.37 us (-11%) | 727.01 us (-8%) | 574.28 us (-21%) | 551.06 us (-4%) | -37% |
| `upsert_existing_entity_with_nested` | 1.95 us | 1.94 us (-1%) | 1.85 us (-4%) | 1.42 us (-23%) | 1.29 us (-9%) | -34% |
| `commit_batch_10_inserts` | 28.38 us | 25.85 us (-9%) | 27.42 us (+6%) | 17.94 us (-35%) | 18.90 us (+5%) | -33% |
| `full_kind_ordered_limit_50` | 1.065 ms | 1.083 ms (+2%) | 816.21 us (-25%) | 578.00 us (-29%) | 715.66 us (+24%) | -33% |
| `update_entity_with_nested` | 2.11 us | 1.75 us (-17%) | 1.78 us (+2%) | 1.48 us (-17%) | 1.45 us (-1%) | -31% |
| `insert_entity_with_nested` | 1.76 us | 1.74 us (-1%) | 1.74 us (+0%) | 1.29 us (-26%) | 1.22 us (-5%) | -30% |
| `mixed_scope_ordered_limit_50` | 87.23 us | 91.74 us (+5%) | 57.86 us (-37%) | 58.46 us (+1%) | 62.01 us (+6%) | -29% |
| `commit_transactional_batch_10` | 24.67 us | 24.32 us (-1%) | 25.67 us (+6%) | 18.46 us (-28%) | 19.18 us (+4%) | -22% |
| `mixed_scope_inequality_limit_50` | 73.46 us | 76.32 us (+4%) | 57.40 us (-25%) | 58.33 us (+2%) | 59.20 us (+1%) | -19% |
| `read_only_transaction_lifecycle` | 182.9 ns | 183.4 ns (+0%) | 191.2 ns (+4%) | 195.1 ns (+2%) | 147.6 ns (-24%) | -19% |
| `insert_entity_key_only` | 300.9 ns | 291.4 ns (-3%) | 301.3 ns (+3%) | 260.0 ns (-14%) | 262.4 ns (+1%) | -13% |
| `missing_kind_keys_only_cleanup` | 203.5 ns | 189.6 ns (-7%) | 189.9 ns (+0%) | 194.1 ns (+2%) | 189.7 ns (-2%) | -7% |
| `lookup_batch_100` | 68.84 us | 68.04 us (-1%) | 68.49 us (+1%) | 73.03 us (+7%) | 70.97 us (-3%) | +3% |
| `ancestor_query_rows_20` | 8.74 us | 8.80 us (+1%) | 9.16 us (+4%) | 9.01 us (-2%) | 9.22 us (+2%) | +6% |
| `aggregation_count_bucket` | 27.77 us | 27.98 us (+1%) | 27.89 us (-0%) | 30.06 us (+8%) | 30.43 us (+1%) | +10% |

Notes:

- Queries that used to materialize every candidate of the kind before the
  limit (`nested_indexed_equality`, `keys_only_offset_500`,
  `indexed_equality_limit_20`) now cost about the limit, not the kind.
- `distinct_on_region_bucket` sorted 10k candidates and then deduplicated;
  it now sorts the representatives.
- Writes drop with the index keys: one allocation per new index key and a
  refcount bump per entry instead of three Strings and a KeyStruct clone.
- `lookup_batch_100`, `ancestor_query_rows_20` and `aggregation_count_bucket`
  move within a few percent, up: entity map lookups now compare keys
  through an `Arc`, one more pointer to follow per comparison.
- Run-to-run noise on this machine is about ±5% (`full_kind_ordered_limit_50`
  swings 24% between two runs of the same code).
