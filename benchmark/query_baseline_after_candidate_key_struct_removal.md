# Query Baseline After Removing the Per-Candidate KeyStruct

`QueryCandidate` no longer carries an `Option<KeyStruct>`. Key filters,
key ordering, cursors and `distinct_on(__key__)` compare the entity key with
`compare_keys`, which has the same order as `KeyStruct`, so the clone of three
`String`s and a `Vec` per candidate is gone. Queries ordered only by `__key__`
also skip the sort, because candidates already arrive in key order (a
descending order just reverses the vector).

Baseline: `query_baseline_after_release_profile.md` (LTO + mimalloc).

Command:

```sh
DATASTORE_QUERY_BENCH_SAMPLES=20 DATASTORE_QUERY_BENCH_MEASUREMENT_SECONDS=3 \
cargo bench --bench query_baseline
```

Results (median):

| Benchmark | Before | After | Change |
| --- | ---: | ---: | ---: |
| `key_filter_greater_than` | 626.18 us | 244.65 us | -61.2% |
| `keys_only_offset_500` | 137.51 us | 70.35 us | -48.9% |
| `mixed_scope_full_kind_limit_50` | 23.28 us | 16.36 us | -29.5% |
| `inequality_scan_sort_limit_50` | 924.64 us | 750.27 us | -18.8% |
| `mixed_scope_inequality_limit_50` | 83.85 us | 72.77 us | -13.4% |
| `mixed_scope_ordered_limit_50` | 99.49 us | 86.62 us | -13.0% |
| `distinct_on_region_bucket` | 9.828 ms | 8.611 ms | -10.8% |
| `full_kind_ordered_limit_50` | 1.259 ms | 1.120 ms | -8.1% |
| `indexed_array_in_limit_50` | 1.369 ms | 1.336 ms | -3.5% |
| `nested_indexed_equality` | 537.01 us | 520.06 us | -2.2% |
| `indexed_equality_limit_20` | 36.27 us | 35.51 us | -2.1% |
| `lookup_batch_100` | 69.36 us | 68.65 us | -0.8% |

Write-path benchmarks moved within noise (-7% to +7%); they do not build
query candidates.
