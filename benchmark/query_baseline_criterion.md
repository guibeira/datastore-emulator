# Criterion Query Baseline

Command:

```bash
cargo bench --bench query_baseline --locked
```

Environment:

- Benchmark harness: Criterion.rs 0.7.0
- Entities: 10000
- Samples: 50
- Measurement time: 5 seconds per case
- Project: bench-project
- Kind: BenchItem

Results from the locked run after converting `benches/query_baseline.rs` to Criterion:

| Case | Time |
|---|---:|
| indexed_equality_limit_20 | [71.127 us, 71.288 us, 71.466 us] |
| indexed_array_in_limit_50 | [2.0840 ms, 2.1105 ms, 2.1401 ms] |
| nested_indexed_equality | [1.3494 ms, 1.3719 ms, 1.3970 ms] |
| inequality_scan_sort_limit_50 | [1.3823 ms, 1.4499 ms, 1.5184 ms] |
| full_kind_ordered_limit_50 | [1.9145 ms, 1.9771 ms, 2.0477 ms] |
| keys_only_offset_500 | [1.0228 ms, 1.0438 ms, 1.0684 ms] |

Criterion comparison against the previous Criterion run:

| Case | Criterion assessment |
|---|---|
| indexed_equality_limit_20 | No change in performance detected |
| indexed_array_in_limit_50 | Performance has improved |
| nested_indexed_equality | Change within noise threshold |
| inequality_scan_sort_limit_50 | No change in performance detected |
| full_kind_ordered_limit_50 | Performance has improved |
| keys_only_offset_500 | Performance has improved |
