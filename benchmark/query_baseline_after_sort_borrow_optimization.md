# Query Baseline After Sort Borrow Optimization

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

Before this change:

| Case | Time |
|---|---:|
| indexed_equality_limit_20 | [70.819 us, 71.066 us, 71.402 us] |
| indexed_array_in_limit_50 | [2.1636 ms, 2.2100 ms, 2.2631 ms] |
| nested_indexed_equality | [1.3693 ms, 1.3857 ms, 1.4030 ms] |
| inequality_scan_sort_limit_50 | [1.3032 ms, 1.3250 ms, 1.3491 ms] |
| full_kind_ordered_limit_50 | [1.9701 ms, 2.0210 ms, 2.0743 ms] |
| keys_only_offset_500 | [1.0205 ms, 1.0414 ms, 1.0712 ms] |

After this change:

| Case | Time |
|---|---:|
| indexed_equality_limit_20 | [70.491 us, 71.285 us, 72.683 us] |
| indexed_array_in_limit_50 | [2.1360 ms, 2.1549 ms, 2.1747 ms] |
| nested_indexed_equality | [1.3700 ms, 1.4045 ms, 1.4495 ms] |
| inequality_scan_sort_limit_50 | [1.0727 ms, 1.0984 ms, 1.1380 ms] |
| full_kind_ordered_limit_50 | [1.5666 ms, 1.6010 ms, 1.6388 ms] |
| keys_only_offset_500 | [1.0085 ms, 1.0179 ms, 1.0295 ms] |

Middle estimate changes:

| Case | Before | After | Change |
|---|---:|---:|---:|
| indexed_equality_limit_20 | 71.066 us | 71.285 us | +0.31% |
| indexed_array_in_limit_50 | 2.2100 ms | 2.1549 ms | -2.49% |
| nested_indexed_equality | 1.3857 ms | 1.4045 ms | +1.36% |
| inequality_scan_sort_limit_50 | 1.3250 ms | 1.0984 ms | -17.10% |
| full_kind_ordered_limit_50 | 2.0210 ms | 1.6010 ms | -20.78% |
| keys_only_offset_500 | 1.0414 ms | 1.0179 ms | -2.26% |
