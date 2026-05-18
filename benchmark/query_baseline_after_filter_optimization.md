# Query Baseline After Filter Optimization

Command:

```bash
cargo bench --bench query_baseline --locked
```

Environment:

- Entities: 10000
- Iterations: 40
- Project: bench-project
- Kind: BenchItem

Results:

| Case | Avg us | Min us | Total ms | Returned |
|---|---:|---:|---:|---:|
| indexed_equality_limit_20 | 71.65 | 69.38 | 2.87 | 20 |
| indexed_array_in_limit_50 | 2107.76 | 2009.00 | 84.31 | 50 |
| nested_indexed_equality | 1319.61 | 1241.38 | 52.78 | 50 |
| inequality_scan_sort_limit_50 | 1246.08 | 1171.71 | 49.84 | 50 |
| full_kind_ordered_limit_50 | 1816.70 | 1725.92 | 72.67 | 50 |
| keys_only_offset_500 | 1099.12 | 930.71 | 43.96 | 50 |

Comparison against `query_baseline_before.md`:

| Case | Before avg us | After avg us | Change |
|---|---:|---:|---:|
| indexed_equality_limit_20 | 79.21 | 71.65 | -9.54% |
| indexed_array_in_limit_50 | 2542.37 | 2107.76 | -17.09% |
| nested_indexed_equality | 1457.17 | 1319.61 | -9.44% |
| inequality_scan_sort_limit_50 | 1855.57 | 1246.08 | -32.85% |
| full_kind_ordered_limit_50 | 1989.50 | 1816.70 | -8.69% |
| keys_only_offset_500 | 973.51 | 1099.12 | +12.90% |
