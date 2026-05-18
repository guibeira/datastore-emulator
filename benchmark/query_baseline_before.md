# Query Baseline Before Optimization

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
| indexed_equality_limit_20 | 79.21 | 74.00 | 3.17 | 20 |
| indexed_array_in_limit_50 | 2542.37 | 2244.17 | 101.69 | 50 |
| nested_indexed_equality | 1457.17 | 1297.17 | 58.29 | 50 |
| inequality_scan_sort_limit_50 | 1855.57 | 1486.25 | 74.22 | 50 |
| full_kind_ordered_limit_50 | 1989.50 | 1770.71 | 79.58 | 50 |
| keys_only_offset_500 | 973.51 | 901.17 | 38.94 | 50 |
