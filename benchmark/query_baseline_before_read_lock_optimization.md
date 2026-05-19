# Query baseline before read-lock optimization

Command:

```text
cargo bench --bench query_baseline --locked
```

Environment:

- Date: 2026-05-18
- Entity count: default 10,000
- Criterion samples: default 50
- Criterion measurement time: default 5s

Results:

| Benchmark | Mean |
| --- | ---: |
| `indexed_equality_limit_20` | 71.366 us |
| `indexed_array_in_limit_50` | 2.1672 ms |
| `nested_indexed_equality` | 1.3730 ms |
| `inequality_scan_sort_limit_50` | 1.1104 ms |
| `full_kind_ordered_limit_50` | 1.6175 ms |
| `keys_only_offset_500` | 1.0509 ms |
