# Query baseline after read-lock optimization

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

| Benchmark | Before | After | Change |
| --- | ---: | ---: | ---: |
| `indexed_equality_limit_20` | 71.366 us | 62.815 us | -11.98% |
| `indexed_array_in_limit_50` | 2.1672 ms | 1.9055 ms | -12.08% |
| `nested_indexed_equality` | 1.3730 ms | 1.2136 ms | -11.61% |
| `inequality_scan_sort_limit_50` | 1.1104 ms | 1.1054 ms | -0.45% |
| `full_kind_ordered_limit_50` | 1.6175 ms | 1.5193 ms | -6.07% |
| `keys_only_offset_500` | 1.0509 ms | 1.0125 ms | -3.65% |

Notes:

- `run_query` now holds the storage read lock only while collecting a query snapshot.
- Entity records are stored behind `Arc`, so query snapshots clone cheap shared handles and keep read consistency while writes can replace entity records through copy-on-write.
- Scan fallback queries still apply filters while collecting candidates to avoid snapshotting entities that will be discarded.
