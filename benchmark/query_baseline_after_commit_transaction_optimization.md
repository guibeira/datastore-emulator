# Commit transaction baseline after optimization

Command:

```text
DATASTORE_QUERY_BENCH_SAMPLES=30 DATASTORE_QUERY_BENCH_MEASUREMENT_SECONDS=3 cargo bench --bench query_baseline --locked -- commit_
```

Environment:

- Date: 2026-05-19
- Criterion samples: 30
- Criterion measurement time: 3s

Results:

| Benchmark | Before | After | Change |
| --- | ---: | ---: | ---: |
| `commit_batch_10_inserts` | 40.648 us | 40.151 us | -1.22% |
| `commit_transactional_batch_10` | 40.829 us | 39.880 us | -2.32% |

Notes:

- `commit` preallocates mutation results, removes transaction selector clones, and uses one commit timestamp across mutations.
- `insert_entity_at` lets commit reuse that timestamp for insert and incomplete-key upsert paths.
- `TransactionState` no longer stores unused mutation/snapshot/read-only fields.
