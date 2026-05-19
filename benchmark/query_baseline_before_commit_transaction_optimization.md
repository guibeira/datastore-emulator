# Commit transaction baseline before optimization

Command:

```text
DATASTORE_QUERY_BENCH_SAMPLES=30 DATASTORE_QUERY_BENCH_MEASUREMENT_SECONDS=3 cargo bench --bench query_baseline --locked -- commit_
```

Environment:

- Date: 2026-05-19
- Criterion samples: 30
- Criterion measurement time: 3s

Results:

| Benchmark | Mean |
| --- | ---: |
| `commit_batch_10_inserts` | 40.648 us |
| `commit_transactional_batch_10` | 40.829 us |
