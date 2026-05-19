# Insert/index baseline before direct traversal

Command:

```text
DATASTORE_QUERY_BENCH_SAMPLES=30 DATASTORE_QUERY_BENCH_MEASUREMENT_SECONDS=3 cargo bench --bench query_baseline --locked -- 'insert_entity|index_entity|update_entity|upsert_existing|delete_entity|commit_batch'
```

Environment:

- Date: 2026-05-19
- Criterion samples: 30
- Criterion measurement time: 3s

Results:

| Benchmark | Mean |
| --- | ---: |
| `insert_entity_with_nested` | 3.5418 us |
| `insert_entity_key_only` | 503.77 ns |
| `index_entity_with_nested` | 2.6011 us |
| `update_entity_with_nested` | 6.7749 us |
| `delete_entity_with_nested` | 3.7532 us |
| `upsert_existing_entity_with_nested` | 6.6589 us |
| `commit_batch_10_inserts` | 39.661 us |

Observation:

- Direct indexing accounts for roughly 73% of `insert_entity_with_nested`.
- Existing update/upsert paths remove and rebuild indexes, so they pay most indexing cost twice.
