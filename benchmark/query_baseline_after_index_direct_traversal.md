# Insert/index baseline after direct traversal

Command:

```text
DATASTORE_QUERY_BENCH_SAMPLES=30 DATASTORE_QUERY_BENCH_MEASUREMENT_SECONDS=3 cargo bench --bench query_baseline --locked -- 'insert_entity|index_entity|update_entity|upsert_existing|delete_entity|commit_batch'
```

Environment:

- Date: 2026-05-19
- Criterion samples: 30
- Criterion measurement time: 3s

Results:

| Benchmark | Before | After | Change |
| --- | ---: | ---: | ---: |
| `insert_entity_with_nested` | 3.5418 us | 3.2401 us | -8.52% |
| `insert_entity_key_only` | 503.77 ns | 501.72 ns | -0.41% |
| `index_entity_with_nested` | 2.6011 us | 2.2041 us | -15.26% |
| `update_entity_with_nested` | 6.7749 us | 6.2821 us | -7.27% |
| `delete_entity_with_nested` | 3.7532 us | 3.4094 us | -9.16% |
| `upsert_existing_entity_with_nested` | 6.6589 us | 6.4987 us | -2.41% |
| `commit_batch_10_inserts` | 39.661 us | 36.945 us | -6.85% |

Notes:

- `insert_entity_key_only` shows the non-index insert path is around 0.5 us.
- `index_entity_with_nested` was the dominant insert cost and improved by avoiding intermediate `Vec<(path, Vec<value>)>` materialization.
- Update/delete paths benefit because they remove and/or rebuild indexes.
