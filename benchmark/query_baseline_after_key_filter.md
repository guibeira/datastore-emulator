# Query bench: __key__ filter without Value wrapper

Optimization: replaced the per-entity wrapper `Value { value_type: Some(ValueType::KeyValue(key.clone())) }` followed by `values_match_filter` with a dedicated `match_key_against_filter` helper that compares the entity `Key` directly via `KeyStruct::cmp`. The previous path cloned the full `Key` proto for every candidate entity just to satisfy the `&Value` signature; the new path skips both the clone and the `compare_values` dispatch.

Bench case added: `key_filter_greater_than` (10k entities, `__key__ > entity-005000`).

| Case | Before | After | Change |
|---|---|---|---|
| key_filter_greater_than | 3.08 ms | 2.32 ms | **−24.2%** (p=0.00) |
