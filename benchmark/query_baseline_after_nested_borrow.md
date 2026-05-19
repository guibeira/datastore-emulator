# Query bench: resolve_nested_property returns borrowed values

Optimization: `resolve_nested_property` and `resolve_property_path_recursive`
previously cloned every leaf `Value` (`array.values.clone()` /
`vec![value.clone()]`) before returning. The sole caller in
`apply_filter` then iterated the returned `Vec<Value>` to feed into
`values_match_filter`, which only needs `&Value`. Changed the return type
to `Vec<&'a Value>` so the entity properties stay borrowed for the whole
filter evaluation, eliminating the per-entity leaf clone on every nested
filter check.

For the existing `nested_indexed_equality` bench (10k entities, filter
`profile.tier == "tier-3"`), `apply_filter` is invoked on every index
candidate, which makes the per-entity clone noticeable in aggregate.

| Case | Before | After | Change |
|---|---|---|---|
| nested_indexed_equality | 1.184 ms | 1.166 ms | **−3.76%** (p=0.00) |
