# Insert bench: nested path concat without format!

Optimization: `collect_nested_indexable_paths` was building each nested
property path with `format!("{}.{}", prefix, prop_name)`. Replaced with
manual `String::with_capacity(...)` plus `push_str` to skip the
`fmt::Write` plumbing. Also preallocated the outer `results` Vec to
`properties.len()` and gave `get_indexable_strings_for_value` an
explicit capacity (1 for scalars, `array.values.len()` for arrays).

Added an `insert_entity_with_nested` bench case (single entity, nested
`profile.*` entity, three indexed tags) using `iter_batched` against a
fresh `DatastoreStorage` per iteration.

| Case | Before | After | Change |
|---|---|---|---|
| insert_entity_with_nested | 4.87 µs | 4.73 µs | **−2.77%** (p=0.00) |
