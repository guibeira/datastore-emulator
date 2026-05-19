# Delete bench: return Arc<EntityWithMetadata> from delete_entity

Optimization: `delete_entity` already owned the `Arc<EntityWithMetadata>` returned by `BTreeMap::remove`, but deep-cloned it via `.as_ref().clone()` before handing it back. The single caller (the `Operation::Delete` arm in `core::commit`) only accesses `version` and `create_time` on the result, both reachable through `Arc::deref`, so returning the `Arc` directly eliminates the full `EntityWithMetadata`+`Entity` clone on every delete.

Added a `delete_entity_with_nested` bench case (commit with `Operation::Delete` against a pre-seeded entity).

| Case | Before | After | Change |
|---|---|---|---|
| delete_entity_with_nested | 4.68 µs | 4.04 µs | **−13.5%** (p=0.00) |
