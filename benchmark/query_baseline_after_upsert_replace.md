# Upsert bench: drop redundant clones in Operation::Upsert

Optimization: applied the same `std::mem::replace` pattern from the update path to the upsert Occupied branch, and stopped cloning the caller's `Key` up front when the caller already holds it by reference (`entity.key.as_ref()` instead of `entity.key.clone()`). Net effect: one fewer `Entity` clone and one fewer `Key` clone per upsert hit against an existing row.

Added a `upsert_existing_entity_with_nested` bench case (commit with `Operation::Upsert` over a pre-seeded entity).

| Case | Before | After | Change |
|---|---|---|---|
| upsert_existing_entity_with_nested | 7.61 µs | 6.99 µs | **−8.5%** (p=0.00) |
