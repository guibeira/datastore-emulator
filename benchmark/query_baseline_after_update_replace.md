# Update bench: drop redundant clones in Operation::Update

Optimization: the `Operation::Update` arm in `core::commit` was doing three clones of the entity payload per update:

1. `existing_entity_metadata.entity.clone()` for the eventual `remove_from_indexes` call,
2. `entity.clone()` to overwrite the stored entity,
3. `existing_entity_metadata.entity.clone()` again to feed `update_indexes`.

The first and third can be folded into a single `std::mem::replace(&mut existing_entity_metadata.entity, entity.clone())`: the replaced value is exactly the previous entity (used for `remove_from_indexes`), and the freshly-installed entity is structurally identical to the caller's `&Entity`, so `update_indexes` can just borrow the caller's reference. That trims the per-update Entity clone count from three to one.

Added a `update_entity_with_nested` bench case (commit with `Operation::Update` against a pre-seeded entity, using `iter_batched` to re-seed the storage between iterations).

| Case | Before | After | Change |
|---|---|---|---|
| update_entity_with_nested | 8.18 µs | 6.96 µs | **−15.8%** (p=0.00) |
