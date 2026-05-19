# Insert bench: share Arc<EntityWithMetadata> between storage and return value

Optimization: `insert_entity` was building `EntityWithMetadata` on the stack, cloning it once for the BTreeMap (`Arc::new(entity_metadata.clone())`) and returning the original by value. The contained `Entity` proto was also cloned twice (once into `db_entity`, once again into the metadata). Restructured to:

- skip the upfront `original_key.clone()` (the key was only borrowed)
- build the metadata directly inside `Arc::new(...)`, moving `db_entity` in
- return `Arc<EntityWithMetadata>` so the caller shares the storage Arc instead of taking a deep copy
- look up `observe_key_id` via the Arc'd entity key

`core::commit` callers already accessed `metadata.version`/`create_time`/`update_time` through field access, so the switch from owned to `Arc` was a one-line type change without further edits.

| Case | Before | After | Change |
|---|---|---|---|
| insert_entity_with_nested | 4.82 µs | 3.53 µs | **−26.8%** (p=0.00) |
