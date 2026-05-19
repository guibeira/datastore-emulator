# Lookup bench: get_entity returns Arc instead of cloning

Optimization: `DatastoreStorage::get_entity` previously did
`self.entities.get(&key_struct).map(|m| m.as_ref().clone())`, deep-cloning
the entire `EntityWithMetadata` (including the `Entity` proto and timestamps)
on every successful lookup. Since the storage map now holds
`Arc<EntityWithMetadata>`, returning a cloned `Arc` is a single atomic
increment. The single existing caller (`core::lookup`) only needs to clone
the `Entity`/`Timestamp` fields it actually puts in the response, so the
metadata clone was pure waste.

Bench case added: `lookup_batch_100` (100 keys per call, all present).

| Case | Before | After | Change |
|---|---|---|---|
| lookup_batch_100 | 155.0 µs | 104.8 µs | **−32.39%** (p=0.00) |
