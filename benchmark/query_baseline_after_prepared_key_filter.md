# Query bench: prepared __key__ filter (pre-converted + cached candidate KeyStruct)

Optimization: top-level `__key__` filters (LT/LE/GT/GE/EQ/NE/IN/NOT_IN, excluding `HAS_ANCESTOR`) were going through `apply_filter -> match_key_against_filter`, which called `KeyStruct::from_datastore_key` twice per candidate entity — once on the entity key and once on the filter key — even though the filter key is constant across the whole query and the entity-side `KeyStruct` was already available either as the storage `BTreeMap` key (scan path) or as `candidate.key_struct` (index path).

Added a `PreparedKeyFilter` enum and a `try_prepare_key_filter` helper that pre-converts the filter side once. Both filter call sites now use the prepared form when available and compare against the already-materialized `KeyStruct`, bypassing `apply_filter`/`match_key_against_filter` entirely for the common `__key__` filter shape.

For `key_filter_greater_than` (10k entities, `__key__ > entity-005000`) this removes roughly `2 * 10_000` `KeyStruct::from_datastore_key` allocations per query (each builds a Vec<(String, KeyId)> plus several String clones).

| Case | Before | After | Change |
|---|---|---|---|
| key_filter_greater_than | 2.225 ms | 0.788 ms | **−64.7%** (p=0.00) |
