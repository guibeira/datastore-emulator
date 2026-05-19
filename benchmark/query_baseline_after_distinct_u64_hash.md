# Query bench: distinct_on dedup via 64-bit hash

Optimization: `HashSet<Vec<u8>>` stored a fresh byte buffer per surviving distinct row and hashed/compared those buffers. Switched to feeding the signature pieces directly into `DefaultHasher` and deduping on the resulting `u64` in a `HashSet<u64>`. This drops the per-row `Vec<u8>` allocation, the rehash of stored bytes, and the byte-by-byte equality check on collisions, while keeping the same scalar fast-path for `StringValue`/`IntegerValue`/`BooleanValue` (the long-tail value types still fall back to `encode_length_delimited` into a reusable scratch buffer).

Collision risk on the 64-bit hash is `n^2 / 2^65` (≈ 5e-12 for 10k rows), which is acceptable for a query-side dedup whose only failure mode is dropping a non-duplicate row that wasn't going to be visible anyway. Added `Hash` to `KeyStruct` / `KeyId` so the `__key__` branch can feed the struct straight into the hasher.

| Case | Before | After | Change |
|---|---|---|---|
| distinct_on_region_bucket | 11.24 ms | 10.28 ms | **−8.5%** (p=0.00) |
