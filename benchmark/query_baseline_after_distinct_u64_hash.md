# Query bench: distinct_on dedup via 64-bit hash

Optimization: `HashSet<Vec<u8>>` stored a fresh byte buffer per surviving distinct row and hashed/compared those buffers. Switched to feeding the signature pieces directly into `DefaultHasher` and using the resulting `u64` as a prefilter into stored canonical signatures. On hash matches, the implementation verifies exact signature equality before treating the row as a duplicate. This drops the per-row `Vec<u8>` allocation while keeping correctness on hash collisions and preserving the same scalar fast-path for `StringValue`/`IntegerValue`/`BooleanValue` (the long-tail value types still fall back to `encode_length_delimited` into a reusable scratch buffer).

The `u64` hash is not a correctness boundary; it only narrows the exact comparison set. Added `Hash` to `KeyStruct` / `KeyId` so the `__key__` branch can feed the struct straight into the hasher.

| Case | Before | After | Change |
|---|---|---|---|
| distinct_on_region_bucket | 11.24 ms | 10.28 ms | **−8.5%** (p=0.00) |
