# Query bench: distinct_on signature fast-path for scalar Values

Optimization: the distinct_on signature builder always called `value.encode_length_delimited(&mut signature)`, which routes through prost's full proto-encoding machinery (tag + wire-type + varint length + payload) for every property. For the common scalar types (`StringValue`, `IntegerValue`, `BooleanValue`) we can write a much tighter representation directly into the signature buffer (`u32 len + bytes` for strings, raw little-endian bytes for ints/bools) and reserve the prost path for the long-tail value types.

The signature only needs to be a unique, deterministic byte sequence per logical (distinct_on) tuple — the encoding format never crosses a process boundary, so swapping in a hand-rolled scalar layout is safe.

| Case | Before | After | Change |
|---|---|---|---|
| distinct_on_region_bucket | 13.37 ms | 11.34 ms | **−15.2%** (p=0.00) |
