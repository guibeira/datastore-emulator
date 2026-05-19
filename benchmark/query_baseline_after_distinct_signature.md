# Query bench: distinct_on signature via bincode/prost bytes

Optimization: replaced the per-candidate `Vec<String>` signature (built with `format!("{:?}", ...)` and joined with `|`) with a single `Vec<u8>` populated by `prost::Message::encode_length_delimited` for property `Value`s and `bincode::serde::encode_into_std_write` for `KeyStruct`. Debug formatting recurses the entire protobuf tree and allocates many small strings; the binary encoding writes raw field bytes into a single buffer.

Bench case added: `distinct_on_region_bucket` (10k entities, distinct on `region` and `bucket`).

| Case | Before | After | Change |
|---|---|---|---|
| distinct_on_region_bucket | 16.7 ms | 12.3 ms | **−26.2%** (p=0.00) |
