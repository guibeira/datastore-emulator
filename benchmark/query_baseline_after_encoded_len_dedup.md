# Query bench: reuse cached encoded_len in pagination loop

Optimization: `get_entities_from_candidates` was calling `entity.encoded_len()` twice per result — once as `entity_size_bytes` for the payload-size break check and again on the next line as `current_entities_payload_size += entity.encoded_len()`. `encoded_len()` walks the entire proto, so dropping the second call halves the encoding cost per emitted result.

| Case | Before | After | Change |
|---|---|---|---|
| full_kind_ordered_limit_50 | 1.973 ms | 1.529 ms | **−17.4%** (p=0.00) |
| keys_only_offset_500 | 1.074 ms | 0.982 ms | **−11.4%** (p=0.00) |
| distinct_on_region_bucket | 12.078 ms | 11.293 ms | **−6.5%** (p=0.00) |
| key_filter_greater_than | 0.792 ms | 0.784 ms | no change |
| lookup_batch_100 | 101.4 µs | 102.5 µs | noise |
| insert_entity_with_nested | 4.76 µs | 4.84 µs | noise |
