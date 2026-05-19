# Query bench: PR review fixes

Scope: measure the PR review fixes that can touch hot paths:

- `distinct_on` now treats the `u64` hash as a prefilter and verifies exact signature equality on hash matches.
- FULL query results now populate `EntityResult` metadata.
- commit mutation results now return the persisted entity version.

Commands:

```sh
git worktree add --detach /tmp/datastore-emulator-pr-head-perf 474bbe4927ebfecbfcaf74d4dca340a4c66c73ea

DATASTORE_QUERY_BENCH_SAMPLES=20 \
DATASTORE_QUERY_BENCH_MEASUREMENT_SECONDS=2 \
cargo bench --bench query_baseline -- <case>
```

The baseline was run from a detached worktree at `474bbe4927ebfecbfcaf74d4dca340a4c66c73ea`, the PR head before these review fixes. The "After" column was run from the working tree with the PR review fixes. Deltas are computed from the reported Criterion means.

| Case | HEAD | After | Change | Notes |
|---|---:|---:|---:|---|
| distinct_on_region_bucket | 14.440 ms | 14.785 ms | +2.39% | confidence intervals overlap |
| full_kind_ordered_limit_50 | 1.5030 ms | 1.5364 ms | +2.22% | confidence intervals overlap |
| update_one_field_many_indexes | 14.229 us | 14.213 us | -0.11% | no regression |
| insert_entity_with_nested | 3.6160 us | 3.6012 us | -0.41% | no regression |

Conclusion: the correctness fixes do not show a clear benchmark regression. The DISTINCT collision check has the expected small cost in the measured mean, but it stayed within overlapping intervals in this short targeted run.
