
### How to run the benchmark


First step is make sure you have python installed. This project uses poetry, so you need to install it first. You can find the installation instructions [here](https://python-poetry.org/docs/#installation). 

For convinience, you use a docker-compose file to spin up the datastore emulatores. This means, you need to have docker and docker-compose installed. You can find the installation instructions [here](https://docs.docker.com/get-docker/) and [here](https://docs.docker.com/compose/install/).


Once you have docker and docker-compose installed, you can run the following command to start the emulators:

```bash
docker compose up --build -d
```
This will start the emulators in the background. You can stop them by running:

```bash
docker compose down
```


Once you have poetry installed, you can install the dependencies by running the following commands in the project root directory:

```bash
poetry install
```

Build the load generator and run it against both emulators:

```bash
cargo build --release --features loadgen --bin loadgen
./target/release/loadgen --num-clients 10 --num-runs 20 --warmup-runs 5 --repeat 3
```

The load generator (`src/bin/loadgen.rs`) is a gRPC client on the emulator's
own generated `DatastoreClient`, so a call costs microseconds on the client
side and the emulators are what gets measured. `benchmark/test_benchmark.py`
runs the same scenarios through the Python `google-cloud-datastore` client and
shows what a Python application sees instead; with it the client is the
bottleneck (one core under the GIL, ~2 ms per call) and most scenarios come
out at 1x whatever the emulator does:

```bash
poetry run python benchmark/test_benchmark.py --num-clients 10 --num-runs 20 --warmup-runs 5 --repeat 3
```

Each client runs every scenario `--warmup-runs` times untimed, then `--num-runs`
times with each call timed; the whole run repeats `--repeat` times and the median
is kept. Before the clients start, the target is seeded with a shared dataset of
5,000 entities (10 groups of 500, random `score`); each client also gets its own
entity and 100 entities for `get_multi`. Everything is deleted at the end.

| Scenario | One call does |
|---|---|
| Single Insert | `put` of one new entity |
| Bulk Insert (50) | `put_multi` of 50 new entities |
| Simple Query | equality filter on the client's entities, limit 10 |
| Get | `get` of the client's entity |
| Get Multi (100) | `get_multi` of 100 keys |
| Update | `get` then `put` of the client's entity |
| Query Filter+Order (5k) | `group == g` ordered by `-score`, limit 20, over the seed |
| Cursor Pagination (5x100) | walks the client's group of 500 in 5 pages of 100 through cursors |
| Transaction RMW | begin, `get`, `put`, commit on the client's entity |
| Mixed (get, get_multi, update, query) | the four calls of an app-like request |

The insert scenarios are bound by the Python client (protobuf + gRPC), so
they barely tell the emulators apart; the others make the server work. The
summary shows, per scenario and emulator, the average time per client (the
dashboard's historical metric), the p50 and the p95 per call, and the Java/Rust
speedup:

```bash
--- Benchmark Summary ---

Operation: Single Insert
  - Rust (10 clients, 20 runs each): avg per client 0.1694s, p50 7.8 ms, p95 12.1 ms
  - Java (10 clients, 20 runs each): avg per client 0.5895s, p50 27.4 ms, p95 48.0 ms
  - Verdict: Rust p50 is 3.51x faster than Java.
```

`--target rust` or `--target java` benchmarks one emulator (`--host` overrides its
URL). If an emulator stops answering, the run aborts with exit code 2 and says
which scenario and how many clients failed. `benchmark/sample.py --container NAME
--json out.json -- <command>` runs either harness while sampling that emulator's
container through `docker stats` and adds mean and peak CPU (percent of one core)
and peak and final memory to the JSON (`test_benchmark.py --container` does the
same on its own).

### Emitting JSON for the continuous dashboard

`--json-out PATH` writes the per-target summaries. `merge_results.py` turns one or
more of those files into the `github-action-benchmark` files: `latency.json`
(`customSmallerIsBetter`: avg per client, p50, p95), `speedup.json`
(`customBiggerIsBetter`: Java/Rust p50 ratio, stable across runners of different
speed) and `resources.json` (`customSmallerIsBetter`: CPU and memory of each
container, one chart per metric with both emulators):

```bash
poetry run python benchmark/test_benchmark.py --target rust --json-out out/rust.json
poetry run python benchmark/test_benchmark.py --target java --json-out out/java.json
poetry run python benchmark/merge_results.py --out-dir out out/rust.json out/java.json
```

### Continuous benchmark dashboard

The `Release` workflow calls `.github/workflows/benchmark.yml` after the Docker
image is published. It pulls that image and the official emulator image
(`guibeira/datastore-emulator-google`, built from `Dockerfile.google` by
`.github/workflows/google-emulator-image.yml`), runs each emulator alone pinned
to two CPUs (`benchmark/compose.yml`) with the client on the other two, and
publishes the results to GitHub Pages via
[`benchmark-action/github-action-benchmark`][gab]:

<https://guibeira.github.io/datastore-emulator/dev/bench/>

If the Java emulator dies or hangs, the Rust numbers are still published and
the speedup chart skips that release. To run it by hand, use the **Actions**
tab (`Benchmark` → **Run workflow**) with one or more tags, oldest first, e.g.
`v0.1.6 v0.1.7`; `num_clients`, `num_runs` and `repeat` can be overridden. The
harness comes from the branch the workflow runs on, so old releases can be
benchmarked with the current scenarios to backfill the charts.

The page at `dev/bench/index.html` on `gh-pages` is customized to draw both
emulators on one chart per scenario; the action keeps it as it only writes
that file when it does not exist. The workflow builds the load generator once
(`build-loadgen`, cached with `Swatinem/rust-cache`) and each tag's job downloads
it as an artifact.

[gab]: https://github.com/benchmark-action/github-action-benchmark
