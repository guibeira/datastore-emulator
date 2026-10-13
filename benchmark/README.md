
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

Once the dependencies are installed, run the benchmark against both emulators:

```bash
poetry run python benchmark/test_benchmark.py --num-clients 10 --num-runs 20 --warmup-runs 5 --repeat 3
```

Each client runs every scenario `--warmup-runs` times untimed, then `--num-runs`
times with each call timed; the whole run repeats `--repeat` times and the median
is kept. The summary shows, per scenario and emulator, the average time per client
(the dashboard's historical metric), the p50 and the p95 per call, and the
Java/Rust speedup:

```bash
--- Benchmark Summary ---

Operation: Single Insert
  - Rust (10 clients, 20 runs each): avg per client 0.1694s, p50 7.8 ms, p95 12.1 ms
  - Java (10 clients, 20 runs each): avg per client 0.5895s, p50 27.4 ms, p95 48.0 ms
  - Verdict: Rust p50 is 3.51x faster than Java.
```

`--target rust` or `--target java` benchmarks one emulator (`--host` overrides its
URL). If an emulator stops answering, the run aborts with exit code 2 and says
which scenario and how many clients failed.

### Emitting JSON for the continuous dashboard

`--json-out PATH` writes the per-target summaries. `merge_results.py` turns one or
more of those files into the two `github-action-benchmark` files: `latency.json`
(`customSmallerIsBetter`: avg per client, p50, p95) and `speedup.json`
(`customBiggerIsBetter`: Java/Rust p50 ratio, stable across runners of different
speed):

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
the speedup chart skips that release. To re-run a release by hand, use the
**Actions** tab (`Benchmark` → **Run workflow**) with its tag; `num_clients`,
`num_runs` and `repeat` can be overridden.

The page at `dev/bench/index.html` on `gh-pages` is customized to draw both
emulators on one chart per scenario; the action keeps it as it only writes
that file when it does not exist.

[gab]: https://github.com/benchmark-action/github-action-benchmark
