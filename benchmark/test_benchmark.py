"""Benchmark an emulator with N concurrent clients.

Each client runs every scenario `--warmup-runs` times untimed, then
`--num-runs` times with each call timed. The whole thing repeats `--repeat`
times and the median is kept. Per scenario the result is the mean per client
of its total time (the metric the dashboard has always shown) plus the p50
and p95 of all timed calls.

    poetry run python benchmark/test_benchmark.py --target rust --json-out out/rust.json
    poetry run python benchmark/test_benchmark.py --target both   # both emulators up

`--target both` runs Rust then Java against the docker-compose ports. In CI
each emulator runs alone, pinned to its own CPUs, and the two JSON files are
combined by merge_results.py.
"""

import argparse
import json
import statistics
import threading
import time
import uuid
from pathlib import Path

from google.auth.credentials import AnonymousCredentials
from google.cloud import datastore
from google.cloud.datastore.query import PropertyFilter

import sys

# Runnable as `python benchmark/<script>.py` from anywhere: import from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from benchmark.stats import median_of_summaries, merge_entries, summarize

TARGETS = {
    "rust": ("Rust", "http://localhost:8042"),
    "java": ("Java", "http://localhost:8044"),
}

# Per-request deadline, so a hung emulator fails the run instead of stalling it.
REQUEST_TIMEOUT = 60.0


class EmulatorFailed(RuntimeError):
    """The emulator stopped answering; carries the partial per-client results."""

    def __init__(self, label, scenario, error, partial):
        super().__init__(f"{label} emulator failed during {scenario!r}: {error}")
        self.partial = partial


def test_single_insert(client, test_id):
    """Tests inserting a single entity."""
    key = client.key("PerfTest", f"task-{test_id}-{uuid.uuid4()}")
    entity = client.entity(key)
    entity["test_id"] = test_id
    entity["value"] = str(uuid.uuid4())
    client.put(entity, timeout=REQUEST_TIMEOUT)


def test_bulk_insert(client, test_id, num_entities=50):
    """Tests inserting a batch of entities."""
    entities = []
    for i in range(num_entities):
        key = client.key("PerfTest", f"task-{test_id}-{uuid.uuid4()}-{i}")
        entity = client.entity(key)
        entity["test_id"] = test_id
        entities.append(entity)
    client.put_multi(entities, timeout=REQUEST_TIMEOUT)


def test_simple_query(client, test_id):
    """Tests a simple query to find records."""
    query = client.query(kind="PerfTest")
    query.add_filter(filter=PropertyFilter("test_id", "=", test_id))
    # Consume the generator to ensure the work is done
    list(query.fetch(limit=10, timeout=REQUEST_TIMEOUT))


SCENARIOS = [
    ("Single Insert", test_single_insert),
    ("Bulk Insert (50)", test_bulk_insert),
    ("Simple Query", test_simple_query),
]


def cleanup_data(client, test_id):
    """Deletes all data created during a test run."""
    query = client.query(kind="PerfTest")
    query.add_filter(filter=PropertyFilter("test_id", "=", test_id))
    query.keys_only()

    keys_to_delete = [entity.key for entity in query.fetch(timeout=REQUEST_TIMEOUT)]
    for i in range(0, len(keys_to_delete), 500):
        batch = keys_to_delete[i : i + 500]
        if batch:
            client.delete_multi(batch, timeout=REQUEST_TIMEOUT)


def make_client(base_url):
    client = datastore.Client(project="test-project-2", credentials=AnonymousCredentials())
    client.base_url = base_url
    return client


def wait_until_ready(label, base_url, timeout=120.0):
    """A bound port is not readiness: the Java emulator resets connections
    while it starts. Poll with a lookup until one succeeds."""
    client = make_client(base_url)
    deadline = time.monotonic() + timeout
    last_error = "no attempt"
    while time.monotonic() < deadline:
        try:
            client.get(client.key("Healthcheck", "ping"), timeout=5)
            return
        except Exception as error:  # noqa: BLE001 - not ready yet
            last_error = f"{type(error).__name__}: {error}"
            time.sleep(2)
    raise EmulatorFailed(label, "startup", f"not ready after {timeout:.0f}s: {last_error}", [])


def time_calls(fn, runs):
    timings = []
    for _ in range(runs):
        start = time.perf_counter()
        fn()
        timings.append(time.perf_counter() - start)
    return timings


def run_single_client(index, client, num_runs, warmup_runs, results, failures):
    """Runs every scenario for one client; appends {scenario: timings}, or
    records failures[index] = (scenario, error) when the emulator stops answering."""
    test_run_id = f"perf-{uuid.uuid4()}"
    client_results = {}
    name = "setup"
    try:
        for name, scenario in SCENARIOS:
            call = lambda: scenario(client, test_run_id)  # noqa: E731
            for _ in range(warmup_runs):
                call()
            client_results[name] = time_calls(call, num_runs)
        results.append(client_results)
    except Exception as error:  # noqa: BLE001 - any failure means the target is unusable
        failures[index] = (name, f"{type(error).__name__}: {error}")
    finally:
        try:
            cleanup_data(client, test_run_id)
        except Exception as error:  # noqa: BLE001
            failures.setdefault(index, ("cleanup", f"{type(error).__name__}: {error}"))


def run_once(label, base_url, num_clients, num_runs, warmup_runs):
    """One pass with `num_clients` threads; returns {scenario: summary}."""
    results = []
    failures = {}
    threads = [
        threading.Thread(
            target=run_single_client,
            args=(index, make_client(base_url), num_runs, warmup_runs, results, failures),
        )
        for index in range(num_clients)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    if failures:
        scenario, error = next(iter(failures.values()))
        raise EmulatorFailed(label, scenario, f"{error} ({len(failures)} of {num_clients} clients failed)", results)

    summary = {}
    for name, _ in SCENARIOS:
        per_client = [summarize(r[name]) for r in results]
        pooled = summarize([t for r in results for t in r[name]])
        summary[name] = {
            "total": statistics.fmean(s["total"] for s in per_client),
            "p50": pooled["p50"],
            "p95": pooled["p95"],
        }
    return summary


def run_target(label, base_url, num_clients, num_runs, warmup_runs, repeat):
    print(f"\n--- {label} at {base_url}: {num_clients} clients, {num_runs} runs/client, "
          f"{warmup_runs} warm-up, {repeat} repeat(s) ---")
    wait_until_ready(label, base_url)
    repeats = []
    for i in range(repeat):
        started = time.perf_counter()
        repeats.append(run_once(label, base_url, num_clients, num_runs, warmup_runs))
        print(f"  repeat {i + 1}/{repeat} done in {time.perf_counter() - started:.1f}s")
    return {name: median_of_summaries([r[name] for r in repeats]) for name, _ in SCENARIOS}


def print_summary(results_by_target, num_clients, num_runs):
    print("\n--- Benchmark Summary ---")
    scenarios = next(iter(results_by_target.values())).keys()
    for scenario in scenarios:
        print(f"\nOperation: {scenario}")
        for label, results in results_by_target.items():
            r = results[scenario]
            print(f"  - {label} ({num_clients} clients, {num_runs} runs each): "
                  f"avg per client {r['total']:.4f}s, p50 {r['p50']*1000:.1f} ms, p95 {r['p95']*1000:.1f} ms")
        if {"Rust", "Java"} <= results_by_target.keys():
            ratio = results_by_target["Java"][scenario]["p50"] / results_by_target["Rust"][scenario]["p50"]
            print(f"  - Verdict: Rust p50 is {ratio:.2f}x faster than Java.")


def main():
    parser = argparse.ArgumentParser(description="Run datastore benchmarks.")
    parser.add_argument("--target", choices=["rust", "java", "both"], default="both")
    parser.add_argument("--host", help="Base URL override, e.g. http://localhost:8042")
    parser.add_argument("--num-clients", type=int, default=10)
    parser.add_argument("--num-runs", type=int, default=100, help="Timed runs per client per scenario.")
    parser.add_argument("--warmup-runs", type=int, default=3, help="Untimed runs before timing.")
    parser.add_argument("--repeat", type=int, default=1, help="Whole-run repeats; the median is kept.")
    parser.add_argument("--json-out", help="Write the per-target summaries as JSON for merge_results.py.")
    args = parser.parse_args()

    targets = ["rust", "java"] if args.target == "both" else [args.target]
    if args.host and len(targets) > 1:
        parser.error("--host needs a single --target")

    results_by_target = {}
    for target in targets:
        label, default_url = TARGETS[target]
        try:
            results_by_target[label] = run_target(
                label, args.host or default_url, args.num_clients, args.num_runs, args.warmup_runs, args.repeat
            )
        except EmulatorFailed as failure:
            print(f"\n::error::{failure}")
            print(f"Clients that finished before the failure: {len(failure.partial)}")
            raise SystemExit(2) from failure

    print_summary(results_by_target, args.num_clients, args.num_runs)

    if args.json_out:
        out = Path(args.json_out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps({
            "settings": {
                "num_clients": args.num_clients,
                "num_runs": args.num_runs,
                "warmup_runs": args.warmup_runs,
                "repeat": args.repeat,
            },
            "targets": results_by_target,
        }, indent=2))
        print(f"\nWrote {out}")
        if len(results_by_target) > 1:
            extra = f"{args.num_clients} clients, {args.num_runs} runs/client"
            latency, _ = merge_entries(results_by_target, extra)
            print(f"({len(latency)} dashboard entries available via merge_results.py)")


if __name__ == "__main__":
    main()
