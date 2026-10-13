"""Benchmark an emulator with N concurrent clients.

Each client runs every scenario `--warmup-runs` times untimed, then
`--num-runs` times with each call timed. The whole thing repeats `--repeat`
times and the median is kept. Per scenario the result is the mean per client
of its total time (the metric the dashboard has always shown) plus the p50
and p95 of all timed calls.

The insert scenarios are bound by the Python client (protobuf + gRPC), so
they barely tell the emulators apart. The rest make the server work: reads
of seeded entities, an update, a filtered and ordered query and a cursor
walk over a shared dataset of SEED_TOTAL entities, a read-modify-write
transaction, and an app-like mix.

    poetry run python benchmark/test_benchmark.py --target rust --json-out out/rust.json
    poetry run python benchmark/test_benchmark.py --target both   # both emulators up

`--target both` runs Rust then Java against the docker-compose ports. In CI
each emulator runs alone, pinned to its own CPUs, and the two JSON files are
combined by merge_results.py.
"""

import argparse
import json
import random
import statistics
import threading
import time
import uuid
from dataclasses import dataclass
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

KIND = "PerfTest"
GET_MULTI_KEYS = 100
QUERY_LIMIT = 20
# Shared dataset: SEED_GROUPS groups of SEED_TOTAL / SEED_GROUPS entities; the
# cursor walk pages through one group PAGE_SIZE at a time.
SEED_TOTAL = 5000
SEED_GROUPS = 10
PAGE_SIZE = 100


class EmulatorFailed(RuntimeError):
    """The emulator stopped answering; carries the partial per-client results."""

    def __init__(self, label, scenario, error, partial):
        super().__init__(f"{label} emulator failed during {scenario!r}: {error}")
        self.partial = partial


class ScenarioFailed(RuntimeError):
    """One client's scenario raised `error`; `released` when the error is the
    barrier broken by another client's failure."""

    def __init__(self, scenario, error):
        super().__init__(f"{type(error).__name__}: {error}")
        self.scenario = scenario
        self.released = isinstance(error, threading.BrokenBarrierError)


@dataclass
class Seed:
    """The shared dataset one target's clients query: `kind` holds `groups`
    groups of `per_group` entities with a random `score`."""

    kind: str
    groups: int
    per_group: int
    page_size: int


@dataclass
class ClientRun:
    """What one client's scenarios work on: its own entity (get, update,
    transaction), GET_MULTI_KEYS entities for get_multi, and the seed group
    it queries."""

    client: object
    run_id: str
    own_key: object
    multi_keys: list
    seed: Seed
    group: int


def seed_rows(total, groups, rng):
    return [
        {"name": f"item-{i}", "group": i % groups, "score": rng.randrange(1_000_000)}
        for i in range(total)
    ]


def put_in_chunks(client, entities, size=500):
    for i in range(0, len(entities), size):
        client.put_multi(entities[i : i + size], timeout=REQUEST_TIMEOUT)


def seed_dataset(client, total=SEED_TOTAL, groups=SEED_GROUPS, page_size=PAGE_SIZE):
    seed = Seed(kind=f"PerfSeed_{uuid.uuid4().hex}", groups=groups, per_group=total // groups, page_size=page_size)
    entities = []
    for row in seed_rows(total, groups, random.Random(42)):
        entity = client.entity(client.key(seed.kind, row["name"]))
        entity.update(row)
        entities.append(entity)
    try:
        put_in_chunks(client, entities)
    except Exception:
        # Commits are per chunk: drop what the earlier chunks left behind.
        try:
            cleanup_seed(client, seed)
        except Exception:  # noqa: BLE001 - the first error is the one to report
            pass
        raise
    return seed


def delete_kind(client, kind, filter_=None):
    query = client.query(kind=kind)
    if filter_ is not None:
        query.add_filter(filter=filter_)
    query.keys_only()
    keys = [entity.key for entity in query.fetch(timeout=REQUEST_TIMEOUT)]
    for i in range(0, len(keys), 500):
        client.delete_multi(keys[i : i + 500], timeout=REQUEST_TIMEOUT)


def cleanup_seed(client, seed):
    delete_kind(client, seed.kind)


def setup_client_run(client, index, seed, run_id=None):
    run_id = run_id or f"perf-{uuid.uuid4()}"
    own = client.entity(client.key(KIND, f"own-{run_id}"))
    own.update({"test_id": run_id, "counter": 0, "version": 0})
    multi = []
    for i in range(GET_MULTI_KEYS):
        entity = client.entity(client.key(KIND, f"multi-{run_id}-{i}"))
        entity.update({"test_id": run_id, "index": i})
        multi.append(entity)
    put_in_chunks(client, [own] + multi)
    return ClientRun(
        client=client,
        run_id=run_id,
        own_key=own.key,
        multi_keys=[entity.key for entity in multi],
        seed=seed,
        group=index % seed.groups,
    )


def cleanup_data(client, run_id):
    """Deletes what setup_client_run and the scenarios wrote for `run_id`."""
    delete_kind(client, KIND, PropertyFilter("test_id", "=", run_id))


def scenario_single_insert(run):
    entity = run.client.entity(run.client.key(KIND, f"task-{run.run_id}-{uuid.uuid4()}"))
    entity.update({"test_id": run.run_id, "value": str(uuid.uuid4())})
    run.client.put(entity, timeout=REQUEST_TIMEOUT)


def scenario_bulk_insert(run, num_entities=50):
    entities = []
    for i in range(num_entities):
        entity = run.client.entity(run.client.key(KIND, f"task-{run.run_id}-{uuid.uuid4()}-{i}"))
        entity["test_id"] = run.run_id
        entities.append(entity)
    run.client.put_multi(entities, timeout=REQUEST_TIMEOUT)


def scenario_simple_query(run):
    query = run.client.query(kind=KIND)
    query.add_filter(filter=PropertyFilter("test_id", "=", run.run_id))
    return list(query.fetch(limit=10, timeout=REQUEST_TIMEOUT))


def scenario_get(run):
    return run.client.get(run.own_key, timeout=REQUEST_TIMEOUT)


def scenario_get_multi(run):
    return run.client.get_multi(run.multi_keys, timeout=REQUEST_TIMEOUT)


def scenario_update(run):
    entity = run.client.get(run.own_key, timeout=REQUEST_TIMEOUT)
    entity["version"] += 1
    entity["updated"] = str(uuid.uuid4())
    run.client.put(entity, timeout=REQUEST_TIMEOUT)


def scenario_query_filter_order(run):
    query = run.client.query(kind=run.seed.kind)
    query.add_filter(filter=PropertyFilter("group", "=", run.group))
    query.order = ["-score"]
    return list(query.fetch(limit=QUERY_LIMIT, timeout=REQUEST_TIMEOUT))


def scenario_cursor_pagination(run):
    """Walks the client's seed group page by page through cursors; always the
    same number of requests, whatever the emulator says after the last page."""
    query = run.client.query(kind=run.seed.kind)
    query.add_filter(filter=PropertyFilter("group", "=", run.group))
    query.order = ["-score"]
    keys = []
    cursor = None
    for _ in range(run.seed.per_group // run.seed.page_size):
        iterator = query.fetch(limit=run.seed.page_size, start_cursor=cursor, timeout=REQUEST_TIMEOUT)
        keys.extend(entity.key for entity in next(iterator.pages))
        cursor = iterator.next_page_token
        if cursor is None:
            break
    return keys


def scenario_transaction_rmw(run):
    transaction = run.client.transaction()
    transaction.begin(timeout=REQUEST_TIMEOUT)
    try:
        entity = run.client.get(run.own_key, transaction=transaction, timeout=REQUEST_TIMEOUT)
        entity["counter"] += 1
        transaction.put(entity)
        transaction.commit(timeout=REQUEST_TIMEOUT)
    except Exception:
        transaction.rollback(timeout=REQUEST_TIMEOUT)
        raise


def scenario_mixed(run):
    """An app-like request: two reads, one update, one small query."""
    scenario_get(run)
    scenario_get_multi(run)
    scenario_update(run)
    scenario_simple_query(run)


SCENARIOS = [
    ("Single Insert", scenario_single_insert),
    ("Bulk Insert (50)", scenario_bulk_insert),
    ("Simple Query", scenario_simple_query),
    ("Get", scenario_get),
    ("Get Multi (100)", scenario_get_multi),
    ("Update", scenario_update),
    ("Query Filter+Order (5k)", scenario_query_filter_order),
    ("Cursor Pagination (5x100)", scenario_cursor_pagination),
    ("Transaction RMW", scenario_transaction_rmw),
    ("Mixed (get, get_multi, update, query)", scenario_mixed),
]


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


def run_scenarios(run, scenarios, num_runs, warmup_runs, barrier):
    """Times every scenario for one client. Clients meet at `barrier` before
    each scenario, so a slow scenario on one client never overlaps the next
    scenario's timings on another; a failing client breaks the barrier to
    release the others."""
    timings = {}
    name = "start"
    try:
        for name, scenario in scenarios:
            barrier.wait()
            call = lambda: scenario(run)  # noqa: E731
            for _ in range(warmup_runs):
                call()
            timings[name] = time_calls(call, num_runs)
    except Exception as error:
        barrier.abort()
        raise ScenarioFailed(name, error) from error
    return timings


def run_single_client(index, client, seed, barrier, num_runs, warmup_runs, results, failures):
    """Runs every scenario for one client; appends {scenario: timings}, or
    records failures[index] = (scenario, error) when the emulator stops answering."""
    run_id = f"perf-{uuid.uuid4()}"
    try:
        run = setup_client_run(client, index, seed, run_id)
        results.append(run_scenarios(run, SCENARIOS, num_runs, warmup_runs, barrier))
    except ScenarioFailed as failure:
        failures[index] = failure
    except Exception as error:  # noqa: BLE001 - any failure means the target is unusable
        barrier.abort()
        failures[index] = ScenarioFailed("setup", error)
    finally:
        try:
            cleanup_data(client, run_id)
        except Exception as error:  # noqa: BLE001
            failures.setdefault(index, ScenarioFailed("cleanup", error))


def run_once(label, base_url, seed, num_clients, num_runs, warmup_runs):
    """One pass with `num_clients` threads; returns {scenario: summary}."""
    results = []
    failures = {}
    barrier = threading.Barrier(num_clients)
    threads = [
        threading.Thread(
            target=run_single_client,
            args=(index, make_client(base_url), seed, barrier, num_runs, warmup_runs, results, failures),
        )
        for index in range(num_clients)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    if failures:
        # Clients released by the broken barrier only echo the first failure.
        causes = [f for f in failures.values() if not f.released] or list(failures.values())
        raise EmulatorFailed(
            label, causes[0].scenario, f"{causes[0]} ({len(causes)} of {num_clients} clients failed)", results
        )

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
    client = make_client(base_url)
    try:
        started = time.perf_counter()
        seed = seed_dataset(client)
        print(f"  seeded {SEED_TOTAL} entities in {time.perf_counter() - started:.1f}s")
    except Exception as error:  # noqa: BLE001
        raise EmulatorFailed(label, "seed", f"{type(error).__name__}: {error}", []) from error
    try:
        repeats = []
        for i in range(repeat):
            started = time.perf_counter()
            repeats.append(run_once(label, base_url, seed, num_clients, num_runs, warmup_runs))
            print(f"  repeat {i + 1}/{repeat} done in {time.perf_counter() - started:.1f}s")
    finally:
        try:
            cleanup_seed(client, seed)
        except Exception as error:  # noqa: BLE001 - the emulator is already gone
            print(f"  seed cleanup failed: {type(error).__name__}: {error}")
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
