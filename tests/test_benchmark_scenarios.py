"""Benchmark scenarios do what their charts claim, on both emulators.

Needs the emulators on RUST_DATASTORE_EMULATOR_HOST (localhost:8042) and
GOOGLE_DATASTORE_EMULATOR_HOST (localhost:8044), like the parity tests.
"""

import os
import random
import threading
import time

import pytest

from benchmark.test_benchmark import (
    GET_MULTI_KEYS,
    QUERY_LIMIT,
    cleanup_data,
    cleanup_seed,
    make_client,
    scenario_cursor_pagination,
    scenario_get,
    scenario_get_multi,
    scenario_mixed,
    scenario_query_filter_order,
    scenario_transaction_rmw,
    scenario_update,
    seed_dataset,
    seed_rows,
    setup_client_run,
)

HOSTS = {
    "rust": os.environ.get("RUST_DATASTORE_EMULATOR_HOST", "localhost:8042"),
    "java": os.environ.get("GOOGLE_DATASTORE_EMULATOR_HOST", "localhost:8044"),
}


def test_seed_rows_spread_the_groups_evenly():
    rows = seed_rows(total=100, groups=4, rng=random.Random(1))

    assert len(rows) == 100
    assert len({row["name"] for row in rows}) == 100
    assert all(sum(1 for row in rows if row["group"] == g) == 25 for g in range(4))
    assert all(isinstance(row["score"], int) for row in rows)


@pytest.fixture(scope="module", params=list(HOSTS))
def client(request):
    return make_client("http://" + HOSTS[request.param])


@pytest.fixture(scope="module")
def seed(client):
    seed = seed_dataset(client, total=100, groups=2, page_size=10)
    yield seed
    cleanup_seed(client, seed)


@pytest.fixture
def run(client, seed):
    run = setup_client_run(client, index=1, seed=seed)
    yield run
    cleanup_data(client, run.run_id)


def test_get_returns_the_client_entity(run):
    entity = scenario_get(run)

    assert entity.key == run.own_key
    assert entity["counter"] == 0


def test_get_multi_returns_every_key(run):
    entities = scenario_get_multi(run)

    assert len(entities) == GET_MULTI_KEYS
    assert {e.key for e in entities} == set(run.multi_keys)


def test_update_bumps_the_version(run):
    scenario_update(run)
    scenario_update(run)

    assert run.client.get(run.own_key)["version"] == 2


def test_transaction_increments_the_counter(run):
    scenario_transaction_rmw(run)
    scenario_transaction_rmw(run)

    assert run.client.get(run.own_key)["counter"] == 2


def test_query_filter_order_returns_the_group_sorted_by_score(run):
    entities = scenario_query_filter_order(run)

    assert len(entities) == QUERY_LIMIT
    assert all(e["group"] == run.group for e in entities)
    scores = [e["score"] for e in entities]
    assert scores == sorted(scores, reverse=True)


def test_cursor_pagination_walks_the_whole_group(run):
    keys = scenario_cursor_pagination(run)

    assert len(keys) == run.seed.per_group
    assert len(set(keys)) == run.seed.per_group


def test_mixed_workload_updates_the_client_entity(run):
    scenario_mixed(run)

    assert run.client.get(run.own_key)["version"] == 1


def test_cleanup_removes_the_seed_and_the_client_data(client):
    seed = seed_dataset(client, total=20, groups=2, page_size=10)
    run = setup_client_run(client, index=0, seed=seed)

    cleanup_data(client, run.run_id)
    cleanup_seed(client, seed)

    assert client.get_multi([run.own_key] + run.multi_keys) == []
    assert list(client.query(kind=seed.kind).fetch(limit=1)) == []


def test_run_scenarios_keeps_clients_on_the_same_scenario():
    """No client starts scenario k+1 before every client finished scenario k,
    so a slow scenario on one client cannot leak into the next one's timings."""
    from benchmark.test_benchmark import run_scenarios

    clients = 3
    barrier = threading.Barrier(clients)
    events = []
    lock = threading.Lock()

    def scenario(name, delay):
        def call(run):
            start = time.perf_counter()
            time.sleep(delay * run)
            with lock:
                events.append((name, run, start, time.perf_counter()))
        return call

    scenarios = [("slow", scenario("slow", 0.01)), ("fast", scenario("fast", 0.0))]
    results = {}

    def worker(index):
        results[index] = run_scenarios(index + 1, scenarios, num_runs=2, warmup_runs=1, barrier=barrier)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    slow_end = max(end for name, _, _, end in events if name == "slow")
    fast_start = min(start for name, _, start, _ in events if name == "fast")
    assert slow_end <= fast_start
    assert all(set(r) == {"slow", "fast"} and len(r["slow"]) == 2 for r in results.values())


def test_run_scenarios_releases_the_others_when_one_client_fails():
    from benchmark.test_benchmark import ScenarioFailed, run_scenarios

    barrier = threading.Barrier(2)

    def boom(run):
        if run == "bad":
            raise RuntimeError("down")

    scenarios = [("boom", boom), ("next", lambda run: None)]
    errors = {}

    def worker(run):
        try:
            run_scenarios(run, scenarios, num_runs=1, warmup_runs=0, barrier=barrier)
        except ScenarioFailed as failure:
            errors[run] = (failure.scenario, failure.released)

    threads = [threading.Thread(target=worker, args=(run,)) for run in ("bad", "good")]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert not any(t.is_alive() for t in threads)
    assert errors == {"bad": ("boom", False), "good": ("next", True)}


@pytest.fixture(scope="module")
def rust_client():
    return make_client("http://" + HOSTS["rust"])


def _put_first_then_fail(client, entities, size=500):
    client.put_multi(entities[:1])
    raise RuntimeError("emulator gone")


def test_seed_dataset_removes_a_partial_seed_when_seeding_fails(rust_client, monkeypatch):
    import benchmark.test_benchmark as harness

    written = []
    monkeypatch.setattr(harness, "put_in_chunks", lambda c, e, size=500: (written.extend(e[:1]), _put_first_then_fail(c, e)))

    with pytest.raises(RuntimeError, match="emulator gone"):
        seed_dataset(rust_client, total=20, groups=2, page_size=10)

    assert rust_client.get(written[0].key) is None


def test_run_single_client_cleans_up_when_setup_fails(rust_client, monkeypatch):
    import benchmark.test_benchmark as harness

    seed = seed_dataset(rust_client, total=20, groups=2, page_size=10)
    written = []
    monkeypatch.setattr(harness, "put_in_chunks", lambda c, e, size=500: (written.extend(e[:1]), _put_first_then_fail(c, e)))
    results, failures = [], {}

    harness.run_single_client(0, rust_client, seed, threading.Barrier(1), 1, 0, results, failures)

    assert results == []
    assert failures[0].scenario == "setup" and "emulator gone" in str(failures[0])
    assert rust_client.get(written[0].key) is None
    monkeypatch.undo()
    cleanup_seed(rust_client, seed)
