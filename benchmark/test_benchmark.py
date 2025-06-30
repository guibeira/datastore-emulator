import argparse
import statistics
import threading
import timeit
import uuid

from google.cloud import datastore
from google.cloud.datastore.query import PropertyFilter


def test_single_insert(client, test_id):
    """Tests inserting a single entity."""
    key = client.key("PerfTest", f"task-{test_id}-{uuid.uuid4()}")
    entity = client.entity(key)
    entity["test_id"] = test_id
    entity["value"] = str(uuid.uuid4())
    client.put(entity)


def test_bulk_insert(client, test_id, num_entities=50):
    """Tests inserting a batch of entities."""
    entities = []
    for i in range(num_entities):
        key = client.key("PerfTest", f"task-{test_id}-{uuid.uuid4()}-{i}")
        entity = client.entity(key)
        entity["test_id"] = test_id
        entities.append(entity)
    client.put_multi(entities)


def test_key_lookup(client, key):
    """Tests reading a single entity by its known key."""
    client.get(key)


def test_simple_query(client, test_id):
    """Tests a simple query to find records."""
    query = client.query(kind="PerfTest")
    query.add_filter(filter=PropertyFilter("test_id", "=", test_id))
    # Consume the generator to ensure the work is done
    list(query.fetch(limit=10))


def cleanup_data(client, test_id):
    """Deletes all data created during a test run."""
    query = client.query(kind="PerfTest")
    query.add_filter(filter=PropertyFilter("test_id", "=", test_id))
    query.keys_only()  # More efficient

    keys_to_delete = [entity.key for entity in query.fetch()]

    # client.delete_multi usually has a limit (e.g., 500)
    for i in range(0, len(keys_to_delete), 500):
        batch = keys_to_delete[i : i + 500]
        if batch:
            client.delete_multi(batch)
    print(f"   - Cleaned up {len(keys_to_delete)} entities.")


def run_single_client_benchmark(name, client, number_of_runs, results_list):
    """Runs the full benchmark suite for a single client."""
    print(f"--- Starting benchmark for one {name} client ---")
    client_results = {}
    test_run_id = f"perf-{uuid.uuid4()}"

    try:
        # --- Test 1: Single Inserts ---
        stmt = lambda: test_single_insert(client, test_run_id)
        time_taken = timeit.timeit(stmt, number=number_of_runs)
        client_results["Single Insert"] = time_taken

        # --- Test 2: Bulk Inserts (e.g., 50 entities at a time) ---
        stmt = lambda: test_bulk_insert(client, test_run_id, num_entities=50)
        time_taken = timeit.timeit(stmt, number=number_of_runs)
        client_results["Bulk Insert (50)"] = time_taken

        # --- Test 3: Simple Query ---
        stmt = lambda: test_simple_query(client, test_run_id)
        time_taken = timeit.timeit(stmt, number=number_of_runs)
        client_results["Simple Query"] = time_taken

    finally:
        # IMPORTANT: Clean up all data created by this benchmark run
        cleanup_data(client, test_run_id)

    print(f"--- Finished benchmark for one {name} client ---")
    results_list.append(client_results)


def run_benchmarks(num_clients=10, number_of_runs_per_client=100):
    # --- Create clients ---
    rust_clients = []
    for _ in range(num_clients):
        client = datastore.Client(project="test-project-2")
        client.base_url = "http://localhost:8042"
        rust_clients.append(client)

    java_clients = []
    for _ in range(num_clients):
        client = datastore.Client(project="test-project-2")
        client.base_url = "http://localhost:8044"
        java_clients.append(client)

    # --- Run benchmarks sequentially for each implementation ---
    rust_results = []
    java_results = []

    # 1. Run Rust benchmarks
    print(f"\n--- Starting Rust benchmarks with {num_clients} concurrent clients ---")
    threads = []
    for i, client in enumerate(rust_clients):
        thread = threading.Thread(
            target=run_single_client_benchmark,
            args=(f"rust-{i+1}", client, number_of_runs_per_client, rust_results),
        )
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
    print("--- Rust benchmarks finished ---")

    # 2. Run Java benchmarks
    print(f"\n--- Starting Java benchmarks with {num_clients} concurrent clients ---")
    threads = []
    for i, client in enumerate(java_clients):
        thread = threading.Thread(
            target=run_single_client_benchmark,
            args=(f"java-{i+1}", client, number_of_runs_per_client, java_results),
        )
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
    print("--- Java benchmarks finished ---")

    # --- Aggregate and Print Summary ---
    print("\n\n--- Benchmark Summary ---")

    # Assuming all runs have the same test names
    if not rust_results:
        print("No results for Rust to summarize.")
        return
    if not java_results:
        print("No results for Java to summarize.")
        return

    test_names = rust_results[0].keys()

    for test_name in test_names:
        rust_times = [r[test_name] for r in rust_results]
        java_times = [j[test_name] for j in java_results]

        total_rust_time = sum(rust_times)
        total_java_time = sum(java_times)

        print(f"\nOperation: {test_name}")
        print(f"  - Rust ({num_clients} clients, {number_of_runs_per_client} runs each):")
        print(f"    - Total time: {total_rust_time:.4f} seconds")
        print(f"    - Avg time per client: {statistics.mean(rust_times):.4f} seconds")

        print(f"  - Java ({num_clients} clients, {number_of_runs_per_client} runs each):")
        print(f"    - Total time: {total_java_time:.4f} seconds")
        print(f"    - Avg time per client: {statistics.mean(java_times):.4f} seconds")

        if total_rust_time < total_java_time:
            print(f"  - Verdict: Rust was {total_java_time/total_rust_time:.2f}x faster overall.")
        else:
            print(f"  - Verdict: Java was {total_rust_time/total_java_time:.2f}x faster overall.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run datastore benchmarks.")
    parser.add_argument(
        "--num-clients",
        type=int,
        default=10,
        help="Number of concurrent clients for each implementation (default: 10).",
    )
    parser.add_argument(
        "--num-runs",
        type=int,
        default=100,
        help="Number of test runs per client (default: 100).",
    )
    args = parser.parse_args()

    run_benchmarks(num_clients=args.num_clients, number_of_runs_per_client=args.num_runs)
