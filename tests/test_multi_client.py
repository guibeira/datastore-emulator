import os
import uuid

from google.cloud import datastore


def test_multi_client_insert_isolation():
    """
    Tests whether two clients connected to different emulator instances
    operate on isolated datasets, must return the same result.
    """
    os.environ["DATASTORE_EMULATOR_HOST"] = "localhost:8042"
    rust_client = datastore.Client(project="test-project-1")

    os.environ["DATASTORE_EMULATOR_HOST"] = "localhost:8044"
    google_client = datastore.Client(project="test-project-2")

    # Create unique keys for each client
    key1 = rust_client.key("TestKind", f"test-entity-{uuid.uuid4()}")
    key2 = google_client.key("TestKind", f"test-entity-{uuid.uuid4()}")

    try:
        # --- Action: Insert data into each emulator ---

        # client1 will communicate with localhost:8042
        entity1 = datastore.Entity(key=key1)
        entity1["description"] = "Description"
        rust_client.put(entity1)

        # client2 will communicate with localhost:8044
        entity2 = datastore.Entity(key=key2)
        entity2["description"] = "Description"
        google_client.put(entity2)

        # Check emulator 1 using client1
        retrieved_from_1_for_key1 = rust_client.get(key1)

        # Check emulator 2 using client2
        retrieved_from_2_for_key2 = google_client.get(key2)

        assert retrieved_from_1_for_key1["description"] == retrieved_from_2_for_key2["description"]
    finally:
        # --- Cleanup ---
        # Each client remains connected to its original emulator host.
        if "key1" in locals() and rust_client.get(key1):
            rust_client.delete(key1)

        if "key2" in locals() and google_client.get(key2):
            google_client.delete(key2)


def test_multi_client_filter_query():
    """
    Tests whether a filter query returns the same result from both emulators.
    """
    os.environ["DATASTORE_EMULATOR_HOST"] = "localhost:8042"
    rust_client = datastore.Client(project="test-project-1")

    os.environ["DATASTORE_EMULATOR_HOST"] = "localhost:8044"
    google_client = datastore.Client(project="test-project-2")

    test_id = f"test-{uuid.uuid4()}"
    keys_to_delete_rust = []
    keys_to_delete_google = []

    try:
        # --- Action: Insert data for filtering ---
        # Create entities with a common property for filtering
        for client in [rust_client, google_client]:
            keys_to_delete = keys_to_delete_rust if client == rust_client else keys_to_delete_google
            for i in range(3):
                key = client.key("Task", f"task-{test_id}-{i}")
                entity = datastore.Entity(key=key)
                entity.update({
                    "category": f"category-{test_id}",
                    "done": i % 2 == 0,  # Two done, one not done
                })
                client.put(entity)
                keys_to_delete.append(key)

        # --- Query with filter ---
        # Rust client
        rust_query = rust_client.query(kind="Task")
        rust_query.add_filter("category", "=", f"category-{test_id}")
        rust_query.add_filter("done", "=", True)
        rust_results = list(rust_query.fetch())

        # Google client
        google_query = google_client.query(kind="Task")
        google_query.add_filter("category", "=", f"category-{test_id}")
        google_query.add_filter("done", "=", True)
        google_results = list(google_query.fetch())

        # --- Assertion ---
        assert len(rust_results) == len(google_results)
        assert len(rust_results) == 2  # Based on the loop i % 2 == 0 for i in 0, 1, 2

        # Sort results to ensure order doesn't affect comparison
        rust_keys = sorted([e.key.name for e in rust_results])
        google_keys = sorted([e.key.name for e in google_results])
        assert rust_keys == google_keys

    finally:
        # --- Cleanup ---
        if keys_to_delete_rust:
            rust_client.delete_multi(keys_to_delete_rust)
        if keys_to_delete_google:
            google_client.delete_multi(keys_to_delete_google)


def test_multi_client_aggregation_query():
    """
    Tests whether an aggregation query (COUNT) returns the same result from both emulators.
    """
    os.environ["DATASTORE_EMULATOR_HOST"] = "localhost:8042"
    rust_client = datastore.Client(project="test-project-1")

    os.environ["DATASTORE_EMULATOR_HOST"] = "localhost:8044"
    google_client = datastore.Client(project="test-project-2")

    test_id = f"test-{uuid.uuid4()}"
    keys_to_delete_rust = []
    keys_to_delete_google = []

    try:
        # --- Action: Insert data for aggregation ---
        for client in [rust_client, google_client]:
            keys_to_delete = keys_to_delete_rust if client == rust_client else keys_to_delete_google
            for i in range(5):
                key = client.key("DataPoint", f"dp-{test_id}-{i}")
                entity = datastore.Entity(key=key)
                entity.update({
                    "group": f"group-{test_id}",
                    "value": i * 10,
                })
                client.put(entity)
                keys_to_delete.append(key)

        # --- Aggregation Query (COUNT) ---
        # Rust client
        rust_base_query = rust_client.query(kind="DataPoint")
        rust_base_query.add_filter("group", "=", f"group-{test_id}")
        rust_agg_query = rust_client.aggregation_query(rust_base_query).count(alias="total")
        rust_result_iter = rust_agg_query.fetch()
        rust_count = next(rust_result_iter)[0].value

        # Google client
        google_base_query = google_client.query(kind="DataPoint")
        google_base_query.add_filter("group", "=", f"group-{test_id}")
        google_agg_query = google_client.aggregation_query(google_base_query).count(alias="total")
        google_result_iter = google_agg_query.fetch()
        google_count = next(google_result_iter)[0].value

        # --- Assertion ---
        assert rust_count == google_count
        assert rust_count == 5

    finally:
        # --- Cleanup ---
        if keys_to_delete_rust:
            rust_client.delete_multi(keys_to_delete_rust)
        if keys_to_delete_google:
            google_client.delete_multi(keys_to_delete_google)
