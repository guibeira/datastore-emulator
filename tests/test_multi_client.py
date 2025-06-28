import os
import uuid

import pytest
from google.cloud import datastore


@pytest.fixture
def rust_client():
    """
    Fixture to create a Rust client connected to the emulator.
    """
    os.environ["DATASTORE_EMULATOR_HOST"] = "localhost:8042"
    db_client = datastore.Client(project="test-project-2")
    yield db_client
    # Cleanup after test
    for key in db_client.query(kind="Task").fetch():
        db_client.delete(key.key)


@pytest.fixture
def google_client():
    """
    Fixture to create a Google client connected to the emulator.
    """
    os.environ["DATASTORE_EMULATOR_HOST"] = "localhost:8044"
    db_client = datastore.Client(project="test-project-2")
    yield db_client
    # Cleanup after test
    for key in db_client.query(kind="Task").fetch():
        db_client.delete(key.key)


def test_multi_client_insert_isolation(rust_client, google_client):
    """
    Tests whether two clients connected to different emulator instances
    operate on isolated datasets, must return the same result.
    """
    # Create unique keys for each client
    key1 = rust_client.key("Task", f"test-entity-{uuid.uuid4()}")
    key2 = google_client.key("Task", f"test-entity-{uuid.uuid4()}")

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


def test_multi_client_filter_query(rust_client, google_client):
    """
    Tests whether a filter query returns the same result from both emulators.
    """
    test_id = f"test-{uuid.uuid4()}"
    keys_to_delete_rust = []
    keys_to_delete_google = []

    for client in [rust_client, google_client]:
        keys_to_delete = keys_to_delete_rust if client == rust_client else keys_to_delete_google
        for i in range(3):
            key = client.key("Task", f"task-{test_id}-{i}")
            entity = datastore.Entity(key=key)
            entity.update(
                {
                    "category": f"category-{test_id}",
                    "done": i % 2 == 0,  # Two done, one not done
                }
            )
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
