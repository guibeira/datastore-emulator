import os
import uuid
from time import sleep

import pytest
from google.cloud import datastore
from google.cloud.datastore.query import Or, PropertyFilter

use_real_db = "USE_REAL_DB" in os.environ


@pytest.fixture
def rust_client():
    """
    Fixture to create a Rust client connected to the emulator.
    """
    db_client = datastore.Client(project="test-project-2")
    db_client.base_url = "http://localhost:8042"
    yield db_client
    # Cleanup after test
    for kind in ["TaskTest", "Family", "User"]:
        query = db_client.query(kind=kind)
        keys_to_delete = [entity.key for entity in query.fetch()]
        if keys_to_delete:
            db_client.delete_multi(keys_to_delete)


@pytest.fixture
def google_client():
    """
    Fixture to create a Google client connected to the emulator.
    """
    if "USE_REAL_DB" in os.environ:
        database_name = os.environ["DATASTORE_DATABASE_NAME"]
        project_id = os.environ["DATASTORE_PROJECT_ID"]
        db_client = datastore.Client(project_id, database=database_name)
    else:
        db_client = datastore.Client(project="test-project-1")
        db_client.base_url = "http://localhost:8044"
    yield db_client
    # Cleanup after test
    for kind in ["TaskTest", "Family", "User"]:
        query = db_client.query(kind=kind)
        keys_to_delete = [entity.key for entity in query.fetch()]
        if keys_to_delete:
            db_client.delete_multi(keys_to_delete)


def test_multi_client_insert_isolation(google_client, rust_client):
    """
    Tests whether two clients connected to different emulator instances
    operate on isolated datasets, must return the same result.
    """
    # Create unique keys for each client
    key1 = rust_client.key("TaskTest", f"test-entity-{uuid.uuid4()}")
    key2 = google_client.key("TaskTest", f"test-entity-{uuid.uuid4()}")

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


def test_multi_client_filter_query(google_client, rust_client):
    """
    Tests whether a filter query returns the same result from both emulators.
    """
    test_id = f"test-{uuid.uuid4()}"

    for client in [google_client, rust_client]:
        for i in range(3):
            key = client.key("TaskTest", f"task-{test_id}-{i}")
            entity = datastore.Entity(key=key)
            entity.update(
                {
                    "category": f"category-{test_id}",
                    "done": i % 2 == 0,  # Two done, one not done
                }
            )
            client.put(entity)

    # --- Query with filter ---
    # Rust client
    rust_query = rust_client.query(kind="TaskTest")
    rust_query.add_filter("category", "=", f"category-{test_id}")
    rust_query.add_filter("done", "=", True)
    rust_results = list(rust_query.fetch())

    # Google client
    google_query = google_client.query(kind="TaskTest")
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


@pytest.mark.skipif(use_real_db is False, reason="Datastore emulator does not support aggregation queries")
def test_aggregation_count_query(google_client, rust_client):
    """
    Tests whether a COUNT aggregation query returns the same result from both emulators.
    Inspired by `count_query_property_filter` from client_test.py.
    """
    test_id = f"test-{uuid.uuid4()}"
    category = f"category-{test_id}"

    # --- Setup data ---
    for client in [google_client, rust_client]:
        entities = []
        for i in range(3):
            key = client.key("TaskTest", f"task-{test_id}-{i}")
            entity = datastore.Entity(key=key)
            entity.update(
                {
                    "category": category,
                    "done": i % 2 == 0,  # Two done (i=0, 2), one not done (i=1)
                }
            )
            entities.append(entity)
        client.put_multi(entities)

    # --- Run aggregation query on both clients ---
    results = {}
    for client_name, client in [("rust", rust_client), ("google", google_client)]:
        query = client.query(kind="TaskTest")
        query.add_filter("category", "=", category)
        query.add_filter("done", "=", True)

        count_query = client.aggregation_query(query).count(alias="total_done")

        # The result is an iterator
        count_result = list(count_query.fetch())
        # The structure is [[AggregationResult(value=2, alias='total_done')]]
        results[client_name] = count_result[0][0].value

    # --- Assertion ---
    assert results["rust"] == results["google"]
    assert results["rust"] == 2


@pytest.mark.skipif(use_real_db is False, reason="Datastore emulator does not support aggregation queries")
def test_aggregation_sum_avg_query(google_client, rust_client):
    """
    Tests whether SUM and AVG aggregation queries return the same result from both emulators.
    Inspired by `sum_query_on_kind` and `avg_query_on_kind` from client_test.py.
    """
    test_id = f"test-{uuid.uuid4()}"
    category = f"category-{test_id}"

    # --- Setup data ---
    for client in [google_client, rust_client]:
        entities = []
        for i, hours in enumerate([5, 3, 1]):
            key = client.key("TaskTest", f"task-{test_id}-{i}")
            entity = datastore.Entity(key=key)
            entity.update(
                {
                    "category": category,
                    "hours": hours,
                }
            )
            entities.append(entity)
        client.put_multi(entities)

    results = {}
    for client_name, client in [("rust", rust_client), ("google", google_client)]:
        query = client.query(kind="TaskTest")
        query.add_filter("category", "=", category)

        aggregation_query = client.aggregation_query(query)
        aggregation_query.sum("hours", alias="total_hours")
        aggregation_query.avg("hours", alias="avg_hours")

        agg_result_iterator = aggregation_query.fetch()
        agg_results = list(agg_result_iterator)[0]

        client_results = {}
        for agg in agg_results:
            client_results[agg.alias] = agg.value
        results[client_name] = client_results
    # --- Assertion ---
    assert results["rust"]["total_hours"] == results["google"]["total_hours"]
    assert results["rust"]["total_hours"] == 9

    assert results["rust"]["avg_hours"] == results["google"]["avg_hours"]
    assert results["rust"]["avg_hours"] == 3.0


def test_delete_multi(google_client, rust_client):
    """
    Tests whether `delete_multi` works consistently on both emulators.
    Inspired by `delete_multi_example` from client_test.py.
    """
    test_id = f"test-{uuid.uuid4()}"

    # --- Setup data and keys on both clients ---
    rust_keys = [rust_client.key("TaskTest", f"task-{test_id}-{i}") for i in range(3)]
    google_keys = [google_client.key("TaskTest", f"task-{test_id}-{i}") for i in range(3)]

    for i in range(3):
        rust_entity = datastore.Entity(key=rust_keys[i])
        rust_entity["desc"] = f"rust task {i}"
        rust_client.put(rust_entity)

        google_entity = datastore.Entity(key=google_keys[i])
        google_entity["desc"] = f"google task {i}"
        google_client.put(google_entity)

    # --- Delete multiple entities on both clients ---
    keys_to_delete_rust = [rust_keys[0], rust_keys[1]]
    keys_to_delete_google = [google_keys[0], google_keys[1]]

    rust_client.delete_multi(keys_to_delete_rust)
    google_client.delete_multi(keys_to_delete_google)

    # --- Verify deletion ---
    rust_retrieved = rust_client.get_multi(rust_keys)
    google_retrieved = google_client.get_multi(google_keys)

    # --- Assertions ---
    # Check that the number of retrieved entities is the same
    assert len(rust_retrieved) == len(google_retrieved)
    assert len(rust_retrieved) == 1  # Only one should be left

    # Check that the correct entity is left
    assert rust_retrieved[0].key.name == rust_keys[2].name
    assert google_retrieved[0].key.name == google_keys[2].name
    assert rust_retrieved[0]["desc"] == "rust task 2"
    assert google_retrieved[0]["desc"] == "google task 2"


@pytest.mark.skipif(
    use_real_db is False, reason="Datastore emulator returns: 'Only ancestor queries are allowed inside'"
)
def test_transaction_rollback(google_client, rust_client):
    """
    Tests that a transaction is correctly rolled back on failure.
    Inspired by `transaction_rollback_example` from client_test.py.
    """

    def _setup_and_run_transaction(client):
        # Initial data
        task1 = datastore.Entity(client.key("TaskTest", "budget_task1"))
        task1.update({"category": "expense", "amount": 400})
        task2 = datastore.Entity(client.key("TaskTest", "budget_task2"))
        task2.update({"category": "expense", "amount": 300})
        client.put_multi([task1, task2])

        # Run transaction that should fail and rollback
        try:
            with client.transaction() as transaction:
                expense_query = client.query(kind="TaskTest")
                expense_query.add_filter("category", "=", "expense")
                expenses = list(expense_query.fetch())
                current_total = sum(task["amount"] for task in expenses)

                # This new expense exceeds the "budget" of 1000
                new_expense = datastore.Entity(client.key("TaskTest", "budget_task3"))
                new_expense.update({"category": "expense", "amount": 500})

                if current_total + new_expense["amount"] > 1000:
                    # Put inside transaction before raising error
                    transaction.put(new_expense)
                    raise ValueError("Budget exceeded")
        except ValueError:
            # Expected failure
            pass

        # Return the final state of tasks
        return list(client.query(kind="TaskTest").fetch())

    # --- Run for both clients ---
    rust_final_tasks = _setup_and_run_transaction(rust_client)
    google_final_tasks = _setup_and_run_transaction(google_client)

    # --- Assertions ---
    # Both should have rolled back to 2 tasks
    assert len(rust_final_tasks) == 2
    assert len(google_final_tasks) == 2

    # Compare the final state
    rust_task_data = sorted([(t.key.name, t["amount"]) for t in rust_final_tasks])
    google_task_data = sorted([(t.key.name, t["amount"]) for t in google_final_tasks])

    # assert rust_task_data == google_task_data
    expected_data = sorted([("budget_task1", 400), ("budget_task2", 300)])
    assert rust_task_data == expected_data


@pytest.mark.skipif(use_real_db is False, reason="Datastore emulator does not support NOT_IN queries")
def test_not_equals_and_not_in_query(google_client, rust_client):
    """
    Tests '!=' and 'NOT_IN' queries work consistently on both emulators.
    Inspired by `not_equals_query` and `not_in_query` from client_test.py.
    """
    test_id = f"test-{uuid.uuid4()}"
    categories = ["work", "chores", "school", "personal"]

    # --- Setup data ---
    for client in [google_client, rust_client]:
        entities = []
        for i, category in enumerate(categories):
            key = client.key("TaskTest", f"task-{test_id}-{i}")
            entity = datastore.Entity(key=key)
            entity.update({"category": category, "id": i})
            entities.append(entity)
        client.put_multi(entities)

    # --- Run queries and store results ---
    results = {}
    for client_name, client in [("rust", rust_client), ("google", google_client)]:
        # Not equals query
        ne_query = client.query(kind="TaskTest")
        ne_query.add_filter("category", "!=", "work")
        ne_results = {e["category"] for e in ne_query.fetch()}

        # Not in query
        not_in_query = client.query(kind="TaskTest")
        not_in_query.add_filter("category", "NOT_IN", ["work", "chores"])
        not_in_results = {e["category"] for e in not_in_query.fetch()}

        results[client_name] = {
            "not_equals": ne_results,
            "not_in": not_in_results,
        }

    # --- Assertions ---
    # Not equals
    assert results["rust"]["not_equals"] == results["google"]["not_equals"]
    assert results["rust"]["not_equals"] == {"chores", "school", "personal"}

    # Not in
    assert results["rust"]["not_in"] == results["google"]["not_in"]
    assert results["rust"]["not_in"] == {"school", "personal"}


@pytest.mark.skipif(use_real_db is False, reason="Datastore emulator does not support IN queries")
def test_in_query(google_client, rust_client):
    """
    Tests 'IN' queries work consistently on both emulators.
    Inspired by `in_query` from client_test.py.
    """
    test_id = f"test-{uuid.uuid4()}"
    priorities = [1, 2, 3, 4, 4, 5]

    # --- Setup data ---
    for client in [google_client, rust_client]:
        entities = []
        for i, priority in enumerate(priorities):
            key = client.key("TaskTest", f"task-{test_id}-{i}")
            entity = datastore.Entity(key=key)
            entity.update({"priority": priority})
            entities.append(entity)
        client.put_multi(entities)

    # --- Run IN query ---
    results = {}
    for client_name, client in [("rust", rust_client), ("google", google_client)]:
        query = client.query(kind="TaskTest")
        query.add_filter("priority", "IN", [4, 5])
        # Use a set of priorities for comparison, as order is not guaranteed
        results[client_name] = sorted([e["priority"] for e in query.fetch()])

    # --- Assertions ---
    assert len(results["rust"]) == 3
    assert results["rust"] == results["google"]
    assert results["rust"] == [4, 4, 5]


def test_allocate_ids(google_client, rust_client):
    """
    Tests that `allocate_ids` works consistently.
    Inspired by `allocate_ids_example` from client_test.py.
    """

    def _run_allocate_ids_flow(client):
        # 1. Allocate IDs for a new kind
        incomplete_key = client.key("Family")
        allocated_keys = client.allocate_ids(incomplete_key, 2)

        assert len(allocated_keys) == 2
        assert all(key.is_partial is False for key in allocated_keys)

        # 2. Create and save entities with these keys
        entities = []
        for i, key in enumerate(allocated_keys):
            entity = datastore.Entity(key=key)
            entity["name"] = f"member_{i}"
            entities.append(entity)
        client.put_multi(entities)

        # 3. Retrieve and verify
        retrieved_entities = client.get_multi(allocated_keys)
        assert len(retrieved_entities) == 2
        retrieved_names = sorted([e["name"] for e in retrieved_entities])
        assert retrieved_names == ["member_0", "member_1"]
        return len(retrieved_entities)

    # --- Run for both clients ---
    rust_count = _run_allocate_ids_flow(rust_client)
    google_count = _run_allocate_ids_flow(google_client)

    # --- Assertions ---
    # The main assertion is that both flows complete successfully and return the same count.
    assert rust_count == google_count
    assert rust_count == 2


@pytest.mark.skipif(use_real_db is False, reason="Datastore emulator does not support pagination")
def test_pagination(google_client, rust_client):
    """
    Tests that query pagination works consistently.
    Inspired by `fetch_paginated_entities` from client_test.py.
    """
    test_id = f"test-{uuid.uuid4()}"
    num_entities = 25
    page_size = 10

    # --- Setup data ---
    for client in [google_client, rust_client]:
        entities = []
        for i in range(num_entities):
            key = client.key("TaskTest", f"task-{test_id}-{i}")
            entity = datastore.Entity(key=key)
            entity["test_id"] = test_id
            entity["order"] = i
            entities.append(entity)
        client.put_multi(entities)
    # --- Paginate through results for both clients ---
    results = {}
    for client_name, client in [("rust", rust_client), ("google", google_client)]:
        all_entities = []
        cursor = None
        while True:
            query = client.query(kind="TaskTest")
            query.add_filter("test_id", "=", test_id)
            query.order = ["order"]

            query_iter = query.fetch(start_cursor=cursor, limit=page_size)
            page_entities = list(query_iter)

            all_entities.extend(page_entities)
            cursor = query_iter.next_page_token

            if not cursor:
                break

        results[client_name] = {e.key.name for e in all_entities}

    # --- Assertions ---
    assert len(results["rust"]) == num_entities
    assert len(results["google"]) == num_entities
    assert results["rust"] == results["google"]


def test_comparison_operators_query(google_client, rust_client):
    """
    Tests comparison operators (<, <=, >, >=) work consistently.
    """
    test_id = f"test-{uuid.uuid4()}"
    scores = [10, 20, 30, 40]

    # --- Setup data ---
    for client in [google_client, rust_client]:
        entities = []
        for i, score in enumerate(scores):
            key = client.key("TaskTest", f"task-{test_id}-{i}")
            entity = datastore.Entity(key=key)
            entity.update({"test_id": test_id, "score": score})
            entities.append(entity)
        client.put_multi(entities)

    # --- Define queries and expected results ---
    queries_to_test = {
        "less_than": {"operator": "<", "value": 30, "expected_scores": {10, 20}},
        "less_than_or_equal": {"operator": "<=", "value": 30, "expected_scores": {10, 20, 30}},
        "greater_than": {"operator": ">", "value": 20, "expected_scores": {30, 40}},
        "greater_than_or_equal": {"operator": ">=", "value": 20, "expected_scores": {20, 30, 40}},
    }

    # --- Run queries and assert ---
    for test_name, params in queries_to_test.items():
        rust_results = set()
        google_results = set()

        for client_name, client, resultSet in [
            ("rust", rust_client, rust_results),
            ("google", google_client, google_results),
        ]:
            query = client.query(kind="TaskTest")
            query.add_filter("test_id", "=", test_id)
            query.add_filter("score", params["operator"], params["value"])
            for entity in query.fetch():
                resultSet.add(entity["score"])

        assert rust_results == params["expected_scores"], f"Rust client failed on '{test_name}'"
        assert google_results == params["expected_scores"], f"Google client failed on '{test_name}'"
        assert rust_results == google_results, f"Mismatch between clients on '{test_name}'"


def test_has_ancestor_query(google_client, rust_client):
    """
    Tests that HAS_ANCESTOR queries work consistently on both emulators.
    """
    test_id = f"test-{uuid.uuid4()}"

    # --- Setup data ---
    for client in [google_client, rust_client]:
        # Create a parent entity
        parent_key = client.key("User", f"user-{test_id}")
        parent_entity = datastore.Entity(key=parent_key)
        parent_entity["name"] = "Test User"
        client.put(parent_entity)

        # Create child entities
        child_key1 = client.key("TaskTest", "child1", parent=parent_key)
        child1 = datastore.Entity(key=child_key1)
        child1["description"] = "Child task 1"

        child_key2 = client.key("TaskTest", "child2", parent=parent_key)
        child2 = datastore.Entity(key=child_key2)
        child2["description"] = "Child task 2"

        # Create an unrelated entity
        unrelated_key = client.key("TaskTest", f"unrelated-{test_id}")
        unrelated_entity = datastore.Entity(key=unrelated_key)
        unrelated_entity["description"] = "Unrelated task"

        client.put_multi([child1, child2, unrelated_entity])

    # --- Run HAS_ANCESTOR query ---
    results = {}
    for client_name, client in [("rust", rust_client), ("google", google_client)]:
        parent_key = client.key("User", f"user-{test_id}")
        query = client.query(kind="TaskTest", ancestor=parent_key)

        # Store the key names for comparison
        results[client_name] = {e.key.name for e in query.fetch()}

    # --- Assertions ---
    expected_keys = {"child1", "child2"}
    assert len(results["rust"]) == 2
    assert results["rust"] == expected_keys
    assert results["google"] == expected_keys
    assert results["rust"] == results["google"]


@pytest.mark.skipif(use_real_db is False, reason="Datastore emulator does not support composite OR queries")
def test_composite_or_filter_query(google_client, rust_client):
    """
    Tests that a composite OR filter query works consistently.
    """
    test_id = f"test-{uuid.uuid4()}"

    # --- Setup data ---
    for client in [google_client, rust_client]:
        entities = [
            datastore.Entity(key=client.key("TaskTest", f"task-{test_id}-1")),
            datastore.Entity(key=client.key("TaskTest", f"task-{test_id}-2")),
            datastore.Entity(key=client.key("TaskTest", f"task-{test_id}-3")),
            datastore.Entity(key=client.key("TaskTest", f"task-{test_id}-4")),
        ]
        entities[0].update({"test_id": test_id, "priority": 5, "done": False})  # Matches priority
        entities[1].update({"test_id": test_id, "priority": 1, "done": True})  # Matches done
        entities[2].update({"test_id": test_id, "priority": 5, "done": True})  # Matches both
        entities[3].update({"test_id": test_id, "priority": 1, "done": False})  # Matches neither
        client.put_multi(entities)

    # --- Run OR query ---
    results = {}
    for client_name, client in [("rust", rust_client), ("google", google_client)]:
        query = client.query(kind="TaskTest")
        # Note: The client library combines filters with AND by default.
        # Here we filter by test_id AND (priority > 4 OR done = True)
        query.add_filter("test_id", "=", test_id)
        query.add_filter(
            filter=Or(
                [
                    PropertyFilter("priority", ">", 4),
                    PropertyFilter("done", "=", True),
                ]
            )
        )
        # Store key names for comparison
        results[client_name] = {e.key.name for e in query.fetch()}

    # --- Assertions ---
    expected_keys = {f"task-{test_id}-1", f"task-{test_id}-2", f"task-{test_id}-3"}
    assert len(results["rust"]) == 3
    assert results["rust"] == expected_keys
    assert results["google"] == expected_keys
    assert results["rust"] == results["google"]


def test_keys_only_query(google_client, rust_client):
    """
    Tests that keys-only queries work consistently.
    """
    test_id = f"test-{uuid.uuid4()}"

    # --- Setup data ---
    for client in [google_client, rust_client]:
        key = client.key("TaskTest", f"task-{test_id}-1")
        entity = datastore.Entity(key=key)
        entity["description"] = "This should not be returned"
        entity["test_id"] = test_id
        client.put(entity)

    # --- Run keys-only query ---
    results = {}
    for client_name, client in [("rust", rust_client), ("google", google_client)]:
        query = client.query(kind="TaskTest")
        query.keys_only()
        query.add_filter("test_id", "=", test_id)

        fetched_entities = list(query.fetch())
        assert len(fetched_entities) > 0, f"{client_name} returned no entities"

        # The client library returns an entity with only the key populated.
        entity = fetched_entities[0]
        results[client_name] = {
            "key": entity.key.name,
            "properties": dict(entity),
        }

    # --- Assertions ---
    expected_key_name = f"task-{test_id}-1"

    # Check Rust client
    assert results["rust"]["key"] == expected_key_name
    assert results["rust"]["properties"] == {}

    # Check Google client
    assert results["google"]["key"] == expected_key_name
    assert results["google"]["properties"] == {}

    # Compare both
    assert results["rust"] == results["google"]


def test_projection_query(google_client, rust_client):
    """
    Tests that projection queries work consistently.
    """
    test_id = f"test-{uuid.uuid4()}"

    # --- Setup data ---
    for client in [google_client, rust_client]:
        key = client.key("TaskTest", f"task-{test_id}-1")
        entity = datastore.Entity(key=key)
        entity.update(
            {
                "test_id": test_id,
                "description": "This should not be projected",
                "priority": 5,
                "done": True,
            }
        )
        client.put(entity)

    # --- Run projection query ---
    results = {}
    for client_name, client in [("rust", rust_client), ("google", google_client)]:
        query = client.query(kind="TaskTest")
        query.add_filter("test_id", "=", test_id)
        query.projection = ["priority", "done"]

        fetched_entities = list(query.fetch())
        assert len(fetched_entities) == 1, f"{client_name} returned wrong number of entities"

        entity = fetched_entities[0]
        results[client_name] = dict(entity)

    # --- Assertions ---
    expected_properties = {"priority": 5, "done": True}

    assert results["rust"] == expected_properties
    assert results["google"] == expected_properties
    assert results["rust"] == results["google"]


def test_ordering_query(google_client, rust_client):
    """
    Tests that queries with ordering work consistently.
    """
    test_id = f"test-{uuid.uuid4()}"
    tasks = [
        {"name": "task-a", "priority": 2},
        {"name": "task-b", "priority": 3},
        {"name": "task-c", "priority": 1},
    ]

    # --- Setup data ---
    for client in [google_client, rust_client]:
        entities = []
        for task in tasks:
            key = client.key("TaskTest", f"{task['name']}-{test_id}")
            entity = datastore.Entity(key=key)
            entity.update({"test_id": test_id, "priority": task["priority"]})
            entities.append(entity)
        client.put_multi(entities)

    # --- Run queries with ordering ---
    results = {}
    for client_name, client in [("rust", rust_client), ("google", google_client)]:
        # Ascending order
        query_asc = client.query(kind="TaskTest")
        query_asc.add_filter("test_id", "=", test_id)
        query_asc.order = "priority"
        asc_priorities = [e["priority"] for e in query_asc.fetch()]

        # Descending order
        query_desc = client.query(kind="TaskTest")
        query_desc.add_filter("test_id", "=", test_id)
        query_desc.order = "-priority"
        desc_priorities = [e["priority"] for e in query_desc.fetch()]

        results[client_name] = {
            "asc": asc_priorities,
            "desc": desc_priorities,
        }

    # --- Assertions ---
    expected_asc = [1, 2, 3]
    expected_desc = [3, 2, 1]

    # Check ascending
    assert results["rust"]["asc"] == expected_asc
    assert results["google"]["asc"] == expected_asc
    assert results["rust"]["asc"] == results["google"]["asc"]

    # Check descending
    assert results["rust"]["desc"] == expected_desc
    assert results["google"]["desc"] == expected_desc
    assert results["rust"]["desc"] == results["google"]["desc"]
