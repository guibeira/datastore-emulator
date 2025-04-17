# Copyright 2022 Google, Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import os
import time
from datetime import datetime, timedelta, timezone
from pprint import pprint

# os.environ["GRPC_VERBOSITY"] = "DEBUG"
# os.environ["GRPC_TRACE"] = "all"
os.environ["DATASTORE_EMULATOR_HOST"] = "localhost:8080"
from google.cloud import datastore  # noqa: I100
from google.cloud.datastore.key import Key
from google.cloud.datastore.query import PropertyFilter


def _preamble():
    # [START datastore_size_coloration_query]
    from google.cloud import datastore

    # For help authenticating your client, visit
    # https://cloud.google.com/docs/authentication/getting-started
    client = datastore.Client()

    # [END datastore_size_coloration_query]
    assert client is not None


def transaction_rollback_example(client):
    # [START datastore_transaction_rollback]
    # Create initial budget tasks
    task1 = datastore.Entity(client.key("Task", "budget_task1"))
    task2 = datastore.Entity(client.key("Task", "budget_task2"))

    task1["category"] = "expense"
    task1["amount"] = 400
    task2["category"] = "expense"
    task2["amount"] = 300

    tasks = [task1, task2]
    client.put_multi(tasks)

    try:
        with client.transaction() as transaction:
            # Query for current expenses
            expense_query = client.query(kind="Task")
            expense_query.add_filter("category", "=", "expense")

            # Calculate total current expenses
            expenses = list(expense_query.fetch())
            current_total = sum(task["amount"] for task in expenses if "amount" in task)

            # Create a new expense that would exceed our budget limit
            new_expense = datastore.Entity(client.key("Task", "budget_task3"))
            new_expense["category"] = "expense"
            new_expense["amount"] = 500
            transaction.put(new_expense)
            tasks.append(new_expense)

            # Check against budget limit
            raise ValueError("Budget exceeded")
    except ValueError as e:
        print(f"Error: {e}")

    # Verify final state after transaction
    resulting_tasks = list(client.query(kind="Task").fetch())
    return resulting_tasks


def not_equals_query(client):
    # [START datastore_not_equals_query]
    query = client.query(kind="Task")
    query.add_filter("category", "!=", "work")
    # [END datastore_not_equals_query]

    return list(query.fetch())


def not_in_query(client):
    # [START datastore_not_in_query]
    query = client.query(kind="Task")
    query.add_filter("category", "NOT_IN", ["work", "chores", "school"])
    # [END datastore_not_in_query]

    return list(query.fetch())


def query_with_readtime(client):
    # [START datastore_stale_read]
    # Create a read time of 15 seconds in the past
    read_time = datetime.now(timezone.utc) - timedelta(seconds=15)

    # Fetch an entity with read_time
    task_key = client.key("Task", "sampletask")

    entity = client.get(task_key, read_time=read_time)

    # Query Task entities with read_time
    query = client.query(kind="Task")
    tasks = query.fetch(read_time=read_time, limit=10)
    # [END datastore_stale_read]

    results = list(tasks)
    results.append(entity)

    return results


def count_query_in_transaction(client):
    # [START datastore_count_in_transaction]
    task1 = datastore.Entity(client.key("Task", "task1"))
    task2 = datastore.Entity(client.key("Task", "task2"))

    task1["owner"] = "john"
    task2["owner"] = "john"

    tasks = [task1, task2]
    client.put_multi(tasks)

    with client.transaction() as transaction:

        tasks_of_john = client.query(kind="Task")
        tasks_of_john.add_filter("owner", "=", "john")
        total_tasks_query = client.aggregation_query(tasks_of_john)

        query_result = total_tasks_query.count(alias="tasks_count").fetch()
        for task_result in query_result:
            tasks_count = task_result[0]
            if tasks_count.value < 2:
                task3 = datastore.Entity(client.key("Task", "task3"))
                task3["owner"] = "john"
                transaction.put(task3)
                tasks.append(task3)
            else:
                print(f"Found existing {tasks_count.value} tasks, rolling back")
                # client.entities_to_delete.extend(tasks)
                raise ValueError("User 'John' cannot have more than 2 tasks")
    # [END datastore_count_in_transaction]


def reserve_ids(client):
    parent_key = client.key("Parent", 1111)
    client.reserve_ids(parent_key, 2)


def allocate_ids_example(client):
    # [START datastore_allocate_ids_example]
    # Create incomplete keys (without IDs) for which we want IDs allocated
    incomplete_key1 = client.key("Task")
    incomplete_key2 = client.key("Task")
    incomplete_key3 = client.key("Task")

    # Request the allocation of IDs for these incomplete keys
    imcomplete_key = Key("Parent", "foo", "Child", project=client.project)
    keys = client.allocate_ids(imcomplete_key, 2)
    print(f"Allocated {len(keys)} keys")

    # Create entities using the allocated keys
    entities = []
    for i, key in enumerate(keys):
        entity = datastore.Entity(key)
        entity.update(
            {
                "description": f"Task with allocated ID {key.id}",
                "created": datetime.now(timezone.utc),
                "priority": i + 1,
                "done": False,
            }
        )
        entities.append(entity)

    # Save the entities to Datastore
    client.put_multi(entities)
    breakpoint()

    # Now we can retrieve one of the entities to verify it was saved
    first_key = keys[0]
    second_key = keys[1]
    first_retrieved_entity = client.get(first_key)
    print(f"Retrieved entity with key {first_key}")
    print(f"Description: {first_retrieved_entity['description']}")

    second_retrieved_entity = client.get(second_key)
    print(f"Retrieved entity with key {second_key}")
    print(f"Description: {second_retrieved_entity['description']}")

    return entities


def count_query_on_kind(client):
    # [START datastore_count_on_kind]
    task1 = datastore.Entity(client.key("Task", "task1"))
    task2 = datastore.Entity(client.key("Task", "task2"))

    tasks = [task1, task2]
    client.put_multi(tasks)
    all_tasks_query = client.query(kind="Task")
    all_tasks_count_query = client.aggregation_query(all_tasks_query).count()
    query_result = all_tasks_count_query.fetch()
    for aggregation_results in query_result:
        for aggregation in aggregation_results:
            print(f"Total tasks (accessible from default alias) is {aggregation.value}")
    # [END datastore_count_on_kind]
    return tasks


def count_query_with_limit(client):
    # [START datastore_count_with_limit]
    task1 = datastore.Entity(client.key("Task", "task1"))
    task2 = datastore.Entity(client.key("Task", "task2"))
    task3 = datastore.Entity(client.key("Task", "task3"))

    tasks = [task1, task2, task3]
    client.put_multi(tasks)
    all_tasks_query = client.query(kind="Task")
    all_tasks_count_query = client.aggregation_query(all_tasks_query).count()
    query_result = all_tasks_count_query.fetch(limit=2)
    for aggregation_results in query_result:
        for aggregation in aggregation_results:
            print(f"We have at least {aggregation.value} tasks")
    # [END datastore_count_with_limit]
    return tasks


def count_query_property_filter(client):
    # [START datastore_count_with_property_filter]
    task1 = datastore.Entity(client.key("Task", "task1"))
    task2 = datastore.Entity(client.key("Task", "task2"))
    task3 = datastore.Entity(client.key("Task", "task3"))

    task1["done"] = True
    task2["done"] = False
    task3["done"] = True

    tasks = [task1, task2, task3]
    client.put_multi(tasks)
    completed_tasks = client.query(kind="Task").add_filter("done", "=", True)
    remaining_tasks = client.query(kind="Task").add_filter("done", "=", False)

    completed_tasks_query = client.aggregation_query(query=completed_tasks).count(alias="total_completed_count")
    remaining_tasks_query = client.aggregation_query(query=remaining_tasks).count(alias="total_remaining_count")

    completed_query_result = completed_tasks_query.fetch()
    for aggregation_results in completed_query_result:
        for aggregation_result in aggregation_results:
            if aggregation_result.alias == "total_completed_count":
                print(f"Total completed tasks count is {aggregation_result.value}")

    remaining_query_result = remaining_tasks_query.fetch()
    for aggregation_results in remaining_query_result:
        for aggregation_result in aggregation_results:
            if aggregation_result.alias == "total_remaining_count":
                print(f"Total remaining tasks count is {aggregation_result.value}")
    # [END datastore_count_with_property_filter]
    return tasks


def count_query_with_stale_read(client):

    tasks = [task for task in client.query(kind="Task").fetch()]
    client.delete_multi(tasks)  # ensure the database is empty before starting
    # [START datastore_count_query_with_stale_read]
    task1 = datastore.Entity(client.key("Task", "task1"))
    task2 = datastore.Entity(client.key("Task", "task2"))

    # Saving two tasks
    task1["done"] = True
    task2["done"] = False
    client.put_multi([task1, task2])
    time.sleep(10)

    past_timestamp = datetime.now(timezone.utc)  # we have two tasks in database at this time.
    time.sleep(10)

    # Saving third task
    task3 = datastore.Entity(client.key("Task", "task3"))
    task3["done"] = False
    client.put(task3)

    all_tasks = client.query(kind="Task")
    all_tasks_count = client.aggregation_query(
        query=all_tasks,
    ).count(alias="all_tasks_count")

    # Executing aggregation query
    query_result = all_tasks_count.fetch()
    for aggregation_results in query_result:
        for aggregation_result in aggregation_results:
            print(f"Latest tasks count is {aggregation_result.value}")

    # Executing aggregation query with past timestamp
    tasks_in_past = client.aggregation_query(query=all_tasks).count(alias="tasks_in_past")
    tasks_in_the_past_query_result = tasks_in_past.fetch(read_time=past_timestamp)
    for aggregation_results in tasks_in_the_past_query_result:
        for aggregation_result in aggregation_results:
            print(f"Stale tasks count is {aggregation_result.value}")
    # [END datastore_count_query_with_stale_read]
    return [task1, task2, task3]


def sum_query_on_kind(client):
    # [START datastore_sum_aggregation_query_on_kind]
    # Set up sample entities
    # Use incomplete key to auto-generate ID
    task1 = datastore.Entity(client.key("Task"))
    task2 = datastore.Entity(client.key("Task"))
    task3 = datastore.Entity(client.key("Task"))

    task1["hours"] = 5
    task2["hours"] = 3
    task3["hours"] = 1

    tasks = [task1, task2, task3]
    client.put_multi(tasks)
    breakpoint()
    # Execute sum aggregation query
    all_tasks_query = client.query(kind="Task")
    all_tasks_sum_query = client.aggregation_query(all_tasks_query).sum("hours")
    query_result = all_tasks_sum_query.fetch()
    for aggregation_results in query_result:
        for aggregation in aggregation_results:
            print(f"Total sum of hours in tasks is {aggregation.value}")
    # [END datastore_sum_aggregation_query_on_kind]
    return tasks


def sum_query_property_filter(client):
    # [START datastore_sum_aggregation_query_with_filters]
    # Set up sample entities
    # Use incomplete key to auto-generate ID
    task1 = datastore.Entity(client.key("Task"))
    task2 = datastore.Entity(client.key("Task"))
    task3 = datastore.Entity(client.key("Task"))

    task1["hours"] = 5
    task2["hours"] = 3
    task3["hours"] = 1

    task1["done"] = True
    task2["done"] = True
    task3["done"] = False

    tasks = [task1, task2, task3]
    client.put_multi(tasks)

    # Execute sum aggregation query with filters
    completed_tasks = client.query(kind="Task").add_filter("done", "=", True)
    completed_tasks_query = client.aggregation_query(query=completed_tasks).sum(
        property_ref="hours", alias="total_completed_sum_hours"
    )

    completed_query_result = completed_tasks_query.fetch()
    for aggregation_results in completed_query_result:
        for aggregation_result in aggregation_results:
            if aggregation_result.alias == "total_completed_sum_hours":
                print(f"Total sum of hours in completed tasks is {aggregation_result.value}")
    # [END datastore_sum_aggregation_query_with_filters]
    return tasks


def avg_query_on_kind(client):
    # [START datastore_avg_aggregation_query_on_kind]
    # Set up sample entities
    # Use incomplete key to auto-generate ID
    task1 = datastore.Entity(client.key("Task"))
    task2 = datastore.Entity(client.key("Task"))
    task3 = datastore.Entity(client.key("Task"))

    task1["hours"] = 5
    task2["hours"] = 3
    task3["hours"] = 1

    tasks = [task1, task2, task3]
    client.put_multi(tasks)

    # Execute average aggregation query
    all_tasks_query = client.query(kind="Task")
    all_tasks_avg_query = client.aggregation_query(all_tasks_query).avg("hours")
    query_result = all_tasks_avg_query.fetch()
    for aggregation_results in query_result:
        for aggregation in aggregation_results:
            print(f"Total average of hours in tasks is {aggregation.value}")
    # [END datastore_avg_aggregation_query_on_kind]
    return tasks


def avg_query_property_filter(client):
    # [START datastore_avg_aggregation_query_with_filters]
    # Set up sample entities
    # Use incomplete key to auto-generate ID
    task1 = datastore.Entity(client.key("Task"))
    task2 = datastore.Entity(client.key("Task"))
    task3 = datastore.Entity(client.key("Task"))

    task1["hours"] = 5
    task2["hours"] = 3
    task3["hours"] = 1

    task1["done"] = True
    task2["done"] = True
    task3["done"] = False

    tasks = [task1, task2, task3]
    client.put_multi(tasks)

    # Execute average aggregation query with filters
    completed_tasks = client.query(kind="Task").add_filter("done", "=", True)
    completed_tasks_query = client.aggregation_query(query=completed_tasks).avg(
        property_ref="hours", alias="total_completed_avg_hours"
    )

    completed_query_result = completed_tasks_query.fetch()
    for aggregation_results in completed_query_result:
        for aggregation_result in aggregation_results:
            if aggregation_result.alias == "total_completed_avg_hours":
                print(f"Total average of hours in completed tasks is {aggregation_result.value}")
    # [END datastore_avg_aggregation_query_with_filters]
    return tasks


def multiple_aggregations_query(client):
    # [START datastore_multiple_aggregation_in_structured_query]
    # Set up sample entities
    # Use incomplete key to auto-generate ID
    task1 = datastore.Entity(client.key("Task"))
    task2 = datastore.Entity(client.key("Task"))
    task3 = datastore.Entity(client.key("Task"))

    task1["hours"] = 5
    task2["hours"] = 3
    task3["hours"] = 1

    tasks = [task1, task2, task3]
    client.put_multi(tasks)

    # Execute query with multiple aggregations
    all_tasks_query = client.query(kind="Task")
    aggregation_query = client.aggregation_query(all_tasks_query)
    # Add aggregations
    aggregation_query.add_aggregations(
        [
            datastore.aggregation.CountAggregation(alias="count_aggregation"),
            datastore.aggregation.SumAggregation(property_ref="hours", alias="sum_aggregation"),
            datastore.aggregation.AvgAggregation(property_ref="hours", alias="avg_aggregation"),
        ]
    )

    query_result = aggregation_query.fetch()
    for aggregation_results in query_result:
        for aggregation in aggregation_results:
            print(f"{aggregation.alias} value is {aggregation.value}")
    # [END datastore_multiple_aggregation_in_structured_query]
    return tasks


def explain_analyze_entity(client):
    # [START datastore_query_explain_analyze_entity]
    # Build the query with explain_options
    # analzye = true to get back the query stats, plan info, and query results
    query = client.query(kind="Task", explain_options=datastore.ExplainOptions(analyze=True))

    # initiate the query
    iterator = query.fetch()

    # explain_metrics is only available after query is completed
    for task_result in iterator:
        print(task_result)

    # get the plan summary
    plan_summary = iterator.explain_metrics.plan_summary
    print(f"Indexes used: {plan_summary.indexes_used}")

    # get the execution stats
    execution_stats = iterator.explain_metrics.execution_stats
    print(f"Results returned: {execution_stats.results_returned}")
    print(f"Execution duration: {execution_stats.execution_duration}")
    print(f"Read operations: {execution_stats.read_operations}")
    print(f"Debug stats: {execution_stats.debug_stats}")
    # [END datastore_query_explain_analyze_entity]


def explain_entity(client):
    # [START datastore_query_explain_entity]
    # Build the query with explain_options
    # by default (analyze = false), only plan_summary property is available
    query = client.query(kind="Task", explain_options=datastore.ExplainOptions())

    # initiate the query
    iterator = query.fetch()

    # get the plan summary
    plan_summary = iterator.explain_metrics.plan_summary
    print(f"Indexes used: {plan_summary.indexes_used}")
    # [END datastore_query_explain_entity]


def explain_analyze_aggregation(client):
    # [START datastore_query_explain_analyze_aggregation]
    # Build the aggregation query with explain_options
    # analzye = true to get back the query stats, plan info, and query results
    all_tasks_query = client.query(kind="Task")
    count_query = client.aggregation_query(
        all_tasks_query, explain_options=datastore.ExplainOptions(analyze=True)
    ).count()

    # initiate the query
    iterator = count_query.fetch()

    # explain_metrics is only available after query is completed
    for task_result in iterator:
        print(task_result)

    # get the plan summary
    plan_summary = iterator.explain_metrics.plan_summary
    print(f"Indexes used: {plan_summary.indexes_used}")

    # get the execution stats
    execution_stats = iterator.explain_metrics.execution_stats
    print(f"Results returned: {execution_stats.results_returned}")
    print(f"Execution duration: {execution_stats.execution_duration}")
    print(f"Read operations: {execution_stats.read_operations}")
    print(f"Debug stats: {execution_stats.debug_stats}")
    # [END datastore_query_explain_analyze_aggregation]


def in_query(client):
    # [START datastore_in_query]

    task1 = datastore.Entity(client.key("Task", "sample_task_1"))
    task1.update(
        {
            "description": "Item with priority 4",
            "created": datetime.now(timezone.utc),
            "done": False,
            "priority": 4,
            "tags": ["shopping", "errands"],
        }
    )
    client.put(task1)
    task2 = datastore.Entity(client.key("Task", "sample_task_1"))
    task2.update(
        {
            "description": "Item with priority 5",
            "created": datetime.now(timezone.utc),
            "done": False,
            "priority": 5,
            "tags": ["shopping", "errands"],
        }
    )
    client.put(task2)

    query = client.query(kind="Task")
    query.add_filter("priority", "=", 4)
    # query.add_filter(filter=PropertyFilter("confirmed", "=", True))
    # query.add_filter(property_name="tag", operator="IN", value=["learn", "study"])
    # [END datastore_in_query]
    results = [i for i in query.fetch()]
    return results


def insert_examples(client):
    # [START datastore_insert_examples]
    # Example 1: Insert an entity with a specified key
    task1 = datastore.Entity(client.key("Task", "sample_task_1"))
    task1.update(
        {
            "description": "Buy groceries",
            "created": datetime.now(timezone.utc),
            "done": False,
            "priority": 4,
            "tags": ["shopping", "errands"],
        }
    )
    client.put(task1)

    # # Example 2: Insert an entity with auto-generated ID
    # task2 = datastore.Entity(client.key("Task"))  # Incomplete key - ID will be auto-assigned
    # task2.update(
    #     {
    #         "description": "Finish project",
    #         "created": datetime.now(timezone.utc),
    #         "done": False,
    #         "priority": 5,
    #         "tags": ["work", "urgent"],
    #     }
    # )
    # client.put(task2)
    # print(f"Auto-assigned ID: {task2.key.id}")

    # # Example 3: Batch insert multiple entities
    # task3 = datastore.Entity(client.key("Task", "sample_task_3"))
    # task3["description"] = "Schedule meeting"
    # task3["priority"] = 3

    # task4 = datastore.Entity(client.key("Task", "sample_task_4"))
    # task4["description"] = "Call plumber"
    # task4["priority"] = 2

    # batch = [task3, task4]
    # client.put_multi(batch)

    # # Example 4: Insert entity with nested data
    # task5 = datastore.Entity(client.key("Task", "sample_task_5"))
    # task5.update(
    #     {
    #         "description": "Plan vacation",
    #         "details": {"location": "Hawaii", "duration_days": 7, "budget": 3000},
    #         "participants": ["Alice", "Bob"],
    #         "confirmed": True,
    #     }
    # )
    # client.put(task5)
    # [END datastore_insert_examples]
    # return [task1, task2, task3, task4, task5]


def explain_aggregation(client):
    # [START datastore_query_explain_aggregation]
    # Build the aggregation query with explain_options
    # by default (analyze = false), only plan_summary property is available
    all_tasks_query = client.query(kind="Task")
    count_query = client.aggregation_query(all_tasks_query, explain_options=datastore.ExplainOptions()).count()

    # initiate the query
    iterator = count_query.fetch()

    # get the plan summary
    plan_summary = iterator.explain_metrics.plan_summary
    print(f"Indexes used: {plan_summary.indexes_used}")
    # [END datastore_query_explain_aggregation]


def delete_example(client):
    # [START datastore_delete_example]
    # Create sample entities
    task1 = datastore.Entity(client.key("Task", "task_to_delete"))
    task2 = datastore.Entity(client.key("Task", "task_to_keep"))

    task1.update({"description": "This task will be deleted"})
    task2.update({"description": "This task will be kept"})

    # Insert entities into Datastore
    client.put_multi([task1, task2])

    # Delete the first task
    client.delete(task1.key)

    # Verify deletion
    deleted_task = client.get(task1.key)
    kept_task = client.get(task2.key)

    if deleted_task is None:
        print("Task successfully deleted.")
    else:
        print("Task deletion failed.")

    if kept_task is not None:
        print("Task to keep is still present.")
    else:
        print("Task to keep is missing.")
    # [END datastore_delete_example]
    return [task1, task2]


def delete_multi_example(client):
    # [START datastore_delete_multi_example]
    # Create sample entities
    task1 = datastore.Entity(client.key("Task", "task_to_delete_1"))
    task2 = datastore.Entity(client.key("Task", "task_to_delete_2"))
    task3 = datastore.Entity(client.key("Task", "task_to_keep"))

    task1.update({"description": "This task will be deleted"})
    task2.update({"description": "This task will also be deleted"})
    task3.update({"description": "This task will be kept"})

    # Insert entities into Datastore
    client.put_multi([task1, task2, task3])

    # Delete multiple tasks
    client.delete_multi([task1.key, task2.key])

    client.delete_multi([task1.key, task2.key])
    # Verify deletion
    deleted_task1 = client.get(task1.key)
    deleted_task2 = client.get(task2.key)
    kept_task = client.get(task3.key)

    if deleted_task1 is None and deleted_task2 is None:
        print("Tasks successfully deleted.")
    else:
        print("Task deletion failed.")

    if kept_task is not None:
        print("Task to keep is still present.")
    else:
        print("Task to keep is missing.")
    # [END datastore_delete_multi_example]
    return [task1, task2, task3]


def main(project_id):
    client = datastore.Client(project_id)

    functions_to_call = [
        # delete_example,
        # delete_multi_example,
        # insert_examples,
        in_query,
        # not_equals_query,
        # not_in_query,
        # query_with_readtime,
        # count_query_in_transaction,
        # count_query_on_kind,
        # count_query_with_limit,
        # count_query_property_filter,
        # count_query_with_stale_read,
        # sum_query_on_kind,
        # sum_query_property_filter,
        # avg_query_on_kind,
        # avg_query_property_filter,
        # multiple_aggregations_query,
        # explain_analyze_entity,
        # explain_entity,
        # explain_analyze_aggregation,
        # explain_aggregation,
        # transaction_rollback_example,
        # allocate_ids_example,
        # reserve_ids,
    ]

    for function in functions_to_call:
        print(function.__name__, function)
        pprint(function(client))
        print("\n-----------------\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Demonstrates datastore API operations.")
    parser.add_argument("project_id", help="Your cloud project ID.")

    args = parser.parse_args()

    main(args.project_id)
