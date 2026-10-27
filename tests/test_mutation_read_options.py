"""Regression coverage for mutation and read-option API semantics."""

from datetime import datetime, timezone
import time
import uuid

from google.api_core.exceptions import InvalidArgument
from google.cloud import datastore
from google.cloud.datastore import helpers
from google.cloud.datastore_v1 import types
import pytest
from parity_common import RUST_HOST, _emulator_host_env


@pytest.fixture
def client():
    with _emulator_host_env(RUST_HOST):
        yield datastore.Client(project="mutation-read-options", namespace=uuid.uuid4().hex)


def put(client, name="one", **properties):
    entity = datastore.Entity(key=client.key("Probe", name))
    entity.update(properties)
    client.put(entity)
    return entity


def commit(client, mutation):
    return client._datastore_api.commit(
        request={
            "project_id": client.project,
            "mode": types.CommitRequest.Mode.NON_TRANSACTIONAL,
            "mutations": [mutation],
        },
        timeout=5,
    )


def test_property_mask_preserves_unselected_fields(client):
    original = put(client, changed="before", preserved="keep me")
    update = datastore.Entity(key=original.key)
    update.update(changed="after", preserved="must be ignored")
    commit(client, types.Mutation(
        upsert=helpers.entity_to_protobuf(update),
        property_mask=types.PropertyMask(paths=["changed"]),
    ))
    actual = client.get(original.key)
    assert actual["changed"] == "after"
    assert actual["preserved"] == "keep me"


@pytest.mark.parametrize("operation,initial,operand,expected", [
    ("increment", 10, 3, 13),
    ("maximum", 10, 20, 20),
    ("minimum", 10, 5, 5),
])
def test_numeric_property_transform(client, operation, initial, operand, expected):
    original = put(client, value=initial)
    transform = types.PropertyTransform(property="value", **{operation: types.Value(integer_value=operand)})
    commit(client, types.Mutation(upsert=helpers.entity_to_protobuf(original), property_transforms=[transform]))
    assert client.get(original.key)["value"] == expected


@pytest.mark.parametrize("operation,initial,operand,expected", [
    ("append_missing_elements", [1, 2], [2, 3], [1, 2, 3]),
    ("remove_all_from_array", [1, 1, 2, 3], [1], [2, 3]),
])
def test_array_property_transform(client, operation, initial, operand, expected):
    original = put(client, value=initial)
    values = types.ArrayValue(values=[types.Value(integer_value=value) for value in operand])
    transform = types.PropertyTransform(property="value", **{operation: values})
    commit(client, types.Mutation(upsert=helpers.entity_to_protobuf(original), property_transforms=[transform]))
    assert client.get(original.key)["value"] == expected


def test_lookup_starts_requested_transaction(client):
    original = put(client, value="present")
    response = client._datastore_api.lookup(request={
        "project_id": client.project,
        "keys": [original.key.to_protobuf()],
        "read_options": types.ReadOptions(new_transaction=types.TransactionOptions(read_write={})),
    }, timeout=5)
    try:
        assert len(response.found) == 1
        assert response.transaction, "Lookup(new_transaction=...) must return a transaction ID"
    finally:
        if response.transaction:
            client._datastore_api.rollback(request={"project_id": client.project, "transaction": response.transaction}, timeout=5)


@pytest.fixture
def historical_entity(client):
    entity = put(client, value="before")
    time.sleep(1)
    timestamp = datetime.now(timezone.utc)
    time.sleep(1)
    entity["value"] = "after"
    client.put(entity)
    return entity, timestamp


def test_lookup_honors_read_time(client, historical_entity):
    entity, timestamp = historical_entity
    assert client.get(entity.key)["value"] == "after"
    assert client.get(entity.key, read_time=timestamp)["value"] == "before"


def test_query_honors_read_time(client, historical_entity):
    entity, timestamp = historical_entity
    query = client.query(kind="Probe")
    assert list(query.fetch())[0]["value"] == "after"
    assert list(query.fetch(read_time=timestamp))[0]["value"] == "before"


def count(client):
    query = client.aggregation_query(client.query(kind="Probe"))
    query.count(alias="total")
    return list(query.fetch())[0][0].value


def test_aggregation_uses_transaction_snapshot(client):
    with _emulator_host_env(RUST_HOST):
        writer = datastore.Client(project=client.project, namespace=client.namespace)
    with client.transaction(read_only=True):
        assert count(client) == 0
        put(writer, value="outside the transaction")
        assert count(client) == 0


@pytest.mark.parametrize("operation", ["sum", "avg"])
def test_aggregation_rejects_key_property(client, operation):
    put(client, value=1)
    query = client.aggregation_query(client.query(kind="Probe"))
    getattr(query, operation)("__key__", alias="invalid")
    with pytest.raises(InvalidArgument):
        list(query.fetch())
