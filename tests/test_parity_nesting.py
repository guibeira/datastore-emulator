"""Embedded entity depth follows Datastore's 20-level limit."""

import pytest
from google.api_core.exceptions import InvalidArgument
from google.cloud.datastore_v1 import types

from parity_common import google_client, request_ids, rust_client, unique_kind  # noqa: F401


def _entity(value):
    return types.Value(entity_value=types.Entity(properties={"child": value}))


def _array(value):
    return types.Value(array_value=types.ArrayValue(values=[value]))


def _nested(shape, depth):
    value = types.Value(integer_value=1)
    if shape == "empty_entity":
        value = types.Value(entity_value=types.Entity())
        depth -= 1
    elif shape == "empty_array":
        value = types.Value(array_value=types.ArrayValue())
    elif shape == "wide":
        return types.Value(entity_value=types.Entity(properties={
            f"p{i}": _entity(value) for i in range(depth)
        }))
    for _ in range(depth):
        value = _entity(value)
        if shape == "alternating":
            value = _array(value)
    if shape == "array":
        value = _array(value)
    if shape == "unindexed":
        value.exclude_from_indexes = True
    return value


def _commit(client, mutation):
    return client._datastore_api.commit(request=types.CommitRequest(
        **request_ids(client), mode=types.CommitRequest.Mode.NON_TRANSACTIONAL,
        mutations=[mutation],
    ))


@pytest.mark.parametrize("operation", ["insert", "update", "upsert"])
@pytest.mark.parametrize("shape,depth,accepted", [
    ("entity", 20, True),
    ("entity", 21, False),
    ("entity", 25, False),
    ("array", 20, True),
    ("array", 21, False),
    ("empty_entity", 20, True),
    ("empty_entity", 21, False),
    ("empty_array", 20, True),
    ("empty_array", 21, False),
    ("alternating", 10, True),
    ("alternating", 11, True),
    ("wide", 25, True),
    ("unindexed", 20, True),
    ("unindexed", 21, False),
])
def test_commit_enforces_entity_nesting_limit(
    rust_client, google_client, operation, shape, depth, accepted,
):
    for client in (google_client, rust_client):
        key = client.key(unique_kind("Nesting"), "one").to_protobuf()
        try:
            if operation == "update":
                _commit(client, types.Mutation(insert=types.Entity(
                    key=key, properties={"before": types.Value(integer_value=1)},
                )))
            entity = types.Entity(key=key, properties={"prop": _nested(shape, depth)})
            mutation = types.Mutation(**{operation: entity})
            if accepted:
                _commit(client, mutation)
                response = client._datastore_api.lookup(
                    request={**request_ids(client), "keys": [key]},
                )
                assert response.found[0].entity.properties["prop"] == entity.properties["prop"]
            else:
                with pytest.raises(InvalidArgument, match="At most 20 nested entity values"):
                    _commit(client, mutation)
                response = client._datastore_api.lookup(
                    request={**request_ids(client), "keys": [key]},
                )
                if operation == "update":
                    assert response.found[0].entity.properties == {
                        "before": types.Value(integer_value=1),
                    }
                else:
                    assert not response.found
        finally:
            _commit(client, types.Mutation(delete=key))
