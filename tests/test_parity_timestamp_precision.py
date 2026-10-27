"""Timestamp properties and equality filters use microsecond precision."""

import pytest
from google.cloud.datastore_v1 import types
from google.protobuf.timestamp_pb2 import Timestamp

from parity_common import (  # noqa: F401
    cleanup_kind,
    google_client,
    request_ids,
    run_query_raw,
    rust_client,
    unique_kind,
)


def _timestamp(seconds, nanos):
    return types.Value(timestamp_value=Timestamp(seconds=seconds, nanos=nanos))


def _properties(shape, value):
    if shape == "array":
        value = types.Value(array_value=types.ArrayValue(values=[value]))
    if shape == "nested":
        return {"nested": types.Value(entity_value=types.Entity(properties={"time": value}))}
    return {"time": value}


def _write(client, kind, shape, seconds):
    key = client.key(kind, "one").to_protobuf()
    client._datastore_api.commit(request=types.CommitRequest(
        **request_ids(client), mode=types.CommitRequest.Mode.NON_TRANSACTIONAL,
        mutations=[types.Mutation(upsert=types.Entity(
            key=key, properties=_properties(shape, _timestamp(seconds, 123456789)),
        ))],
    ))
    return key


@pytest.mark.parametrize("seconds", [-1, 1730000000])
@pytest.mark.parametrize("shape", ["scalar", "array", "nested"])
def test_timestamp_equality_uses_microsecond_precision(rust_client, google_client, seconds, shape):
    for client in (google_client, rust_client):
        kind = unique_kind("TimestampEquality")
        try:
            _write(client, kind, shape, seconds)
            property_name = "nested.time" if shape == "nested" else "time"
            for nanos, expected in [(123456999, 1), (123457000, 0)]:
                query = types.Query(kind=[types.KindExpression(name=kind)], filter=types.Filter(
                    property_filter=types.PropertyFilter(
                        property=types.PropertyReference(name=property_name),
                        op=types.PropertyFilter.Operator.EQUAL,
                        value=_timestamp(seconds, nanos),
                    ),
                ))
                assert len(run_query_raw(client, query).batch.entity_results) == expected
        finally:
            cleanup_kind(client, kind)


@pytest.mark.parametrize("seconds", [-1, 1730000000])
@pytest.mark.parametrize("shape", ["scalar", "array", "nested"])
def test_lookup_returns_timestamps_at_microsecond_precision(rust_client, google_client, seconds, shape):
    for client in (google_client, rust_client):
        kind = unique_kind("TimestampLookup")
        try:
            key = _write(client, kind, shape, seconds)
            response = client._datastore_api.lookup(request={**request_ids(client), "keys": [key]})
            properties = response.found[0].entity.properties
            if shape == "nested":
                properties = properties["nested"].entity_value.properties
            value = properties["time"]
            if shape == "array":
                value = value.array_value.values[0]
            assert value._pb.timestamp_value.nanos == 123456000
        finally:
            cleanup_kind(client, kind)
