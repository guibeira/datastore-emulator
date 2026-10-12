"""Shared fixtures and helpers for the parity tests.

Each parity test runs the same scenario against the Rust emulator and a
reference Datastore (the official Google emulator, or a real Cloud Datastore
database when USE_REAL_DB is set) and asserts both produce the same outcome.

Imported as a plain module (`from parity_common import ...`); pytest puts
tests/ on sys.path because tests/ has no __init__.py.
"""

import os
import uuid
from contextlib import contextmanager

import grpc
import pytest
from google.api_core import exceptions as gexc
from google.cloud import datastore
from google.cloud.datastore_v1.types import (
    CommitRequest,
    PartitionId,
    RunAggregationQueryRequest,
    RunQueryRequest,
)

USE_REAL_DB = "USE_REAL_DB" in os.environ
RUST_HOST = os.environ.get("RUST_DATASTORE_EMULATOR_HOST", "localhost:8042")
GOOGLE_HOST = os.environ.get("GOOGLE_DATASTORE_EMULATOR_HOST", "localhost:8044")
PROJECT = "test-project-2"

# Reference errors that mean "this reference cannot run the scenario", not a bug.
_SKIP_CODES = {"UNIMPLEMENTED", "UNKNOWN"}
_SKIP_INVALID_ARGUMENT_MARKERS = (
    "Unrecognized PropertyFilter.op",
    "unsupported composite property operator",
    "expected 1",
)


@contextmanager
def _emulator_host_env(host):
    """Set DATASTORE_EMULATOR_HOST to `host` (unset when None), then restore."""
    previous = os.environ.get("DATASTORE_EMULATOR_HOST")
    if host is None:
        os.environ.pop("DATASTORE_EMULATOR_HOST", None)
    else:
        os.environ["DATASTORE_EMULATOR_HOST"] = host
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("DATASTORE_EMULATOR_HOST", None)
        else:
            os.environ["DATASTORE_EMULATOR_HOST"] = previous


def emulator_client(host: str) -> datastore.Client:
    with _emulator_host_env(host):
        return datastore.Client(project=PROJECT)


def reference_client() -> datastore.Client:
    if USE_REAL_DB:
        with _emulator_host_env(None):
            return datastore.Client(
                project=os.environ["DATASTORE_PROJECT_ID"],
                database=os.environ["DATASTORE_DATABASE_NAME"],
            )
    return emulator_client(GOOGLE_HOST)


@pytest.fixture
def rust_client():
    yield emulator_client(RUST_HOST)


@pytest.fixture
def google_client():
    yield reference_client()


def unique_kind(prefix: str, length: int = 8) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:length]}"


def put_entity(client, kind, name, props, exclude=()):
    entity = datastore.Entity(key=client.key(kind, name), exclude_from_indexes=tuple(exclude))
    entity.update(props)
    client.put(entity)


def cleanup_kind(client: datastore.Client, kind: str, namespace=None) -> None:
    keys = [e.key for e in client.query(kind=kind, namespace=namespace).fetch()]
    if keys:
        client.delete_multi(keys)


def request_ids(client: datastore.Client) -> dict:
    """project_id/database_id kwargs for raw datastore_v1 requests."""
    return {"project_id": client.project, "database_id": client.database or ""}


def partition_id(client: datastore.Client, namespace=None) -> PartitionId:
    return PartitionId(namespace_id=namespace or "", **request_ids(client))


def commit_raw(client, mutations, mode=CommitRequest.Mode.NON_TRANSACTIONAL, **kwargs):
    req = CommitRequest(mode=mode, mutations=mutations, **request_ids(client), **kwargs)
    return client._datastore_api.commit(request=req)


def run_query_raw(client, query_pb, namespace=None):
    req = RunQueryRequest(
        partition_id=partition_id(client, namespace),
        query=query_pb,
        **request_ids(client),
    )
    return client._datastore_api.run_query(request=req)


def run_aggregation_raw(client, aggregation_pb, namespace=None):
    req = RunAggregationQueryRequest(
        partition_id=partition_id(client, namespace),
        aggregation_query=aggregation_pb,
        **request_ids(client),
    )
    return client._datastore_api.run_aggregation_query(request=req)


def outcome(fn):
    """Return ("ok", value), or ("err", code_name, message) for API errors."""
    try:
        return ("ok", fn())
    except gexc.GoogleAPICallError as e:
        code = e.grpc_status_code
        return ("err", code.name if code is not None else type(e).__name__, str(e.message))
    except grpc.RpcError as e:
        code = e.code() if hasattr(e, "code") else None
        details = e.details() if hasattr(e, "details") else ""
        return ("err", code.name if code is not None else type(e).__name__, str(details or ""))


def _is_error(value) -> bool:
    return (
        isinstance(value, tuple)
        and len(value) in (2, 3)
        and value[0] == "err"
        and isinstance(value[1], str)
    )


def strip_message(value):
    """Reduce every nested error outcome to ("err", code)."""
    if _is_error(value):
        return ("err", value[1])
    if isinstance(value, dict):
        return {k: strip_message(v) for k, v in value.items()}
    if isinstance(value, list):
        return [strip_message(v) for v in value]
    if isinstance(value, tuple):
        return tuple(strip_message(v) for v in value)
    return value


def _reference_skip_reason(value):
    if _is_error(value):
        code = value[1]
        message = value[2] if len(value) > 2 else ""
        if code in _SKIP_CODES:
            return f"reference does not support this: {code} {message}"
        if code == "INVALID_ARGUMENT" and any(m in message for m in _SKIP_INVALID_ARGUMENT_MARKERS):
            return f"reference does not support this: {code} {message}"
        if code == "FAILED_PRECONDITION" and "no matching index" in message:
            return f"reference needs a composite index (see tests/index.yaml): {message}"
        return None
    if isinstance(value, dict):
        items = value.values()
    elif isinstance(value, (list, tuple)):
        items = value
    else:
        return None
    for item in items:
        reason = _reference_skip_reason(item)
        if reason:
            return reason
    return None


def assert_parity(google, rust, label=""):
    """Compare reference (`google`) and Rust (`rust`) outcomes, ignoring error messages."""
    print(f"PARITY {label} google={google} rust={rust}")
    reason = _reference_skip_reason(google)
    if reason:
        pytest.skip(reason)
    assert strip_message(google) == strip_message(rust), label
