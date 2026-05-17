"""Parity tests for the 5 query bugs from issue #26.

Each test inserts the same entities into both emulators (Rust port 8042,
Google port 8044) and asserts they return the same query result. If the
Google emulator ever disagrees with our expectations, the canonical
behavior we are encoding in the Rust emulator is wrong and the plan
needs to be revisited.

Run after `docker compose up -d datastore-emulator-rust datastore-emulator-google`:

    poetry run pytest tests/test_query_bugs_parity.py -v
"""

import datetime
import os
import uuid

import pytest
from google.cloud import datastore
from google.cloud.datastore.query import PropertyFilter

RUST_HOST = os.environ.get("RUST_DATASTORE_EMULATOR_HOST", "localhost:8042")
GOOGLE_HOST = os.environ.get("GOOGLE_DATASTORE_EMULATOR_HOST", "localhost:8044")
PROJECT = "test-project-2"


def _client(host: str) -> datastore.Client:
    previous = os.environ.get("DATASTORE_EMULATOR_HOST")
    os.environ["DATASTORE_EMULATOR_HOST"] = host
    try:
        return datastore.Client(project=PROJECT)
    finally:
        if previous is None:
            os.environ.pop("DATASTORE_EMULATOR_HOST", None)
        else:
            os.environ["DATASTORE_EMULATOR_HOST"] = previous


def _cleanup(client: datastore.Client, kind: str) -> None:
    q = client.query(kind=kind)
    keys = [e.key for e in q.fetch()]
    if keys:
        client.delete_multi(keys)


@pytest.fixture
def rust_client():
    c = _client(RUST_HOST)
    yield c


@pytest.fixture
def google_client():
    c = _client(GOOGLE_HOST)
    yield c


def _insert_entity(client, kind, name, props):
    key = client.key(kind, name)
    entity = datastore.Entity(key=key)
    for k, v in props.items():
        entity[k] = v
    client.put(entity)


def _insert_with_exclusion(client, kind, name, prop, value):
    """Insert with `prop` flagged exclude_from_indexes."""
    key = client.key(kind, name)
    entity = datastore.Entity(key=key, exclude_from_indexes=(prop,))
    entity[prop] = value
    client.put(entity)


def _names_from(results):
    return [e.key.name for e in results]


# ---------------------------------------------------------------------------
# Bug 1: cross-type ordering
# ---------------------------------------------------------------------------


def test_cross_type_ordering_parity(rust_client, google_client):
    """null < int/double < timestamp < bool < string per Datastore spec."""
    kind = f"CrossType_{uuid.uuid4().hex[:8]}"
    try:
        ts = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        rows = [
            ("str", "a"),
            ("bool", True),
            ("ts", ts),
            ("int", 0),
            ("null", None),
        ]
        for client in (rust_client, google_client):
            for name, value in rows:
                _insert_entity(client, kind, name, {"score": value})

        def fetch(client):
            q = client.query(kind=kind)
            q.order = ["score"]
            return _names_from(q.fetch())

        rust = fetch(rust_client)
        google = fetch(google_client)
        assert rust == google, f"rust={rust} google={google}"
    finally:
        _cleanup(rust_client, kind)
        _cleanup(google_client, kind)


# ---------------------------------------------------------------------------
# Bug 2: implicit ordering for inequality filters
# ---------------------------------------------------------------------------


def test_implicit_order_on_inequality_parity(rust_client, google_client):
    """Inequality filter with no explicit order should sort by that property."""
    kind = f"Implicit_{uuid.uuid4().hex[:8]}"
    try:
        rows = [("a", 30), ("b", 10), ("c", 20)]
        for client in (rust_client, google_client):
            for name, age in rows:
                _insert_entity(client, kind, name, {"age": age})

        def fetch(client):
            q = client.query(kind=kind)
            q.add_filter(filter=PropertyFilter("age", ">", 0))
            return _names_from(q.fetch())

        rust = fetch(rust_client)
        google = fetch(google_client)
        assert rust == google, f"rust={rust} google={google}"
    finally:
        _cleanup(rust_client, kind)
        _cleanup(google_client, kind)


# ---------------------------------------------------------------------------
# Bug 3: exclude_from_indexes on filters
# ---------------------------------------------------------------------------


def test_exclude_from_indexes_filter_parity(rust_client, google_client):
    """Entities with `tag` excluded from indexes should not match filters."""
    kind = f"Excluded_{uuid.uuid4().hex[:8]}"
    try:
        for client in (rust_client, google_client):
            _insert_entity(client, kind, "indexed", {"score": 50})
            _insert_with_exclusion(client, kind, "excluded", "score", 50)

        def fetch(client):
            q = client.query(kind=kind)
            q.add_filter(filter=PropertyFilter("score", ">", 0))
            q.order = ["score"]
            return _names_from(q.fetch())

        rust = fetch(rust_client)
        google = fetch(google_client)
        assert rust == google, f"rust={rust} google={google}"
    finally:
        _cleanup(rust_client, kind)
        _cleanup(google_client, kind)


# ---------------------------------------------------------------------------
# Bug 4: Query.offset
# ---------------------------------------------------------------------------


def test_offset_parity(rust_client, google_client):
    """Offset must skip N results after sorting."""
    kind = f"Offset_{uuid.uuid4().hex[:8]}"
    try:
        rows = [("a", 1), ("b", 2), ("c", 3), ("d", 4), ("e", 5)]
        for client in (rust_client, google_client):
            for name, score in rows:
                _insert_entity(client, kind, name, {"score": score})

        def fetch(client):
            q = client.query(kind=kind)
            q.order = ["score"]
            return _names_from(q.fetch(offset=2))

        rust = fetch(rust_client)
        google = fetch(google_client)
        assert rust == google, f"rust={rust} google={google}"
    finally:
        _cleanup(rust_client, kind)
        _cleanup(google_client, kind)


# ---------------------------------------------------------------------------
# Bug 5: stale index purge on update
# ---------------------------------------------------------------------------


def test_update_purges_stale_index_parity(rust_client, google_client):
    """Query for old value after update returns nothing in both emulators."""
    kind = f"Stale_{uuid.uuid4().hex[:8]}"
    try:
        for client in (rust_client, google_client):
            key = client.key(kind, "x")
            entity = datastore.Entity(key=key)
            entity["tag"] = "old"
            client.put(entity)
            # Update.
            entity["tag"] = "new"
            client.put(entity)

        def fetch_old(client):
            q = client.query(kind=kind)
            q.add_filter(filter=PropertyFilter("tag", "=", "old"))
            return _names_from(q.fetch())

        rust = fetch_old(rust_client)
        google = fetch_old(google_client)
        assert rust == google == [], f"rust={rust} google={google}"
    finally:
        _cleanup(rust_client, kind)
        _cleanup(google_client, kind)
