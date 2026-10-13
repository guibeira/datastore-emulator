"""Parity tests: database_id scoping.

A request's keys and partition must name the request's database, and one
database does not see another's entities. Runs each scenario on the Rust
emulator and the reference.

    poetry run pytest tests/test_parity_database.py -v
"""

import os
import sys

import pytest
from google.cloud import datastore
from google.cloud.datastore_v1.types import Key, LookupRequest
from parity_common import (  # noqa: F401
    GOOGLE_HOST,
    PROJECT,
    RUST_HOST,
    USE_REAL_DB,
    _emulator_host_env,
    assert_parity,
    google_client,
    outcome,
    rust_client,
    unique_kind,
)


def _client(host, database):
    """A client for `database` on `host` (None = the real reference)."""
    with _emulator_host_env(host):
        if host is None:
            return datastore.Client(
                project=os.environ["DATASTORE_PROJECT_ID"], database=database
            )
        return datastore.Client(project=PROJECT, database=database)


def _reference_host():
    return None if USE_REAL_DB else GOOGLE_HOST


def _named_database(host):
    if host is None:
        return os.environ["DATASTORE_DATABASE_NAME"]
    return unique_kind("db").lower()


def _both(scenario):
    results = {}
    for label, host in (("rust", RUST_HOST), ("google", _reference_host())):
        results[label] = scenario(host)
    assert_parity(results["google"], results["rust"], sys._getframe(1).f_code.co_name)


def test_key_from_another_database_is_rejected(rust_client, google_client):
    def scenario(client):
        key = Key(
            partition_id={
                "project_id": client.project,
                "database_id": "other-db",
            },
            path=[{"kind": "Item", "name": "one"}],
        )
        request = LookupRequest(
            project_id=client.project, database_id=client.database or "", keys=[key]
        )
        return outcome(lambda: client._datastore_api.lookup(request=request))

    assert_parity(scenario(google_client), scenario(rust_client))


def test_named_database_round_trip():
    def scenario(host):
        client = _client(host, _named_database(host))
        kind = unique_kind("ParityDb")
        created = []
        try:
            entity = datastore.Entity(key=client.key(kind, "one"))
            entity["score"] = 7
            client.put(entity)
            created.append(entity.key)

            fetched = client.get(client.key(kind, "one"))
            queried = list(client.query(kind=kind).fetch())
            fetched["score"] = 8
            client.put(fetched)
            allocated = client.allocate_ids(client.key(kind), 1)
            return {
                "fetched_db_matches": fetched.key.database == client.database,
                "queried_db_matches": [e.key.database == client.database for e in queried],
                "score_after_put": client.get(fetched.key)["score"],
                "allocated_db_matches": allocated[0].database == client.database,
            }
        finally:
            client.delete_multi(created)

    _both(scenario)


def _seed_two_databases(host):
    """kind/one in the default database, kind/two in a named one."""
    default = _client(host, None)
    named = _client(host, _named_database(host))
    kind = unique_kind("ParityDb")
    one = datastore.Entity(key=default.key(kind, "one"))
    default.put(one)
    two = datastore.Entity(key=named.key(kind, "two"))
    named.put(two)
    cleanup = [(default, one.key), (named, two.key)]
    return default, named, kind, cleanup


@pytest.mark.skipif(USE_REAL_DB, reason="only one real database is available")
def test_lookups_do_not_cross_databases():
    def scenario(host):
        default, named, kind, cleanup = _seed_two_databases(host)
        try:
            return {
                "named_get_of_default_entity": named.get(named.key(kind, "one")),
                "default_get_of_named_entity": default.get(default.key(kind, "two")),
            }
        finally:
            for client, key in cleanup:
                client.delete(key)

    _both(scenario)


@pytest.mark.skipif(USE_REAL_DB, reason="only one real database is available")
def test_queries_do_not_cross_databases():
    """Cloud Datastore databases are separate stores, so a kind query sees
    one database. The official emulator answers kind queries across
    databases (even with an explicit partition database_id), so this asserts
    the Rust emulator directly instead of comparing with the reference."""
    default, named, kind, cleanup = _seed_two_databases(RUST_HOST)
    try:

        def names(client):
            return sorted(e.key.name for e in client.query(kind=kind).fetch())

        assert names(default) == ["one"]
        assert names(named) == ["two"]
    finally:
        for client, key in cleanup:
            client.delete(key)


@pytest.mark.skipif(
    not USE_REAL_DB, reason="the official emulator leaves database_id empty on metadata keys"
)
def test_metadata_keys_carry_the_database():
    def scenario(host):
        client = _client(host, _named_database(host))
        kind = unique_kind("ParityDb")
        entity = datastore.Entity(key=client.key(kind, "one"))
        client.put(entity)
        try:
            query = client.query(kind="__kind__")
            query.keys_only()
            kinds = [e.key for e in query.fetch()]
            mine = [k for k in kinds if k.name == kind]
            return {
                "listed": len(mine),
                "db_matches": [k.database == client.database for k in mine],
            }
        finally:
            client.delete(entity.key)

    _both(scenario)
