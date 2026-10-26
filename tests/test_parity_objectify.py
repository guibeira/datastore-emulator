"""Regressions found by running Objectify's tests against both emulators."""

from contextlib import contextmanager

import pytest
from google.api_core.exceptions import InvalidArgument
from google.cloud.datastore_v1.types import (
    ArrayValue,
    CommitRequest,
    Entity,
    LookupRequest,
    Mutation,
    ReadOptions,
    TransactionOptions,
    Value,
)
from parity_common import (  # noqa: F401
    assert_parity,
    commit_raw,
    google_client,
    outcome,
    request_ids,
    rust_client,
    strip_message,
    unique_kind,
)


@pytest.fixture
def kind():
    return unique_kind("Objectify")


def _both(rust, google, kind, scenario, expected):
    results = []
    for client in (google, rust):
        try:
            results.append(outcome(lambda: scenario(client)))
        finally:
            query = client.query(kind=kind)
            query.keys_only()
            client.delete_multi([entity.key for entity in query.fetch()])
    assert_parity(results[0], results[1], kind)
    assert strip_message(results[0]) == expected


def _entity(client, kind, value=1, name="item"):
    return Entity(
        key=client.key(kind, name).to_protobuf(),
        properties={"v": Value(integer_value=value)},
    )


@contextmanager
def _transaction(client, read_only=False):
    options = (
        TransactionOptions(read_only=TransactionOptions.ReadOnly())
        if read_only
        else TransactionOptions(read_write=TransactionOptions.ReadWrite())
    )
    transaction = client._datastore_api.begin_transaction(
        request=dict(**request_ids(client), transaction_options=options)
    ).transaction
    try:
        yield transaction
    finally:
        try:
            client._datastore_api.rollback(
                request=dict(**request_ids(client), transaction=transaction)
            )
        except InvalidArgument:
            pass  # Committed or aborted transactions no longer exist.


def _lookup(client, kind, transaction):
    response = client._datastore_api.lookup(
        request=LookupRequest(
            **request_ids(client),
            keys=[client.key(kind, "item").to_protobuf()],
            read_options=ReadOptions(transaction=transaction),
        )
    )
    return response.found[0].entity.properties["v"].integer_value if response.found else None


def _commit(client, transaction, mutations):
    commit_raw(
        client,
        mutations,
        mode=CommitRequest.Mode.TRANSACTIONAL,
        transaction=transaction,
    )


@pytest.mark.parametrize("operation", ["insert", "update", "upsert", "delete"])
def test_read_only_transactions_reject_mutations(rust_client, google_client, kind, operation):
    def scenario(client):
        entity = _entity(client, kind)
        if operation != "insert":
            commit_raw(client, [Mutation(upsert=entity)])
        mutation = Mutation(**{operation: entity.key if operation == "delete" else entity})
        with _transaction(client, read_only=True) as transaction:
            _commit(client, transaction, [mutation])

    _both(rust_client, google_client, kind, scenario, ("err", "INVALID_ARGUMENT"))


def test_read_only_transaction_can_commit_without_mutations(rust_client, google_client, kind):
    def scenario(client):
        with _transaction(client, read_only=True) as transaction:
            _commit(client, transaction, [])

    _both(rust_client, google_client, kind, scenario, ("ok", None))


@pytest.mark.parametrize("has_mutations", [False, True])
def test_single_use_commit_requires_read_write(rust_client, google_client, kind, has_mutations):
    def scenario(client):
        commit_raw(
            client,
            [Mutation(upsert=_entity(client, kind))] if has_mutations else [],
            mode=CommitRequest.Mode.TRANSACTIONAL,
            single_use_transaction=TransactionOptions(read_only=TransactionOptions.ReadOnly()),
        )

    _both(rust_client, google_client, kind, scenario, ("err", "INVALID_ARGUMENT"))


@pytest.mark.parametrize("read_only", [False, True])
def test_repeated_lookup_preserves_first_read(rust_client, google_client, kind, read_only):
    def scenario(client):
        commit_raw(client, [Mutation(upsert=_entity(client, kind, 1))])
        with _transaction(client, read_only) as transaction:
            first = _lookup(client, kind, transaction)
            commit_raw(client, [Mutation(upsert=_entity(client, kind, 2))])
            return first, _lookup(client, kind, transaction)

    _both(rust_client, google_client, kind, scenario, ("ok", (1, 1)))


@pytest.mark.parametrize("change", ["update", "same_value", "delete", "recreate"])
def test_lookup_conflict_aborts_commit(rust_client, google_client, kind, change):
    def scenario(client):
        original = _entity(client, kind, 1)
        commit_raw(client, [Mutation(upsert=original)])
        with _transaction(client) as transaction:
            _lookup(client, kind, transaction)
            if change in ("delete", "recreate"):
                commit_raw(client, [Mutation(delete=original.key)])
            if change != "delete":
                value = 1 if change == "same_value" else 2
                commit_raw(client, [Mutation(upsert=_entity(client, kind, value))])
            _commit(client, transaction, [Mutation(upsert=_entity(client, kind, 9))])

    _both(rust_client, google_client, kind, scenario, ("err", "ABORTED"))


@pytest.mark.parametrize("delete_again", [False, True])
def test_missing_lookup_detects_intervening_insert(rust_client, google_client, kind, delete_again):
    def scenario(client):
        with _transaction(client) as transaction:
            assert _lookup(client, kind, transaction) is None
            entity = _entity(client, kind)
            commit_raw(client, [Mutation(upsert=entity)])
            if delete_again:
                commit_raw(client, [Mutation(delete=entity.key)])
            _commit(client, transaction, [Mutation(upsert=_entity(client, kind, 9))])

    _both(rust_client, google_client, kind, scenario, ("err", "ABORTED"))


def test_change_before_first_lookup_does_not_conflict(rust_client, google_client, kind):
    def scenario(client):
        commit_raw(client, [Mutation(upsert=_entity(client, kind, 1))])
        with _transaction(client) as transaction:
            commit_raw(client, [Mutation(upsert=_entity(client, kind, 2))])
            value = _lookup(client, kind, transaction)
            _commit(client, transaction, [Mutation(upsert=_entity(client, kind, 3))])
            return value

    _both(rust_client, google_client, kind, scenario, ("ok", 2))


def test_unrelated_write_does_not_abort_lookup_transaction(rust_client, google_client, kind):
    def scenario(client):
        commit_raw(client, [Mutation(upsert=_entity(client, kind, 1))])
        with _transaction(client) as transaction:
            _lookup(client, kind, transaction)
            commit_raw(client, [Mutation(upsert=_entity(client, kind, 2, name="other"))])
            _commit(client, transaction, [Mutation(upsert=_entity(client, kind, 3))])

    _both(rust_client, google_client, kind, scenario, ("ok", None))


def test_conflict_leaves_all_mutations_unapplied(rust_client, google_client, kind):
    def scenario(client):
        commit_raw(client, [Mutation(upsert=_entity(client, kind, 1))])
        with _transaction(client) as transaction:
            _lookup(client, kind, transaction)
            commit_raw(client, [Mutation(upsert=_entity(client, kind, 2))])
            result = outcome(lambda: _commit(client, transaction, [
                Mutation(upsert=_entity(client, kind, 3, name="other")),
                Mutation(upsert=_entity(client, kind, 9)),
            ]))
            entities = list(client.query(kind=kind).fetch())
            return result, [(entity.key.name, entity["v"]) for entity in entities]

    _both(
        rust_client, google_client, kind, scenario,
        ("ok", (("err", "ABORTED"), [("item", 2)])),
    )


@pytest.mark.parametrize("value_type", ["blob_value", "string_value"])
@pytest.mark.parametrize("length,excluded", [(1500, False), (1501, False), (1501, True)])
def test_indexed_value_size_limit(rust_client, google_client, kind, value_type, length, excluded):
    def scenario(client):
        content = b"x" * length if value_type == "blob_value" else "x" * length
        value = Value(**{value_type: content}, exclude_from_indexes=excluded)
        entity = _entity(client, kind)
        entity.properties["data"] = value
        commit_raw(client, [Mutation(upsert=entity)])

    expected = ("err", "INVALID_ARGUMENT") if length > 1500 and not excluded else ("ok", None)
    _both(rust_client, google_client, kind, scenario, expected)


def test_indexed_string_limit_counts_utf8_bytes(rust_client, google_client, kind):
    def scenario(client):
        entity = _entity(client, kind)
        entity.properties["data"] = Value(string_value="é" * 751)
        commit_raw(client, [Mutation(upsert=entity)])

    _both(rust_client, google_client, kind, scenario, ("err", "INVALID_ARGUMENT"))


@pytest.mark.parametrize("container", ["array", "entity"])
@pytest.mark.parametrize("excluded", [False, True])
def test_nested_indexed_value_size_limit(rust_client, google_client, kind, container, excluded):
    def scenario(client):
        value = Value(blob_value=b"x" * 1501, exclude_from_indexes=excluded)
        value = (
            Value(array_value=ArrayValue(values=[value]))
            if container == "array"
            else Value(entity_value=Entity(properties={"nested": value}))
        )
        entity = _entity(client, kind)
        entity.properties["data"] = value
        commit_raw(client, [Mutation(upsert=entity)])

    expected = ("ok", None) if excluded else ("err", "INVALID_ARGUMENT")
    _both(rust_client, google_client, kind, scenario, expected)


def test_unindexed_parent_does_not_exempt_indexed_child(rust_client, google_client, kind):
    def scenario(client):
        entity = _entity(client, kind)
        entity.properties["data"] = Value(
            entity_value=Entity(properties={"nested": Value(blob_value=b"x" * 1501)}),
            exclude_from_indexes=True,
        )
        commit_raw(client, [Mutation(upsert=entity)])

    _both(rust_client, google_client, kind, scenario, ("err", "INVALID_ARGUMENT"))


@pytest.mark.parametrize("nested", [False, True])
def test_arrays_keep_order_within_each_index_group(rust_client, google_client, kind, nested):
    def scenario(client):
        values = [
            Value(integer_value=123, exclude_from_indexes=True),
            Value(integer_value=456),
            Value(integer_value=234, exclude_from_indexes=True),
            Value(integer_value=567),
        ]
        value = Value(array_value=ArrayValue(values=values))
        if nested:
            value = Value(entity_value=Entity(properties={"list": value}))
        entity = _entity(client, kind)
        entity.properties["data"] = value
        commit_raw(client, [Mutation(upsert=entity)])
        fetched = client._datastore_api.lookup(
            request=LookupRequest(**request_ids(client), keys=[entity.key])
        ).found[0].entity.properties["data"]
        if nested:
            fetched = fetched.entity_value.properties["list"]
        return [(v.integer_value, v.exclude_from_indexes) for v in fetched.array_value.values]

    _both(
        rust_client, google_client, kind, scenario,
        ("ok", [(456, False), (567, False), (123, True), (234, True)]),
    )
