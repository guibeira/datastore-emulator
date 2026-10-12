"""Parity tests for commit / allocate_ids / transaction validation.

Each scenario runs identically against the Rust emulator (8042) and the
official Google emulator (8044) and asserts both produce the same outcome.

    poetry run pytest tests/test_parity_commit.py -v -s
"""

import pytest
from google.cloud import datastore
from google.cloud.datastore import helpers
from google.cloud.datastore_v1.types import (
    CommitRequest,
    Entity,
    Key,
    Mutation,
    RollbackRequest,
    AllocateIdsRequest,
    TransactionOptions,
)
from parity_common import (  # noqa: F401
    outcome,
    cleanup_kind,
    unique_kind,
    assert_parity,
    commit_raw,
    request_ids,
    rust_client,
    google_client,
)


NON_TX = CommitRequest.Mode.NON_TRANSACTIONAL
TX = CommitRequest.Mode.TRANSACTIONAL


@pytest.fixture
def kind():
    return unique_kind("CommitParity", 10)


def _run_both(rust_client, google_client, kind, scenario):
    results = {}
    for name, client in (("rust", rust_client), ("google", google_client)):
        try:
            results[name] = outcome(lambda: scenario(client))
        finally:
            cleanup_kind(client, kind)
    return results["rust"], results["google"]


def _entity_pb(client, kind, name, **props):
    e = datastore.Entity(key=client.key(kind, name))
    e.update(props or {"v": 1})
    return helpers.entity_to_protobuf(e)


def _summary(resp):
    return [len(resp.mutation_results)]


# 1 ---------------------------------------------------------------------------
def test_insert_existing_key(rust_client, google_client, kind):
    def scenario(c):
        commit_raw(c, [Mutation(upsert=_entity_pb(c, kind, "k"))])
        resp = commit_raw(c, [Mutation(insert=_entity_pb(c, kind, "k", v=2))])
        return _summary(resp)

    rust, google = _run_both(rust_client, google_client, kind, scenario)
    assert_parity(google, rust, "test_insert_existing_key")


# 2 ---------------------------------------------------------------------------
def test_double_upsert_same_key(rust_client, google_client, kind):
    def scenario(c):
        resp = commit_raw(
            c,
            [
                Mutation(upsert=_entity_pb(c, kind, "k", v=1)),
                Mutation(upsert=_entity_pb(c, kind, "k", v=2)),
            ],
        )
        return _summary(resp)

    rust, google = _run_both(rust_client, google_client, kind, scenario)
    assert_parity(google, rust, "test_double_upsert_same_key")


# 3 ---------------------------------------------------------------------------
def test_insert_then_delete_same_key(rust_client, google_client, kind):
    def scenario(c):
        resp = commit_raw(
            c,
            [
                Mutation(insert=_entity_pb(c, kind, "k")),
                Mutation(delete=c.key(kind, "k").to_protobuf()),
            ],
        )
        return _summary(resp)

    rust, google = _run_both(rust_client, google_client, kind, scenario)
    assert_parity(google, rust, "test_insert_then_delete_same_key")


# 4 ---------------------------------------------------------------------------
def test_upsert_then_update_same_key(rust_client, google_client, kind):
    def scenario(c):
        resp = commit_raw(
            c,
            [
                Mutation(upsert=_entity_pb(c, kind, "k", v=1)),
                Mutation(update=_entity_pb(c, kind, "k", v=2)),
            ],
        )
        return _summary(resp)

    rust, google = _run_both(rust_client, google_client, kind, scenario)
    assert_parity(google, rust, "test_upsert_then_update_same_key")


# 5 ---------------------------------------------------------------------------
def test_put_multi_duplicate_key(rust_client, google_client, kind):
    def scenario(c):
        e1 = datastore.Entity(key=c.key(kind, "k"))
        e1["v"] = 1
        e2 = datastore.Entity(key=c.key(kind, "k"))
        e2["v"] = 2
        c.put_multi([e1, e2])
        return "done"

    rust, google = _run_both(rust_client, google_client, kind, scenario)
    assert_parity(google, rust, "test_put_multi_duplicate_key")


# 6 ---------------------------------------------------------------------------
def test_allocate_ids_complete_key(rust_client, google_client, kind):
    def scenario(c):
        # The high-level client rejects complete keys locally, so go raw.
        resp = c._datastore_api.allocate_ids(
            request=AllocateIdsRequest(
                **request_ids(c), keys=[c.key(kind, "named").to_protobuf()]
            )
        )
        return len(resp.keys)

    rust, google = _run_both(rust_client, google_client, kind, scenario)
    assert_parity(google, rust, "test_allocate_ids_complete_key")


def test_allocate_ids_incomplete_parent(rust_client, google_client, kind):
    def scenario(c):
        key = Key(
            partition_id=request_ids(c),
            path=[Key.PathElement(kind=kind), Key.PathElement(kind=kind)],
        )
        resp = c._datastore_api.allocate_ids(
            request=AllocateIdsRequest(**request_ids(c), keys=[key])
        )
        return len(resp.keys)

    rust, google = _run_both(rust_client, google_client, kind, scenario)
    assert_parity(google, rust, "test_allocate_ids_incomplete_parent")


# 7 ---------------------------------------------------------------------------
def test_update_missing_key(rust_client, google_client, kind):
    def scenario(c):
        return _summary(commit_raw(c, [Mutation(update=_entity_pb(c, kind, "missing"))]))

    rust, google = _run_both(rust_client, google_client, kind, scenario)
    assert_parity(google, rust, "test_update_missing_key")


# 8 ---------------------------------------------------------------------------
def test_delete_missing_key(rust_client, google_client, kind):
    def scenario(c):
        resp = commit_raw(c, [Mutation(delete=c.key(kind, "missing").to_protobuf())])
        return _summary(resp)

    rust, google = _run_both(rust_client, google_client, kind, scenario)
    assert_parity(google, rust, "test_delete_missing_key")


# 9 ---------------------------------------------------------------------------
def test_delete_existing_version_nonzero(rust_client, google_client, kind):
    def scenario(c):
        v_ins = commit_raw(c, [Mutation(insert=_entity_pb(c, kind, "k"))]).mutation_results[0].version
        v_upd = commit_raw(c, [Mutation(update=_entity_pb(c, kind, "k", v=2))]).mutation_results[0].version
        v_del = (
            commit_raw(c, [Mutation(delete=c.key(kind, "k").to_protobuf())])
            .mutation_results[0]
            .version
        )
        print(f"\n    versions insert={v_ins} update={v_upd} delete={v_del}")
        return {"delete_nonzero": v_del != 0, "monotonic": v_ins < v_upd < (v_del or v_upd + 1)}

    rust, google = _run_both(rust_client, google_client, kind, scenario)
    assert_parity(google, rust, "test_delete_existing_version_nonzero")


# 10 --------------------------------------------------------------------------
def test_commit_already_committed_transaction(rust_client, google_client, kind):
    def scenario(c):
        tx = c.transaction()
        tx.begin()
        e = datastore.Entity(key=c.key(kind, "k"))
        e["v"] = 1
        tx.put(e)
        tx_id = tx.id  # the client clears tx.id after commit
        tx.commit()
        resp = commit_raw(c, [Mutation(upsert=_entity_pb(c, kind, "k2"))], mode=TX, transaction=tx_id)
        return _summary(resp)

    rust, google = _run_both(rust_client, google_client, kind, scenario)
    assert_parity(google, rust, "test_commit_already_committed_transaction")


def test_commit_rolled_back_transaction(rust_client, google_client, kind):
    def scenario(c):
        tx = c.transaction()
        tx.begin()
        tx_id = tx.id  # the client clears tx.id after rollback
        tx.rollback()
        resp = commit_raw(c, [Mutation(upsert=_entity_pb(c, kind, "k"))], mode=TX, transaction=tx_id)
        return _summary(resp)

    rust, google = _run_both(rust_client, google_client, kind, scenario)
    assert_parity(google, rust, "test_commit_rolled_back_transaction")


def test_transactional_commit_without_selector(rust_client, google_client, kind):
    def scenario(c):
        return _summary(commit_raw(c, [Mutation(upsert=_entity_pb(c, kind, "k"))], mode=TX))

    rust, google = _run_both(rust_client, google_client, kind, scenario)
    assert_parity(google, rust, "test_transactional_commit_without_selector")


def test_rollback_unknown_transaction(rust_client, google_client, kind):
    def scenario(c):
        c._datastore_api.rollback(
            request=RollbackRequest(**request_ids(c), transaction=b"made-up-tx-id")
        )
        return "ok"

    rust, google = _run_both(rust_client, google_client, kind, scenario)
    assert_parity(google, rust, "test_rollback_unknown_transaction")


# 11 --------------------------------------------------------------------------
def test_single_use_transaction_commit(rust_client, google_client, kind):
    def scenario(c):
        resp = commit_raw(
            c,
            [Mutation(upsert=_entity_pb(c, kind, "k"))],
            mode=TX,
            single_use_transaction=TransactionOptions(read_write=TransactionOptions.ReadWrite()),
        )
        return _summary(resp)

    rust, google = _run_both(rust_client, google_client, kind, scenario)
    assert_parity(google, rust, "test_single_use_transaction_commit")


# 12 --------------------------------------------------------------------------
def test_empty_key_path(rust_client, google_client, kind):
    def scenario(c):
        empty = Key(partition_id=request_ids(c), path=[])
        try:
            return _summary(commit_raw(c, [Mutation(upsert=Entity(key=empty))]))
        finally:
            outcome(lambda: commit_raw(c, [Mutation(delete=empty)]))

    rust, google = _run_both(rust_client, google_client, kind, scenario)
    assert_parity(google, rust, "test_empty_key_path")


@pytest.mark.parametrize("prop", ["__key__", "__foo__"])
def test_reserved_property_name(rust_client, google_client, kind, prop):
    def scenario(c):
        e = datastore.Entity(key=c.key(kind, "k"))
        e[prop] = 1
        return _summary(commit_raw(c, [Mutation(upsert=helpers.entity_to_protobuf(e))]))

    rust, google = _run_both(rust_client, google_client, kind, scenario)
    assert_parity(google, rust, "test_reserved_property_name")
