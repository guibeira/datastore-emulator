"""Parity: ordering when the sort property is missing, unindexed, null, array or mixed-type.

    poetry run pytest tests/test_parity_order_missing.py -v
"""

from google.cloud import datastore
from google.cloud.datastore.query import PropertyFilter
from parity_common import (  # noqa: F401
    outcome,
    cleanup_kind,
    unique_kind,
    put_entity,
    assert_parity,
    rust_client,
    google_client,
)


def _label(key):
    return key.name if key.name is not None else f"id:{key.id}"


def _run(client, kind, setup, query_fn):
    try:
        setup(client, kind)
        return outcome(lambda: query_fn(client, kind))
    finally:
        cleanup_kind(client, kind)


def _both(rust_client, google_client, prefix, setup, query_fn):
    rust = _run(rust_client, unique_kind(prefix), setup, query_fn)
    google = _run(google_client, unique_kind(prefix), setup, query_fn)
    return rust, google


def _ordered(order, limit=None, keys_only=False, filters=()):
    def q(client, kind):
        query = client.query(kind=kind)
        for f in filters:
            query.add_filter(filter=PropertyFilter(*f))
        if keys_only:
            query.keys_only()
        query.order = order
        return [_label(e.key) for e in query.fetch(limit=limit)]

    return q


def _setup_three(client, kind):
    put_entity(client, kind, "a", {"score": 2})
    put_entity(client, kind, "b", {"score": 1})
    put_entity(client, kind, "c", {"other": 1})


def test_1a_missing_property_asc(rust_client, google_client):
    rust, google = _both(rust_client, google_client, "OM1a", _setup_three, _ordered(["score"]))
    assert_parity(google, rust, "test_1a_missing_property_asc")


def test_1b_missing_property_desc(rust_client, google_client):
    rust, google = _both(rust_client, google_client, "OM1b", _setup_three, _ordered(["-score"]))
    assert_parity(google, rust, "test_1b_missing_property_desc")


def test_2_excluded_from_indexes(rust_client, google_client):
    def setup(client, kind):
        put_entity(client, kind, "a", {"score": 2})
        put_entity(client, kind, "b", {"score": 1}, exclude=("score",))

    rust, google = _both(rust_client, google_client, "OM2", setup, _ordered(["score"]))
    assert_parity(google, rust, "test_2_excluded_from_indexes")


def test_3_null_vs_missing(rust_client, google_client):
    def setup(client, kind):
        put_entity(client, kind, "a", {"score": 2})
        put_entity(client, kind, "b", {"score": None})
        put_entity(client, kind, "c", {"other": 1})

    rust, google = _both(rust_client, google_client, "OM3", setup, _ordered(["score"]))
    assert_parity(google, rust, "test_3_null_vs_missing")


def test_4_two_sort_keys_second_missing(rust_client, google_client):
    def setup(client, kind):
        put_entity(client, kind, "a", {"cat": "x", "score": 2})
        put_entity(client, kind, "b", {"cat": "x"})
        put_entity(client, kind, "c", {"cat": "y", "score": 1})

    rust, google = _both(rust_client, google_client, "OM4", setup, _ordered(["cat", "score"]))
    assert_parity(google, rust, "test_4_two_sort_keys_second_missing")


def test_5_equality_filter_plus_order_missing(rust_client, google_client):
    def setup(client, kind):
        put_entity(client, kind, "a", {"cat": "x", "score": 2})
        put_entity(client, kind, "b", {"cat": "x"})

    rust, google = _both(
        rust_client, google_client, "OM5", setup,
        _ordered(["score"], filters=[("cat", "=", "x")]),
    )
    assert_parity(google, rust, "test_5_equality_filter_plus_order_missing")


def test_6_keys_only_order_missing(rust_client, google_client):
    def setup(client, kind):
        put_entity(client, kind, "a", {"score": 2})
        put_entity(client, kind, "b", {"other": 1})

    rust, google = _both(rust_client, google_client, "OM6", setup, _ordered(["score"], keys_only=True))
    assert_parity(google, rust, "test_6_keys_only_order_missing")


def _setup_tags(client, kind):
    put_entity(client, kind, "a", {"tags": ["b", "z"]})
    put_entity(client, kind, "b", {"tags": ["a", "y"]})
    put_entity(client, kind, "c", {"tags": ["c"]})


def test_7a_array_order_asc(rust_client, google_client):
    rust, google = _both(rust_client, google_client, "OM7a", _setup_tags, _ordered(["tags"]))
    assert_parity(google, rust, "test_7a_array_order_asc")


def test_7b_array_order_desc(rust_client, google_client):
    rust, google = _both(rust_client, google_client, "OM7b", _setup_tags, _ordered(["-tags"]))
    assert_parity(google, rust, "test_7b_array_order_desc")


def _key_order(order):
    """Mixed id/name keys; ids normalized to their rank so both emulators compare."""

    def q(client, kind):
        query = client.query(kind=kind)
        query.order = order
        keys = [e.key for e in query.fetch()]
        ids = sorted(k.id for k in keys if k.id is not None)
        return [k.name if k.name is not None else f"id#{ids.index(k.id)}" for k in keys]

    return q


def _setup_mixed_keys(client, kind):
    for i in range(2):
        e = datastore.Entity(key=client.key(kind))
        e["n"] = i
        client.put(e)
    put_entity(client, kind, "alpha", {"n": 2})
    put_entity(client, kind, "beta", {"n": 3})


def test_8a_key_order_asc_mixed(rust_client, google_client):
    rust, google = _both(rust_client, google_client, "OM8a", _setup_mixed_keys, _key_order(["__key__"]))
    assert_parity(google, rust, "test_8a_key_order_asc_mixed")


def test_8b_key_order_desc_mixed(rust_client, google_client):
    rust, google = _both(rust_client, google_client, "OM8b", _setup_mixed_keys, _key_order(["-__key__"]))
    assert_parity(google, rust, "test_8b_key_order_desc_mixed")


def test_9_mixed_types_asc(rust_client, google_client):
    def setup(client, kind):
        put_entity(client, kind, "int", {"score": 1})
        put_entity(client, kind, "float", {"score": 1.5})
        put_entity(client, kind, "str", {"score": "a"})
        put_entity(client, kind, "bool", {"score": True})
        put_entity(client, kind, "null", {"score": None})

    rust, google = _both(rust_client, google_client, "OM9", setup, _ordered(["score"]))
    assert_parity(google, rust, "test_9_mixed_types_asc")


def test_10_limit_one_missing_lowest(rust_client, google_client):
    def setup(client, kind):
        put_entity(client, kind, "a", {"score": 5})
        put_entity(client, kind, "b", {"other": 1})

    rust, google = _both(rust_client, google_client, "OM10", setup, _ordered(["score"], limit=1))
    assert google == ("ok", ["a"])
    assert_parity(google, rust, "test_10_limit_one_missing_lowest")
