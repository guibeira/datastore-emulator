"""Parity tests for projection queries (index-scan semantics).

Run against both emulators (Rust 8042, Google 8044):

    poetry run pytest tests/test_parity_projection.py -v
"""

from google.cloud import datastore
from parity_common import (  # noqa: F401
    outcome,
    cleanup_kind,
    unique_kind,
    put_entity,
    assert_parity,
    rust_client,
    google_client,
)



def _plain(value):
    if isinstance(value, datastore.Entity):
        return {k: _plain(v) for k, v in sorted(value.items())}
    if isinstance(value, list):
        return [_plain(v) for v in value]
    return value


def _rows(results):
    return [(e.key.name, _plain(e)) for e in results]


def _run_both(rust_client, google_client, prefix, setup, run):
    """Run `setup(client, kind)` then `run(client, kind)` on both emulators."""
    out = {}
    for label, client in (("rust", rust_client), ("google", google_client)):
        kind = unique_kind(prefix)
        try:
            setup(client, kind)
            out[label] = outcome(lambda: run(client, kind))
        finally:
            cleanup_kind(client, kind)
    return out["rust"], out["google"]


def _projection(projection, order=None, distinct_on=None):
    def run(client, kind):
        q = client.query(kind=kind)
        q.projection = projection
        if order:
            q.order = order
        if distinct_on:
            q.distinct_on = distinct_on
        return _rows(q.fetch())

    return run


def test_projection_array_one_result_per_value(rust_client, google_client):
    def setup(c, k):
        put_entity(c, k, "e1", {"tags": ["b", "a", "c"], "name": "x"})

    rust, google = _run_both(rust_client, google_client, "ProjArr", setup, _projection(["tags"]))
    assert_parity(google, rust, "test_projection_array_one_result_per_value")


def test_projection_skips_entities_missing_property(rust_client, google_client):
    def setup(c, k):
        put_entity(c, k, "has", {"score": 1})
        put_entity(c, k, "lacks", {"other": 2})

    rust, google = _run_both(rust_client, google_client, "ProjMissing", setup, _projection(["score"]))
    assert_parity(google, rust, "test_projection_skips_entities_missing_property")


def test_projection_skips_unindexed_property(rust_client, google_client):
    def setup(c, k):
        put_entity(c, k, "unindexed", {"score": 7}, exclude=("score",))

    rust, google = _run_both(rust_client, google_client, "ProjUnidx", setup, _projection(["score"]))
    assert_parity(google, rust, "test_projection_skips_unindexed_property")


def test_projection_two_arrays_cross_product(rust_client, google_client):
    def setup(c, k):
        put_entity(c, k, "e1", {"tags": ["a", "b"], "nums": [1, 2]})

    rust, google = _run_both(
        rust_client, google_client, "ProjCross", setup, _projection(["tags", "nums"])
    )
    assert_parity(google, rust, "test_projection_two_arrays_cross_product")


def test_projection_array_order_asc(rust_client, google_client):
    def setup(c, k):
        put_entity(c, k, "e1", {"tags": ["b", "d"]})
        put_entity(c, k, "e2", {"tags": ["a", "c"]})

    rust, google = _run_both(
        rust_client, google_client, "ProjOrdAsc", setup, _projection(["tags"], order=["tags"])
    )
    assert_parity(google, rust, "test_projection_array_order_asc")


def test_projection_array_order_desc(rust_client, google_client):
    def setup(c, k):
        put_entity(c, k, "e1", {"tags": ["b", "d"]})
        put_entity(c, k, "e2", {"tags": ["a", "c"]})

    rust, google = _run_both(
        rust_client, google_client, "ProjOrdDesc", setup, _projection(["tags"], order=["-tags"])
    )
    assert_parity(google, rust, "test_projection_array_order_desc")


def test_projection_embedded_entity_path(rust_client, google_client):
    def setup(c, k):
        entity = datastore.Entity(key=c.key(k, "e1"))
        address = datastore.Entity()
        address["city"] = "Lisbon"
        entity["address"] = address
        c.put(entity)

    rust, google = _run_both(
        rust_client, google_client, "ProjEmbed", setup, _projection(["address.city"])
    )
    assert_parity(google, rust, "test_projection_embedded_entity_path")


def test_distinct_on_array_values(rust_client, google_client):
    def setup(c, k):
        put_entity(c, k, "e1", {"tags": ["a", "b"]})
        put_entity(c, k, "e2", {"tags": ["b", "c"]})
        put_entity(c, k, "e3", {"tags": ["c"]})

    rust, google = _run_both(
        rust_client,
        google_client,
        "ProjDistinct",
        setup,
        _projection(["tags"], distinct_on=["tags"]),
    )
    assert_parity(google, rust, "test_distinct_on_array_values")


def test_keys_only_with_array_property(rust_client, google_client):
    def setup(c, k):
        put_entity(c, k, "e1", {"tags": ["a", "b", "c"]})

    def run(c, k):
        q = c.query(kind=k)
        q.keys_only()
        return _rows(q.fetch())

    rust, google = _run_both(rust_client, google_client, "ProjKeys", setup, run)
    assert_parity(google, rust, "test_keys_only_with_array_property")


def test_projection_null_value(rust_client, google_client):
    def setup(c, k):
        put_entity(c, k, "null", {"score": None})
        put_entity(c, k, "five", {"score": 5})

    rust, google = _run_both(rust_client, google_client, "ProjNull", setup, _projection(["score"]))
    assert_parity(google, rust, "test_projection_null_value")
