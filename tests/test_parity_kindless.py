"""Parity tests for kindless queries (no kind specified).

Each test runs the same scenario on the Rust emulator (8042) and the official
Google emulator (8044) and asserts identical outcomes.

    poetry run pytest tests/test_parity_kindless.py -v
"""

import uuid
import pytest
from google.cloud import datastore
from google.cloud.datastore.query import PropertyFilter
from parity_common import outcome, assert_parity, rust_client, google_client  # noqa: F401



def _norm(entities):
    return [(e.key.kind, e.key.id_or_name) for e in entities]


def _setup(client, tag):
    """Insert P + children of KindA/KindB + a grandchild in a dedicated namespace."""
    ns = f"kindless-{tag}"
    parent = client.key("KParent", f"p-{tag}", namespace=ns)
    keys = [
        parent,
        client.key("KindA", "a1", parent=parent),
        client.key("KindA", "a2", parent=parent),
        client.key("KindB", "b1", parent=parent),
        client.key("KindB", "b2", parent=parent),
        client.key("KindA", "a1", "KindC", "c1", parent=parent),
    ]
    entities = []
    for i, k in enumerate(keys):
        e = datastore.Entity(key=k)
        e["flag"] = i % 2 == 0
        e["n"] = i
        entities.append(e)
    client.put_multi(entities)
    return ns, parent, keys


def _cleanup(client, keys):
    client.delete_multi(keys)


def run_both(rust_client, google_client, scenario):
    tag = uuid.uuid4().hex[:10]
    results = {}
    for name, c in (("rust", rust_client), ("google", google_client)):
        ns, parent, keys = _setup(c, tag)
        try:
            results[name] = outcome(lambda: scenario(c, ns, parent))
        finally:
            _cleanup(c, keys)
    return results["rust"], results["google"]


def test_kindless_ancestor(rust_client, google_client):
    def s(c, ns, p):
        return _norm(c.query(ancestor=p, namespace=ns).fetch())

    rust, google = run_both(rust_client, google_client, s)
    assert_parity(google, rust, "test_kindless_ancestor")


def test_kindless_ancestor_keys_only(rust_client, google_client):
    def s(c, ns, p):
        q = c.query(ancestor=p, namespace=ns)
        q.keys_only()
        return _norm(q.fetch())

    rust, google = run_both(rust_client, google_client, s)
    assert_parity(google, rust, "test_kindless_ancestor_keys_only")


@pytest.mark.parametrize("order", ["__key__", "-__key__"])
def test_kindless_order_by_key(rust_client, google_client, order):
    def s(c, ns, p):
        q = c.query(ancestor=p, namespace=ns)
        q.order = [order]
        return _norm(q.fetch())

    rust, google = run_both(rust_client, google_client, s)
    assert_parity(google, rust, "test_kindless_order_by_key")


def test_kindless_key_range_filter(rust_client, google_client):
    def s(c, ns, p):
        q = c.query(ancestor=p, namespace=ns)
        lo = c.key("KindA", "a1", parent=p)
        hi = c.key("KindB", "b2", parent=p)
        q.add_filter(filter=PropertyFilter("__key__", ">", lo))
        q.add_filter(filter=PropertyFilter("__key__", "<", hi))
        return _norm(q.fetch())

    rust, google = run_both(rust_client, google_client, s)
    assert_parity(google, rust, "test_kindless_key_range_filter")


def test_kindless_cursor_paging(rust_client, google_client):
    def s(c, ns, p):
        out, cursor, pages = [], None, 0
        while pages < 10:
            it = c.query(ancestor=p, namespace=ns).fetch(limit=2, start_cursor=cursor)
            page = list(next(it.pages))
            out.append(_norm(page))
            pages += 1
            cursor = it.next_page_token
            if not page or cursor is None:
                break
        return [x for x in out if x]

    rust, google = run_both(rust_client, google_client, s)
    assert_parity(google, rust, "test_kindless_cursor_paging")


def test_kindless_aggregation_count(rust_client, google_client):
    def s(c, ns, p):
        agg = c.aggregation_query(c.query(ancestor=p, namespace=ns)).count(alias="total")
        return [[(r.alias, r.value) for r in batch] for batch in agg.fetch()]

    rust, google = run_both(rust_client, google_client, s)
    assert_parity(google, rust, "test_kindless_aggregation_count")


def test_kindless_property_filter(rust_client, google_client):
    def s(c, ns, p):
        q = c.query(ancestor=p, namespace=ns)
        q.add_filter(filter=PropertyFilter("flag", "=", True))
        return _norm(q.fetch())

    rust, google = run_both(rust_client, google_client, s)
    assert_parity(google, rust, "test_kindless_property_filter")


def test_kindless_namespace_no_ancestor(rust_client, google_client):
    def s(c, ns, p):
        return _norm(c.query(namespace=ns).fetch())

    rust, google = run_both(rust_client, google_client, s)
    assert_parity(google, rust, "test_kindless_namespace_no_ancestor")


def test_control_ancestor_with_kind(rust_client, google_client):
    def s(c, ns, p):
        return _norm(c.query(kind="KindA", ancestor=p, namespace=ns).fetch())

    rust, google = run_both(rust_client, google_client, s)
    assert_parity(google, rust, "test_control_ancestor_with_kind")
