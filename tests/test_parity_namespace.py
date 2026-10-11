"""Parity tests: namespace (partition_id) scoping of queries.

Runs each scenario on the Rust emulator (8042) and the Google emulator (8044)
and asserts identical outcomes.

    poetry run pytest tests/test_parity_namespace.py -v
"""

import sys
import uuid
import pytest
from google.cloud import datastore
from google.cloud.datastore.query import PropertyFilter
from parity_common import outcome, assert_parity, rust_client, google_client  # noqa: F401



@pytest.fixture
def ids():
    u = uuid.uuid4().hex[:8]
    return {"kind": f"Task_{u}", "ns1": f"ns1-{u}", "ns2": f"ns2-{u}", "u": u}


def _put(client, kind, name, namespace, props, parent=None):
    if parent is not None:
        key = client.key(kind, name, parent=parent)
    else:
        key = client.key(kind, name, namespace=namespace)
    e = datastore.Entity(key=key)
    e.update(props)
    client.put(e)


def _cleanup(client, kinds, namespaces):
    for ns in namespaces:
        for kind in kinds:
            keys = [e.key for e in client.query(kind=kind, namespace=ns).fetch()]
            if keys:
                client.delete_multi(keys)


def _norm(entities):
    return sorted((e.key.namespace or "", e.key.name) for e in entities)


def _seed_three(client, ids):
    k = ids["kind"]
    _put(client, k, "a", None, {"p": 1, "tag": "x"})
    _put(client, k, "b", ids["ns1"], {"p": 2, "tag": "x"})
    _put(client, k, "b2", ids["ns1"], {"p": 3, "tag": "y"})
    _put(client, k, "c", ids["ns2"], {"p": 1, "tag": "x"})


def _both(rust_client, google_client, ids, kinds, seed, scenario):
    results = {}
    for label, c in (("rust", rust_client), ("google", google_client)):
        try:
            seed(c)
            results[label] = scenario(c)
        finally:
            _cleanup(c, kinds, [None, ids["ns1"], ids["ns2"]])
    assert_parity(results["google"], results["rust"], sys._getframe(1).f_code.co_name)
    return results


def test_kind_query_scoped_to_namespace(rust_client, google_client, ids):
    k = ids["kind"]

    def scenario(c):
        return {
            "ns1": outcome(lambda: _norm(c.query(kind=k, namespace=ids["ns1"]).fetch())),
            "default": outcome(lambda: _norm(c.query(kind=k, namespace=None).fetch())),
        }

    _both(rust_client, google_client, ids, [k], lambda c: _seed_three(c, ids), scenario)


def test_filter_and_keys_only_in_namespace(rust_client, google_client, ids):
    k = ids["kind"]

    def scenario(c):
        def filt():
            q = c.query(kind=k, namespace=ids["ns1"])
            q.add_filter(filter=PropertyFilter("tag", "=", "x"))
            return _norm(q.fetch())

        def keys_only():
            q = c.query(kind=k, namespace=ids["ns1"])
            q.keys_only()
            return _norm(q.fetch())

        def filt_keys_only():
            q = c.query(kind=k, namespace=ids["ns1"])
            q.add_filter(filter=PropertyFilter("p", "=", 1))
            q.keys_only()
            return _norm(q.fetch())

        return {"filter": outcome(filt), "keys_only": outcome(keys_only),
                "filter_keys_only": outcome(filt_keys_only)}

    _both(rust_client, google_client, ids, [k], lambda c: _seed_three(c, ids), scenario)


def test_order_and_limit_in_namespace(rust_client, google_client, ids):
    k = ids["kind"]

    def scenario(c):
        def ordered():
            q = c.query(kind=k, namespace=ids["ns1"])
            q.order = ["p"]
            return [(e.key.namespace, e.key.name) for e in q.fetch()]

        def limited():
            q = c.query(kind=k, namespace=ids["ns1"])
            q.order = ["p"]
            return [(e.key.namespace, e.key.name) for e in q.fetch(limit=1)]

        return {"order": outcome(ordered), "limit1": outcome(limited)}

    _both(rust_client, google_client, ids, [k], lambda c: _seed_three(c, ids), scenario)


def test_ancestor_query_in_namespace(rust_client, google_client, ids):
    k, parent_kind = ids["kind"], f"Parent_{ids['u']}"

    def seed(c):
        p_ns1 = c.key(parent_kind, "p", namespace=ids["ns1"])
        p_def = c.key(parent_kind, "p")
        _put(c, k, "child_ns1_a", None, {"v": 1}, parent=p_ns1)
        _put(c, k, "child_ns1_b", None, {"v": 2}, parent=p_ns1)
        _put(c, k, "child_def", None, {"v": 3}, parent=p_def)

    def scenario(c):
        anc = c.key(parent_kind, "p", namespace=ids["ns1"])
        return {
            "kind_ancestor": outcome(lambda: _norm(
                c.query(kind=k, namespace=ids["ns1"], ancestor=anc).fetch())),
            "kindless_ancestor": outcome(lambda: _norm(
                c.query(namespace=ids["ns1"], ancestor=anc).fetch())),
        }

    _both(rust_client, google_client, ids, [k], seed, scenario)


def test_kind_metadata_in_namespace(rust_client, google_client, ids):
    kx, ky = f"X_{ids['u']}", f"Y_{ids['u']}"
    mine = {kx, ky}

    def seed(c):
        _put(c, kx, "x1", None, {"a": 1})
        _put(c, ky, "y1", ids["ns1"], {"b": 1})

    def kinds(c, ns):
        q = c.query(kind="__kind__", namespace=ns)
        q.keys_only()
        return sorted(e.key.name for e in q.fetch() if e.key.name in mine)

    def scenario(c):
        return {"ns1": outcome(lambda: kinds(c, ids["ns1"])),
                "default": outcome(lambda: kinds(c, None))}

    _both(rust_client, google_client, ids, [kx, ky], seed, scenario)


def test_property_metadata_in_namespace(rust_client, google_client, ids):
    k = ids["kind"]

    def seed(c):
        _put(c, k, "d", None, {"only_default": 1, "shared": 1})
        _put(c, k, "n", ids["ns1"], {"only_ns1": "s", "shared": 2})

    def props(c, ns):
        anc = c.key("__kind__", k, namespace=ns)
        q = c.query(kind="__property__", namespace=ns, ancestor=anc)
        q.keys_only()
        return sorted(e.key.name for e in q.fetch())

    def props_with_repr(c, ns):
        anc = c.key("__kind__", k, namespace=ns)
        q = c.query(kind="__property__", namespace=ns, ancestor=anc)
        return sorted((e.key.name, tuple(sorted(e.get("property_representation", []))))
                      for e in q.fetch())

    def scenario(c):
        return {"ns1": outcome(lambda: props(c, ids["ns1"])),
                "default": outcome(lambda: props(c, None)),
                "ns1_repr": outcome(lambda: props_with_repr(c, ids["ns1"]))}

    _both(rust_client, google_client, ids, [k], seed, scenario)


def test_count_aggregation_in_namespace(rust_client, google_client, ids):
    """Google's emulator does not implement RunAggregationQuery (UNIMPLEMENTED),
    so the reference count is the length of the equivalent plain query on Google."""
    k = ids["kind"]
    nss = {"ns1": ids["ns1"], "ns2": ids["ns2"], "default": None}

    def count(c, ns):
        q = c.query(kind=k, namespace=ns)
        res = list(c.aggregation_query(q).count(alias="n").fetch())
        return [r.value for batch in res for r in batch]

    def rust_scenario(c):
        return {label: outcome(lambda ns=ns: count(c, ns)) for label, ns in nss.items()}

    def google_scenario(c):
        out = {}
        for label, ns in nss.items():
            agg = outcome(lambda ns=ns: count(c, ns))
            if agg[0] == "err" and agg[1] == "UNIMPLEMENTED":
                agg = outcome(lambda ns=ns: [len(list(c.query(kind=k, namespace=ns).fetch()))])
            out[label] = agg
        return out

    results = {}
    for label, c, sc in (("rust", rust_client, rust_scenario),
                         ("google", google_client, google_scenario)):
        try:
            _seed_three(c, ids)
            results[label] = sc(c)
        finally:
            _cleanup(c, [k], [None, ids["ns1"], ids["ns2"]])
    assert_parity(results["google"], results["rust"], "test_count_aggregation_in_namespace")


def test_namespace_metadata_control(rust_client, google_client, ids):
    k = ids["kind"]
    mine = {ids["ns1"], ids["ns2"]}

    def scenario(c):
        def nss():
            q = c.query(kind="__namespace__")
            q.keys_only()
            return sorted(e.key.name for e in q.fetch() if e.key.name in mine)
        return {"ns": outcome(nss)}

    _both(rust_client, google_client, ids, [k], lambda c: _seed_three(c, ids), scenario)
