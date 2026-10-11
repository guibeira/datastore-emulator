"""Parity tests for RunAggregationQuery (COUNT/SUM/AVG) between the Rust
emulator (8042) and the official Google emulator (8044).

    poetry run pytest tests/test_parity_aggregation.py -v
"""

from google.cloud import datastore
from google.cloud.datastore.query import PropertyFilter
from parity_common import (  # noqa: F401
    outcome,
    cleanup_kind,
    unique_kind,
    assert_parity,
    run_aggregation_raw,
    rust_client,
    google_client,
)



def _norm_value_pb(v):
    """Normalize a raw datastore_v1 Value proto to (type_name, python_value)."""
    which = v._pb.WhichOneof("value_type")
    if which == "integer_value":
        return ("int", v.integer_value)
    if which == "double_value":
        return ("float", v.double_value)
    if which == "null_value":
        return ("NoneType", None)
    return (which, str(v))


def _run_raw(client, agg_query, namespace=None, mutate=None):
    """Run aggregation via the raw GAPIC API. Returns list (ordered by alias)
    of (alias, (type, value)) per result row. `mutate(pb)` may edit the
    AggregationQuery proto (e.g. nested_query.offset)."""
    pb = agg_query.fetch()._build_protobuf()
    if mutate is not None:
        mutate(pb)
    resp = run_aggregation_raw(client, pb, namespace)
    rows = []
    for r in resp.batch.aggregation_results:
        rows.append(
            sorted((alias, _norm_value_pb(v)) for alias, v in r.aggregate_properties.items())
        )
    return rows


def _high_level(agg_query, **fetch_kwargs):
    rows = list(agg_query.fetch(**fetch_kwargs))
    return [sorted((a.alias, (type(a.value).__name__, a.value)) for a in row) for row in rows]


def _put(client, kind, rows, parent=None, namespace=None):
    ents = []
    for i, props in enumerate(rows):
        if parent is not None:
            key = client.key(*parent.flat_path, kind, f"e{i}", namespace=namespace)
        else:
            key = client.key(kind, f"e{i}", namespace=namespace)
        e = datastore.Entity(key=key)
        e.update(props)
        ents.append(e)
    client.put_multi(ents)


def _both(rust_client, google_client, setup, run, cleanup):
    results = {}
    for name, c in (("rust", rust_client), ("google", google_client)):
        try:
            setup(c)
            results[name] = outcome(lambda: run(c))
        finally:
            cleanup(c)
    return results["rust"], results["google"]


# 1. count with fetch(limit=3) vs no limit
def test_count_with_limit(rust_client, google_client):
    kind = unique_kind("Agg", 32)

    def run(c):
        q = c.query(kind=kind)
        limited = _high_level(c.aggregation_query(q).count(alias="c"), limit=3)
        q2 = c.query(kind=kind)
        full = _high_level(c.aggregation_query(q2).count(alias="c"))
        return {"limited": limited, "full": full}

    rust, google = _both(
        rust_client, google_client,
        lambda c: _put(c, kind, [{"n": i} for i in range(10)]),
        run, lambda c: cleanup_kind(c, kind),
    )
    assert_parity(google, rust, "test_count_with_limit")


# 2. integer sum/avg
def test_sum_avg_integers(rust_client, google_client):
    kind = unique_kind("Agg", 32)

    def run(c):
        return _run_raw(c, c.aggregation_query(c.query(kind=kind)).sum("hours", alias="s").avg("hours", alias="a"))

    rust, google = _both(
        rust_client, google_client,
        lambda c: _put(c, kind, [{"hours": h} for h in (5, 3, 1)]),
        run, lambda c: cleanup_kind(c, kind),
    )
    assert_parity(google, rust, "test_sum_avg_integers")


# 3. mixed int/float
def test_sum_avg_mixed(rust_client, google_client):
    kind = unique_kind("Agg", 32)

    def run(c):
        return _run_raw(c, c.aggregation_query(c.query(kind=kind)).sum("hours", alias="s").avg("hours", alias="a"))

    rust, google = _both(
        rust_client, google_client,
        lambda c: _put(c, kind, [{"hours": 5}, {"hours": 2.5}]),
        run, lambda c: cleanup_kind(c, kind),
    )
    assert_parity(google, rust, "test_sum_avg_mixed")


# 4. empty result set
def test_aggregations_on_empty(rust_client, google_client):
    kind = unique_kind("Agg", 32)

    def run(c):
        q = c.query(kind=kind)
        q.add_filter(filter=PropertyFilter("category", "=", "nope"))
        return _run_raw(
            c,
            c.aggregation_query(q).count(alias="c").sum("hours", alias="s").avg("hours", alias="a"),
        )

    rust, google = _both(
        rust_client, google_client,
        lambda c: _put(c, kind, [{"category": "x", "hours": 1}]),
        run, lambda c: cleanup_kind(c, kind),
    )
    assert_parity(google, rust, "test_aggregations_on_empty")


# 5. default alias names
def test_default_alias_names(rust_client, google_client):
    kind = unique_kind("Agg", 32)

    def run(c):
        return _run_raw(c, c.aggregation_query(c.query(kind=kind)).count().sum("hours").avg("hours"))

    rust, google = _both(
        rust_client, google_client,
        lambda c: _put(c, kind, [{"hours": h} for h in (5, 3, 1)]),
        run, lambda c: cleanup_kind(c, kind),
    )
    assert_parity(google, rust, "test_default_alias_names")


# 6. string value + missing property
def test_sum_avg_skips_non_numeric_and_missing(rust_client, google_client):
    kind = unique_kind("Agg", 32)

    def run(c):
        return _run_raw(c, c.aggregation_query(c.query(kind=kind)).sum("hours", alias="s").avg("hours", alias="a"))

    rust, google = _both(
        rust_client, google_client,
        lambda c: _put(c, kind, [{"hours": 4}, {"hours": 2}, {"hours": "ten"}, {"other": 1}]),
        run, lambda c: cleanup_kind(c, kind),
    )
    assert_parity(google, rust, "test_sum_avg_skips_non_numeric_and_missing")


# 7. count on ancestor query
def test_count_ancestor(rust_client, google_client):
    kind = unique_kind("Agg", 32)

    def setup(c):
        parent = c.key("Parent" + kind, "p1")
        _put(c, kind, [{"n": i} for i in range(3)], parent=parent)
        other = c.key("Parent" + kind, "p2")
        e = datastore.Entity(key=c.key(*other.flat_path, kind, "x"))
        e["n"] = 99
        c.put(e)
        c.put(datastore.Entity(key=c.key(kind, "root")))

    def run(c):
        q = c.query(kind=kind, ancestor=c.key("Parent" + kind, "p1"))
        return _run_raw(c, c.aggregation_query(q).count(alias="c"))

    rust, google = _both(rust_client, google_client, setup, run, lambda c: cleanup_kind(c, kind))
    assert_parity(google, rust, "test_count_ancestor")


# 8. count in namespace
def test_count_namespace(rust_client, google_client):
    kind = unique_kind("Agg", 32)
    ns = "ns-agg"

    def setup(c):
        _put(c, kind, [{"n": i} for i in range(2)], namespace=ns)
        _put(c, kind, [{"n": i} for i in range(5)])

    def run(c):
        q = c.query(kind=kind, namespace=ns)
        return _run_raw(c, c.aggregation_query(q).count(alias="c"), namespace=ns)

    def cleanup(c):
        cleanup_kind(c, kind)
        cleanup_kind(c, kind, namespace=ns)

    rust, google = _both(rust_client, google_client, setup, run, cleanup)
    assert_parity(google, rust, "test_count_namespace")


# 9. sum over array property
def test_sum_array_property(rust_client, google_client):
    kind = unique_kind("Agg", 32)

    # COUNT is kept in its own aggregation query: when combined with SUM/AVG on
    # an array property, real Datastore counts index entries (3 here), not
    # entities. That is a backend plan detail the emulator does not reproduce.
    def run(c):
        sums = _run_raw(c, c.aggregation_query(c.query(kind=kind)).sum("hours", alias="s").avg("hours", alias="a"))
        count = _run_raw(c, c.aggregation_query(c.query(kind=kind)).count(alias="c"))
        return (sums, count)

    rust, google = _both(
        rust_client, google_client,
        lambda c: _put(c, kind, [{"hours": [1, 2]}, {"hours": 3}]),
        run, lambda c: cleanup_kind(c, kind),
    )
    assert_parity(google, rust, "test_sum_array_property")


# 10a. count with limit and order
def test_count_with_limit_and_order(rust_client, google_client):
    kind = unique_kind("Agg", 32)

    def run(c):
        q = c.query(kind=kind)
        q.order = ["n"]
        return _high_level(c.aggregation_query(q).count(alias="c"), limit=3)

    rust, google = _both(
        rust_client, google_client,
        lambda c: _put(c, kind, [{"n": i} for i in range(10)]),
        run, lambda c: cleanup_kind(c, kind),
    )
    assert_parity(google, rust, "test_count_with_limit_and_order")


# 10b. count with nested offset
def test_count_with_offset(rust_client, google_client):
    kind = unique_kind("Agg", 32)

    def mutate(pb):
        pb.nested_query.offset = 2

    def run(c):
        return _run_raw(c, c.aggregation_query(c.query(kind=kind)).count(alias="c"), mutate=mutate)

    rust, google = _both(
        rust_client, google_client,
        lambda c: _put(c, kind, [{"n": i} for i in range(10)]),
        run, lambda c: cleanup_kind(c, kind),
    )
    assert_parity(google, rust, "test_count_with_offset")


# 10c. sum with limit (limit applies to the entities aggregated)
def test_sum_with_limit(rust_client, google_client):
    kind = unique_kind("Agg", 32)

    def run(c):
        q = c.query(kind=kind)
        q.order = ["hours"]
        return _high_level(c.aggregation_query(q).sum("hours", alias="s"), limit=2)

    rust, google = _both(
        rust_client, google_client,
        lambda c: _put(c, kind, [{"hours": h} for h in (1, 2, 3, 4)]),
        run, lambda c: cleanup_kind(c, kind),
    )
    assert_parity(google, rust, "test_sum_with_limit")


# 11. duplicate alias
def test_duplicate_alias(rust_client, google_client):
    kind = unique_kind("Agg", 32)

    def run(c):
        return _run_raw(c, c.aggregation_query(c.query(kind=kind)).count(alias="x").sum("hours", alias="x"))

    rust, google = _both(
        rust_client, google_client,
        lambda c: _put(c, kind, [{"hours": 1}]),
        run, lambda c: cleanup_kind(c, kind),
    )
    assert_parity(google, rust, "test_duplicate_alias")


# 12. one result row + read_time present
def test_single_row_and_read_time(rust_client, google_client):
    kind = unique_kind("Agg", 32)

    def run(c):
        pb = c.aggregation_query(c.query(kind=kind)).count(alias="c").fetch()._build_protobuf()
        resp = run_aggregation_raw(c, pb)
        return (len(resp.batch.aggregation_results), resp.batch._pb.HasField("read_time"),
                resp.batch.more_results.name)

    rust, google = _both(
        rust_client, google_client,
        lambda c: _put(c, kind, [{"n": 1}]),
        run, lambda c: cleanup_kind(c, kind),
    )
    assert_parity(google, rust, "test_single_row_and_read_time")
