"""Parity tests for query limit/offset batch metadata (more_results, cursors).

Runs each scenario on the Rust emulator (8042) and the Google emulator (8044)
and asserts identical outcomes.

    poetry run pytest tests/test_parity_limit.py -v -s
"""

import sys
import pytest
from google.cloud import datastore
from google.cloud.datastore_v1 import types
from google.protobuf import wrappers_pb2
from parity_common import (  # noqa: F401
    outcome,
    cleanup_kind,
    unique_kind,
    assert_parity,
    run_query_raw,
    rust_client,
    google_client,
)



def _insert(client, kind, count=5):
    entities = []
    for i in range(count):
        e = datastore.Entity(key=client.key(kind, f"e{i}"))
        e["n"] = i
        e["tag"] = "x"
        entities.append(e)
    client.put_multi(entities)


def _run(client, kind, limit=None, offset=0, start_cursor=b"", filt=False, order=False):
    q = types.Query(kind=[types.KindExpression(name=kind)], offset=offset)
    if limit is not None:
        q._pb.limit.CopyFrom(wrappers_pb2.Int32Value(value=limit))
    if start_cursor:
        q.start_cursor = start_cursor
    if filt:
        q.filter = types.Filter(
            property_filter=types.PropertyFilter(
                property=types.PropertyReference(name="tag"),
                op=types.PropertyFilter.Operator.EQUAL,
                value=types.Value(string_value="x"),
            )
        )
    if order:
        q.order = [
            types.PropertyOrder(
                property=types.PropertyReference(name="n"),
                direction=types.PropertyOrder.Direction.ASCENDING,
            )
        ]
    return run_query_raw(client, q).batch


def _summary(batch):
    return {
        "len": len(batch.entity_results),
        "more_results": types.QueryResultBatch.MoreResultsType(batch.more_results).name,
        "skipped": batch.skipped_results,
        "end_cursor": bool(batch.end_cursor),
        "skipped_cursor": bool(batch.skipped_cursor),
        "result_type": types.EntityResult.ResultType(batch.entity_result_type).name,
    }


def _compare(rust_client, google_client, scenario):
    results = {}
    for name, client in (("rust", rust_client), ("google", google_client)):
        kind = unique_kind("Limit", 10)
        try:
            _insert(client, kind)
            results[name] = outcome(lambda: scenario(client, kind))
        finally:
            cleanup_kind(client, kind)
    assert_parity(results["google"], results["rust"], sys._getframe(1).f_code.co_name)


# 1
def test_raw_limit_zero(rust_client, google_client):
    _compare(rust_client, google_client, lambda c, k: _summary(_run(c, k, limit=0)))


def test_highlevel_fetch_limit_zero(rust_client, google_client):
    _compare(
        rust_client, google_client,
        lambda c, k: len(list(c.query(kind=k).fetch(limit=0))),
    )


# 2
def test_raw_limit_equals_count_then_cursor(rust_client, google_client):
    def scenario(c, k):
        b1 = _run(c, k, limit=5)
        b2 = _run(c, k, limit=5, start_cursor=b1.end_cursor)
        return {"first": _summary(b1), "second": _summary(b2)}

    _compare(rust_client, google_client, scenario)


# 3
def test_raw_limit_above_count(rust_client, google_client):
    _compare(rust_client, google_client, lambda c, k: _summary(_run(c, k, limit=10)))


# 4
def test_raw_limit_below_count(rust_client, google_client):
    _compare(rust_client, google_client, lambda c, k: _summary(_run(c, k, limit=3)))


# 5
def test_raw_offset_equals_count(rust_client, google_client):
    _compare(rust_client, google_client, lambda c, k: _summary(_run(c, k, offset=5)))


# 6
def test_raw_offset_above_count(rust_client, google_client):
    _compare(rust_client, google_client, lambda c, k: _summary(_run(c, k, offset=7)))


# 7
def test_raw_offset_and_limit_exhaust(rust_client, google_client):
    _compare(
        rust_client, google_client,
        lambda c, k: _summary(_run(c, k, offset=2, limit=3)),
    )


# 8
@pytest.mark.parametrize("filt,order", [(True, False), (False, True), (True, True)])
def test_raw_limit_equals_count_filter_order(rust_client, google_client, filt, order):
    def scenario(c, k):
        b1 = _run(c, k, limit=5, filt=filt, order=order)
        b2 = _run(c, k, limit=5, start_cursor=b1.end_cursor, filt=filt, order=order)
        return {"first": _summary(b1), "second": _summary(b2)}

    _compare(rust_client, google_client, scenario)


# 9
def test_highlevel_next_page_token_limit_equals_count(rust_client, google_client):
    def scenario(c, k):
        it = c.query(kind=k).fetch(limit=5)
        first = len(list(it))
        token = it.next_page_token
        second = None
        if token is not None:
            second = len(list(c.query(kind=k).fetch(limit=5, start_cursor=token)))
        return {"first": first, "token_is_none": token is None, "second": second}

    _compare(rust_client, google_client, scenario)


# 10
def test_highlevel_pages_count(rust_client, google_client):
    def scenario(c, k):
        p5 = [len(list(p)) for p in c.query(kind=k).fetch(limit=5).pages]
        p2 = [len(list(p)) for p in c.query(kind=k).fetch(limit=2).pages]
        pall = [len(list(p)) for p in c.query(kind=k).fetch().pages]
        return {"limit5": p5, "limit2": p2, "nolimit": pall}

    _compare(rust_client, google_client, scenario)


# 11
def test_raw_start_cursor_at_end(rust_client, google_client):
    def scenario(c, k):
        b1 = _run(c, k)
        b2 = _run(c, k, start_cursor=b1.end_cursor)
        s = _summary(b2)
        s["end_cursor_equals_input"] = b2.end_cursor == b1.end_cursor
        s["first_more_results"] = _summary(b1)["more_results"]
        return s

    _compare(rust_client, google_client, scenario)
