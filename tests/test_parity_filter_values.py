"""Parity tests for filter value matching (numeric equality, arrays, cross-type).

Runs each scenario against the Rust emulator (8042) and the official Google
emulator (8044) and asserts identical outcomes.

    poetry run pytest tests/test_parity_filter_values.py -v
"""

import sys
from google.cloud import datastore
from google.cloud.datastore.query import And, Or, PropertyFilter
from parity_common import (  # noqa: F401
    outcome,
    cleanup_kind,
    unique_kind,
    assert_parity,
    rust_client,
    google_client,
)



NUMBERS = {
    "i1": {"score": 1},
    "d1": {"score": 1.0},
    "i2": {"score": 2},
    "s": {"score": "1"},
    "b": {"score": True},
    "none": {"score": None},
    "missing": {"other": 0},
}

TAGS = {
    "ab": {"tags": ["a", "b"]},
    "a": {"tags": ["a"]},
    "c": {"tags": ["c"]},
    "empty": {"tags": []},
    "missing": {"other": 0},
}

MIXED = {
    "s1": {"name": "abc"},
    "s2": {"name": "5"},
    "n": {"name": 3},
}


def _insert(client, kind, dataset):
    entities = []
    for name, props in dataset.items():
        e = datastore.Entity(key=client.key(kind, name))
        e.update(props)
        entities.append(e)
    client.put_multi(entities)


def _run(client, prefix, dataset, filt):
    kind = unique_kind(prefix)
    try:
        _insert(client, kind, dataset)
        q = client.query(kind=kind)
        q.add_filter(filter=filt)
        return outcome(lambda: sorted(e.key.name for e in q.fetch()))
    finally:
        cleanup_kind(client, kind)


def _compare(rust_client, google_client, prefix, dataset, filt):
    rust = _run(rust_client, prefix, dataset, filt)
    google = _run(google_client, prefix, dataset, filt)
    assert_parity(google, rust, sys._getframe(1).f_code.co_name)


def P(prop, op, value):
    return PropertyFilter(prop, op, value)


# --- Data set 1: mixed numeric / typed score -------------------------------


def test_01_eq_int(rust_client, google_client):
    _compare(rust_client, google_client, "N", NUMBERS, P("score", "=", 1))


def test_02_eq_double(rust_client, google_client):
    _compare(rust_client, google_client, "N", NUMBERS, P("score", "=", 1.0))


def test_03_gt_half(rust_client, google_client):
    _compare(rust_client, google_client, "N", NUMBERS, P("score", ">", 0.5))


def test_04_ge_int(rust_client, google_client):
    _compare(rust_client, google_client, "N", NUMBERS, P("score", ">=", 1))


def test_05_ge_double(rust_client, google_client):
    _compare(rust_client, google_client, "N", NUMBERS, P("score", ">=", 1.0))


def test_06_in_double_int(rust_client, google_client):
    _compare(rust_client, google_client, "N", NUMBERS, P("score", "IN", [1.0, 2]))


def test_07_in_int_string(rust_client, google_client):
    _compare(rust_client, google_client, "N", NUMBERS, P("score", "IN", [1, "1"]))


def test_08_ne_int(rust_client, google_client):
    _compare(rust_client, google_client, "N", NUMBERS, P("score", "!=", 1))


def test_09_ne_double(rust_client, google_client):
    _compare(rust_client, google_client, "N", NUMBERS, P("score", "!=", 1.0))


def test_10_not_in_ints(rust_client, google_client):
    _compare(rust_client, google_client, "N", NUMBERS, P("score", "NOT_IN", [1, 2]))


def test_11_eq_none(rust_client, google_client):
    _compare(rust_client, google_client, "N", NUMBERS, P("score", "=", None))


def test_12_ne_none(rust_client, google_client):
    _compare(rust_client, google_client, "N", NUMBERS, P("score", "!=", None))


def test_13_eq_true(rust_client, google_client):
    _compare(rust_client, google_client, "N", NUMBERS, P("score", "=", True))


def test_14_lt_two(rust_client, google_client):
    _compare(rust_client, google_client, "N", NUMBERS, P("score", "<", 2))


# --- Data set 2: array tags -------------------------------------------------


def test_15_tags_eq_a(rust_client, google_client):
    _compare(rust_client, google_client, "T", TAGS, P("tags", "=", "a"))


def test_16_tags_ne_a(rust_client, google_client):
    _compare(rust_client, google_client, "T", TAGS, P("tags", "!=", "a"))


def test_17_tags_not_in_a(rust_client, google_client):
    _compare(rust_client, google_client, "T", TAGS, P("tags", "NOT_IN", ["a"]))


def test_18_tags_not_in_ab(rust_client, google_client):
    _compare(rust_client, google_client, "T", TAGS, P("tags", "NOT_IN", ["a", "b"]))


def test_19_tags_in_bz(rust_client, google_client):
    _compare(rust_client, google_client, "T", TAGS, P("tags", "IN", ["b", "z"]))


def test_20_tags_gt_a(rust_client, google_client):
    _compare(rust_client, google_client, "T", TAGS, P("tags", ">", "a"))


def test_21_tags_eq_a_and_eq_b(rust_client, google_client):
    filt = And([P("tags", "=", "a"), P("tags", "=", "b")])
    _compare(rust_client, google_client, "T", TAGS, filt)


def test_22_tags_gt_a_and_lt_c(rust_client, google_client):
    filt = And([P("tags", ">", "a"), P("tags", "<", "c")])
    _compare(rust_client, google_client, "T", TAGS, filt)


def test_23_tags_ge_b_and_le_b(rust_client, google_client):
    filt = And([P("tags", ">=", "b"), P("tags", "<=", "b")])
    _compare(rust_client, google_client, "T", TAGS, filt)


def test_24_tags_eq_c_or_eq_b(rust_client, google_client):
    filt = Or([P("tags", "=", "c"), P("tags", "=", "b")])
    _compare(rust_client, google_client, "T", TAGS, filt)


# --- Control: type mismatch -------------------------------------------------


def test_25_string_prop_lt_int(rust_client, google_client):
    _compare(rust_client, google_client, "M", MIXED, P("name", "<", 10))
