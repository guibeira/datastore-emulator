"""Pure functions behind benchmark/test_benchmark.py: per-call statistics,
median across repeats, and the dashboard entries."""

import pytest

from benchmark.stats import median_of_summaries, merge_entries, summarize


def test_summarize_reports_percentiles_and_total():
    summary = summarize([0.3, 0.1, 1.0, 0.4, 0.2])

    assert summary["count"] == 5
    assert summary["total"] == pytest.approx(2.0)
    assert summary["p50"] == pytest.approx(0.3)
    assert summary["p95"] == pytest.approx(1.0)


def test_median_of_summaries_takes_the_median_per_metric():
    merged = median_of_summaries(
        [{"p50": 1.0, "p95": 5.0}, {"p50": 3.0, "p95": 4.0}, {"p50": 2.0, "p95": 9.0}]
    )

    assert merged == {"p50": 2.0, "p95": 5.0}


def test_merge_entries_pairs_targets_and_computes_speedup():
    rust = {"Single Insert": {"total": 1.0, "p50": 0.01, "p95": 0.02}}
    java = {"Single Insert": {"total": 4.0, "p50": 0.04, "p95": 0.1}}

    latency, speedup = merge_entries({"Rust": rust, "Java": java}, extra="10 clients")

    names = {entry["name"] for entry in latency}
    assert "Single Insert (Rust) - avg seconds per client" in names
    assert "Single Insert (Java) - p50 s/op" in names
    assert "Single Insert (Java) - p95 s/op" in names
    assert all(entry["unit"] == "s" and entry["extra"] == "10 clients" for entry in latency)
    assert speedup == [
        {
            "name": "Single Insert (Java/Rust) - p50 speedup",
            "unit": "x",
            "value": 4.0,
            "extra": "Java p50 0.04 s / Rust p50 0.01 s",
        }
    ]
