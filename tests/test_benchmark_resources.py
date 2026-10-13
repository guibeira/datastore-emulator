"""CPU and memory of the emulator container during a benchmark, sampled from
`docker stats`, summarized and shaped into dashboard entries."""

import sys
import textwrap

import pytest

from benchmark.resources import (
    StatsSampler,
    parse_bytes,
    parse_percent,
    parse_stats_line,
    resource_entries,
    summarize_samples,
)


@pytest.mark.parametrize(
    ("text", "expected"),
    [("12.34%", 12.34), ("0.00%", 0.0), ("150%", 150.0)],
)
def test_parse_percent(text, expected):
    assert parse_percent(text) == pytest.approx(expected)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("512B", 512),
        ("1.5kB", 1500),
        ("101.3MiB", 101.3 * 1024**2),
        ("2GiB", 2 * 1024**3),
        ("7.65GB", 7.65 * 1e9),
    ],
)
def test_parse_bytes(text, expected):
    assert parse_bytes(text) == pytest.approx(expected)


def test_parse_stats_line_strips_terminal_control_sequences():
    line = '\x1b[H{"CPUPerc":"1.46%","MemUsage":"101.3MiB / 17.78GiB","Name":"benchmark-rust-1"}\x1b[K'

    assert parse_stats_line(line) == {"cpu": pytest.approx(1.46), "mem": pytest.approx(101.3 * 1024**2)}


def test_parse_stats_line_ignores_noise():
    assert parse_stats_line("\x1b[K") is None
    assert parse_stats_line("") is None


def test_summarize_samples_reports_mean_peak_and_end_in_mb():
    mb = 1024**2
    samples = [
        {"cpu": 10.0, "mem": 50 * mb},
        {"cpu": 30.0, "mem": 80 * mb},
        {"cpu": 20.0, "mem": 60 * mb},
    ]

    assert summarize_samples(samples) == {
        "samples": 3,
        "cpu_mean_percent": pytest.approx(20.0),
        "cpu_peak_percent": pytest.approx(30.0),
        "mem_peak_mb": pytest.approx(80.0),
        "mem_end_mb": pytest.approx(60.0),
    }


def test_summarize_samples_is_none_without_samples():
    assert summarize_samples([]) is None


def test_sampler_collects_until_stopped():
    fake_stats = textwrap.dedent(
        """
        import sys, time
        for cpu, mem in ((5, "10MiB"), (15, "30MiB"), (10, "20MiB")):
            print('\\x1b[H{"CPUPerc":"%d.00%%","MemUsage":"%s / 1GiB"}\\x1b[K' % (cpu, mem), flush=True)
            time.sleep(0.05)
        time.sleep(60)
        """
    )
    sampler = StatsSampler("ignored", command=[sys.executable, "-c", fake_stats])

    sampler.start()
    import time

    time.sleep(0.5)
    summary = sampler.stop()

    assert summary["samples"] == 3
    assert summary["cpu_mean_percent"] == pytest.approx(10.0)
    assert summary["cpu_peak_percent"] == pytest.approx(15.0)
    assert summary["mem_peak_mb"] == pytest.approx(30.0)
    assert summary["mem_end_mb"] == pytest.approx(20.0)


def test_sampler_without_samples_reports_none():
    sampler = StatsSampler("ignored", command=[sys.executable, "-c", "import time; time.sleep(60)"])
    sampler.start()
    assert sampler.stop() is None


def test_resource_entries_one_chart_per_metric_both_emulators():
    rust = {"samples": 10, "cpu_mean_percent": 40.0, "cpu_peak_percent": 90.0, "mem_peak_mb": 25.0, "mem_end_mb": 20.0}
    java = {"samples": 12, "cpu_mean_percent": 150.0, "cpu_peak_percent": 199.0, "mem_peak_mb": 900.0, "mem_end_mb": 850.0}

    entries = resource_entries({"Rust": rust, "Java": java}, extra="10 clients")

    assert [e["name"] for e in entries] == [
        "CPU (Rust) - mean %",
        "CPU (Rust) - peak %",
        "Memory (Rust) - peak MB",
        "Memory (Rust) - end MB",
        "CPU (Java) - mean %",
        "CPU (Java) - peak %",
        "Memory (Java) - peak MB",
        "Memory (Java) - end MB",
    ]
    by_name = {e["name"]: e for e in entries}
    assert by_name["CPU (Java) - mean %"] == {"name": "CPU (Java) - mean %", "unit": "%", "value": 150.0, "extra": "10 clients"}
    assert by_name["Memory (Rust) - peak MB"]["unit"] == "MB"


def test_resource_entries_skips_targets_without_samples():
    entries = resource_entries({"Rust": None, "Java": {"samples": 1, "cpu_mean_percent": 1.0, "cpu_peak_percent": 1.0, "mem_peak_mb": 1.0, "mem_end_mb": 1.0}}, extra="")

    assert all("(Java)" in e["name"] for e in entries) and len(entries) == 4
