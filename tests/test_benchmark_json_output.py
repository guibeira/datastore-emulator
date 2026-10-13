"""merge_results turns the per-target files written by test_benchmark.py
into the two dashboard files."""

import json

import pytest

from benchmark.merge_results import merge_files


def _write_target(path, label, scale):
    scenarios = {
        "Single Insert": {"total": 1.0 * scale, "p50": 0.01 * scale, "p95": 0.02 * scale},
        "Bulk Insert (50)": {"total": 2.0 * scale, "p50": 0.05 * scale, "p95": 0.09 * scale},
        "Simple Query": {"total": 0.5 * scale, "p50": 0.004 * scale, "p95": 0.01 * scale},
    }
    path.write_text(json.dumps({
        "settings": {"num_clients": 2, "num_runs": 10, "warmup_runs": 3, "repeat": 3},
        "targets": {label: scenarios},
    }))
    return path


def test_emits_three_metrics_per_scenario_per_emulator(tmp_path):
    rust = _write_target(tmp_path / "rust.json", "Rust", 1.0)
    java = _write_target(tmp_path / "java.json", "Java", 5.0)

    latency, speedup = merge_files([rust, java], tmp_path / "out")

    names = [entry["name"] for entry in latency]
    assert len(latency) == 18
    for scenario in ("Single Insert", "Bulk Insert (50)", "Simple Query"):
        for label in ("Rust", "Java"):
            assert f"{scenario} ({label}) - avg seconds per client" in names
            assert f"{scenario} ({label}) - p50 s/op" in names
            assert f"{scenario} ({label}) - p95 s/op" in names
    assert [entry["name"] for entry in speedup] == [
        "Single Insert (Java/Rust) - p50 speedup",
        "Bulk Insert (50) (Java/Rust) - p50 speedup",
        "Simple Query (Java/Rust) - p50 speedup",
    ]
    assert all(entry["value"] == pytest.approx(5.0) for entry in speedup)


def test_values_and_extra_come_from_the_files(tmp_path):
    rust = _write_target(tmp_path / "rust.json", "Rust", 1.0)
    java = _write_target(tmp_path / "java.json", "Java", 5.0)

    latency, _ = merge_files([rust, java], tmp_path / "out")

    by_name = {entry["name"]: entry for entry in latency}
    avg = by_name["Single Insert (Rust) - avg seconds per client"]
    assert avg["unit"] == "s"
    assert avg["value"] == pytest.approx(1.0)
    assert by_name["Single Insert (Java) - p95 s/op"]["value"] == pytest.approx(0.1)
    assert "2 clients" in avg["extra"]
    assert "10 runs/client" in avg["extra"]
    assert "median of 3" in avg["extra"]


def test_writes_both_files_creating_the_directory(tmp_path):
    rust = _write_target(tmp_path / "rust.json", "Rust", 1.0)
    java = _write_target(tmp_path / "java.json", "Java", 5.0)
    out = tmp_path / "nested" / "dir"

    merge_files([rust, java], out)

    assert len(json.loads((out / "latency.json").read_text())) == 18
    assert len(json.loads((out / "speedup.json").read_text())) == 3


def test_publishes_rust_alone_when_the_java_file_is_missing(tmp_path, capsys):
    rust = _write_target(tmp_path / "rust.json", "Rust", 1.0)

    latency, speedup = merge_files([rust, tmp_path / "java.json"], tmp_path / "out")

    assert len(latency) == 9
    assert speedup == []
    assert json.loads((tmp_path / "out" / "speedup.json").read_text()) == []
    assert "java.json is missing" in capsys.readouterr().out


def test_fails_when_no_file_exists(tmp_path):
    with pytest.raises(SystemExit):
        merge_files([tmp_path / "rust.json"], tmp_path / "out")
