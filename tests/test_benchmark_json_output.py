import json
from pathlib import Path

import pytest

from benchmark.test_benchmark import write_json_output


def _sample_results():
    rust = [
        {"Single Insert": 1.0, "Bulk Insert (50)": 2.0, "Simple Query": 0.5},
        {"Single Insert": 1.2, "Bulk Insert (50)": 2.4, "Simple Query": 0.6},
    ]
    java = [
        {"Single Insert": 5.0, "Bulk Insert (50)": 10.0, "Simple Query": 2.0},
        {"Single Insert": 5.2, "Bulk Insert (50)": 10.4, "Simple Query": 2.2},
    ]
    return rust, java


def test_emits_one_entry_per_op_per_implementation(tmp_path):
    rust, java = _sample_results()
    out = tmp_path / "bench.json"

    write_json_output(rust, java, num_clients=2, number_of_runs_per_client=10, output_path=out)

    data = json.loads(out.read_text())
    names = [entry["name"] for entry in data]
    assert len(data) == 6
    assert any("Single Insert (Rust)" in n for n in names)
    assert any("Single Insert (Java)" in n for n in names)
    assert any("Bulk Insert (50) (Rust)" in n for n in names)
    assert any("Bulk Insert (50) (Java)" in n for n in names)
    assert any("Simple Query (Rust)" in n for n in names)
    assert any("Simple Query (Java)" in n for n in names)


def test_values_are_mean_seconds_per_client(tmp_path):
    rust, java = _sample_results()
    out = tmp_path / "bench.json"

    write_json_output(rust, java, num_clients=2, number_of_runs_per_client=10, output_path=out)

    data = {entry["name"]: entry for entry in json.loads(out.read_text())}
    rust_entry = next(v for k, v in data.items() if k.startswith("Single Insert (Rust)"))
    java_entry = next(v for k, v in data.items() if k.startswith("Single Insert (Java)"))

    assert rust_entry["unit"] == "s"
    assert rust_entry["value"] == pytest.approx((1.0 + 1.2) / 2, rel=1e-6)
    assert java_entry["value"] == pytest.approx((5.0 + 5.2) / 2, rel=1e-6)
    assert "2 clients" in rust_entry["extra"]
    assert "10 runs/client" in rust_entry["extra"]


def test_creates_parent_directory(tmp_path):
    rust, java = _sample_results()
    out = tmp_path / "nested" / "dir" / "bench.json"

    write_json_output(rust, java, num_clients=2, number_of_runs_per_client=10, output_path=out)

    assert out.exists()
    assert json.loads(out.read_text())


def test_skips_when_either_side_missing(tmp_path, capsys):
    out = tmp_path / "bench.json"

    write_json_output([], [{"Single Insert": 1.0}], num_clients=1, number_of_runs_per_client=1, output_path=out)

    assert not out.exists()
    captured = capsys.readouterr()
    assert "Skipping" in captured.out
