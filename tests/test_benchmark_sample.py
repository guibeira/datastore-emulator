"""sample.py runs the load generator while sampling its emulator's container
and adds the resource summary to the JSON the command wrote."""

import json
import sys
import textwrap

from benchmark.resources import StatsSampler
from benchmark.sample import main, sample_around

FAKE_STATS = textwrap.dedent(
    """
    import time
    for cpu, mem in ((5, "10MiB"), (15, "30MiB")):
        print('{"CPUPerc":"%d.00%%","MemUsage":"%s / 1GiB"}' % (cpu, mem), flush=True)
        time.sleep(0.05)
    time.sleep(60)
    """
)


def _writer(path, exit_code=0):
    script = textwrap.dedent(
        f"""
        import json, time
        time.sleep(0.3)
        json.dump({{"settings": {{"num_clients": 1}}, "targets": {{"Rust": {{"Get": {{"total": 1, "p50": 0.1, "p95": 0.2}}}}}}}}, open({str(path)!r}, "w"))
        raise SystemExit({exit_code})
        """
    )
    return [sys.executable, "-c", script]


def test_adds_the_container_usage_to_the_json_the_command_wrote(tmp_path):
    out = tmp_path / "rust.json"
    sampler = StatsSampler("ignored", command=[sys.executable, "-c", FAKE_STATS])

    code = sample_around(_writer(out), out, sampler)

    assert code == 0
    data = json.loads(out.read_text())
    assert data["targets"]["Rust"]["Get"]["p50"] == 0.1
    assert data["resources"]["Rust"]["samples"] == 2
    assert data["resources"]["Rust"]["mem_peak_mb"] == 30.0


def test_keeps_the_command_exit_code(tmp_path):
    out = tmp_path / "rust.json"
    sampler = StatsSampler("ignored", command=[sys.executable, "-c", FAKE_STATS])

    assert sample_around(_writer(out, exit_code=2), out, sampler) == 2
    assert "resources" in json.loads(out.read_text())


def test_without_json_output_there_is_nothing_to_add(tmp_path):
    out = tmp_path / "missing.json"
    sampler = StatsSampler("ignored", command=[sys.executable, "-c", FAKE_STATS])

    assert sample_around([sys.executable, "-c", "raise SystemExit(3)"], out, sampler) == 3
    assert not out.exists()


def test_main_wires_the_container_and_the_command(tmp_path):
    out = tmp_path / "rust.json"
    made = []

    def factory(container):
        made.append(container)
        return StatsSampler(container, command=[sys.executable, "-c", FAKE_STATS])

    code = main(["--container", "abc123", "--json", str(out), "--", *_writer(out)], sampler_factory=factory)

    assert code == 0 and made == ["abc123"]
    assert json.loads(out.read_text())["resources"]["Rust"]["samples"] == 2


def test_main_takes_the_command_without_a_separator(tmp_path):
    """`poetry run` swallows `--`, so the command is simply what follows the options."""
    out = tmp_path / "rust.json"

    def factory(container):
        return StatsSampler(container, command=[sys.executable, "-c", FAKE_STATS])

    code = main(["--container", "abc123", "--json", str(out), *_writer(out)], sampler_factory=factory)

    assert code == 0
    assert json.loads(out.read_text())["resources"]["Rust"]["samples"] == 2
