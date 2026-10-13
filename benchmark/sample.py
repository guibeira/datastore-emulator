"""Run a benchmark command while sampling its emulator's container, then add
the CPU and memory summary to the JSON the command wrote.

    poetry run python benchmark/sample.py --container NAME --json out/rust.json \\
        ./target/release/loadgen --target rust --json-out out/rust.json

The command is everything after the options (no `--`: poetry run swallows it).

Exits with the command's exit code.
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

# Runnable as `python benchmark/<script>.py` from anywhere: import from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from benchmark.resources import StatsSampler


# Below the benchmark job's 45-minute limit with room for both emulators.
DEFAULT_TIMEOUT = 1200.0


def sample_around(command, json_path, sampler, timeout=DEFAULT_TIMEOUT):
    """Runs `command` with `sampler` active; writes the summary under
    `resources` for the single target in `json_path`, when the file exists.
    A command still running after `timeout` seconds is killed and reported
    with exit code 124."""
    sampler.start()
    try:
        code = subprocess.call(command, timeout=timeout)
    except subprocess.TimeoutExpired:
        print(f"::error::{command[0]} still running after {timeout:.0f}s; killed")
        code = 124
    finally:
        usage = sampler.stop()
    json_path = Path(json_path)
    if json_path.exists():
        data = json.loads(json_path.read_text())
        targets = list(data.get("targets", {}))
        if len(targets) == 1:
            data.setdefault("resources", {})[targets[0]] = usage
            json_path.write_text(json.dumps(data, indent=2))
            if usage:
                print(f"\n{targets[0]} container ({usage['samples']} samples): CPU mean {usage['cpu_mean_percent']:.0f}%, "
                      f"peak {usage['cpu_peak_percent']:.0f}%; memory peak {usage['mem_peak_mb']:.0f} MB, "
                      f"end {usage['mem_end_mb']:.0f} MB")
            else:
                print(f"\n{targets[0]} container: no samples")
        else:
            print(f"::warning::{json_path} has {len(targets)} targets; resources need exactly one")
    return code


def main(argv=None, sampler_factory=StatsSampler):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--container", required=True, help="Docker container of the emulator under test")
    parser.add_argument("--json", required=True, help="The JSON file the command writes (its --json-out)")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="Seconds before the command is killed")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="The benchmark command and its arguments")
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("no command given")
    return sample_around(command, args.json, sampler_factory(args.container), timeout=args.timeout)


if __name__ == "__main__":
    raise SystemExit(main())
