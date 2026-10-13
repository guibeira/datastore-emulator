"""Merge per-target benchmark results into the dashboard files.

    python benchmark/merge_results.py --out-dir benchmark-output rust.json java.json

Writes `latency.json` (customSmallerIsBetter), `speedup.json`
(customBiggerIsBetter) and `resources.json` (customSmallerIsBetter; empty
when no target sampled its container) for github-action-benchmark.
"""

import argparse
import json
from pathlib import Path

import sys

# Runnable as `python benchmark/<script>.py` from anywhere: import from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from benchmark.resources import resource_entries
from benchmark.stats import merge_entries


def merge_files(paths, out_dir):
    """Merge the per-target files that exist into `out_dir/latency.json`,
    `out_dir/speedup.json` and `out_dir/resources.json`; returns the three
    entry lists."""
    targets = {}
    resources = {}
    settings = None
    for path in paths:
        path = Path(path)
        if not path.exists():
            print(f"::warning::{path} is missing; publishing without it")
            continue
        data = json.loads(path.read_text())
        targets.update(data["targets"])
        resources.update(data.get("resources", {}))
        settings = settings or data["settings"]
    if not targets:
        raise SystemExit("no benchmark results to merge")

    extra = (
        f"{settings['num_clients']} clients, {settings['num_runs']} runs/client, "
        f"{settings['warmup_runs']} warm-up, median of {settings['repeat']}"
    )
    latency, speedup = merge_entries(targets, extra)
    usage = resource_entries(resources, extra)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "latency.json").write_text(json.dumps(latency, indent=2))
    (out / "speedup.json").write_text(json.dumps(speedup, indent=2))
    (out / "resources.json").write_text(json.dumps(usage, indent=2))
    print(f"Wrote {len(latency)} latency, {len(speedup)} speedup and {len(usage)} resource entries to {out}")
    return latency, speedup, usage


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("files", nargs="+", help="JSON files written by test_benchmark.py --json-out")
    args = parser.parse_args()
    merge_files(args.files, args.out_dir)


if __name__ == "__main__":
    main()
