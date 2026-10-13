"""Merge per-target benchmark results into the two dashboard files.

    python benchmark/merge_results.py --out-dir benchmark-output rust.json java.json

Writes `latency.json` (customSmallerIsBetter) and `speedup.json`
(customBiggerIsBetter) for github-action-benchmark.
"""

import argparse
import json
from pathlib import Path

import sys

# Runnable as `python benchmark/<script>.py` from anywhere: import from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from benchmark.stats import merge_entries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("files", nargs="+", help="JSON files written by test_benchmark.py --json-out")
    args = parser.parse_args()

    targets = {}
    settings = None
    for path in args.files:
        if not Path(path).exists():
            print(f"::warning::{path} is missing; publishing without it")
            continue
        data = json.loads(Path(path).read_text())
        targets.update(data["targets"])
        settings = settings or data["settings"]
    if not targets:
        raise SystemExit("no benchmark results to merge")

    extra = (
        f"{settings['num_clients']} clients, {settings['num_runs']} runs/client, "
        f"{settings['warmup_runs']} warm-up, median of {settings['repeat']}"
    )
    latency, speedup = merge_entries(targets, extra)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "latency.json").write_text(json.dumps(latency, indent=2))
    (out / "speedup.json").write_text(json.dumps(speedup, indent=2))
    print(f"Wrote {len(latency)} latency and {len(speedup)} speedup entries to {out}")


if __name__ == "__main__":
    main()
