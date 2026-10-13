"""Statistics and dashboard entries for the benchmark.

A run produces, per scenario, the timing of every call of every client.
`summarize` turns timings into total/mean/p50/p95, `median_of_summaries`
picks the median across repeats, and `merge_entries` shapes the per-target
summaries into github-action-benchmark entries: latencies (smaller is
better) and the Java/Rust speedup (bigger is better).
"""

import math
import statistics


def percentile(sorted_values, fraction):
    """Nearest-rank percentile of an ascending list."""
    rank = max(1, math.ceil(fraction * len(sorted_values)))
    return sorted_values[rank - 1]


def summarize(timings):
    ordered = sorted(timings)
    return {
        "count": len(ordered),
        "total": sum(ordered),
        "mean": statistics.fmean(ordered),
        "p50": statistics.median(ordered),
        "p95": percentile(ordered, 0.95),
    }


def median_of_summaries(summaries):
    return {key: statistics.median(s[key] for s in summaries) for key in summaries[0]}


def _entry(name, unit, value, extra):
    return {"name": name, "unit": unit, "value": round(value, 6), "extra": extra}


def merge_entries(results_by_target, extra):
    """`results_by_target` maps a target label ("Rust", "Java") to
    {scenario: {"total", "p50", "p95"}}, where `total` is the mean per client
    of the time its runs took (the metric the dashboard has always shown)."""
    targets = list(results_by_target)
    scenarios = list(results_by_target[targets[0]])
    latency = []
    speedup = []
    for scenario in scenarios:
        for target in targets:
            result = results_by_target[target][scenario]
            latency.append(
                _entry(f"{scenario} ({target}) - avg seconds per client", "s", result["total"], extra)
            )
            latency.append(_entry(f"{scenario} ({target}) - p50 s/op", "s", result["p50"], extra))
            latency.append(_entry(f"{scenario} ({target}) - p95 s/op", "s", result["p95"], extra))
        if "Rust" in results_by_target and "Java" in results_by_target:
            rust = results_by_target["Rust"][scenario]
            java = results_by_target["Java"][scenario]
            speedup.append(
                {
                    "name": f"{scenario} (Java/Rust) - p50 speedup",
                    "unit": "x",
                    "value": round(java["p50"] / rust["p50"], 2),
                    "extra": f"Java p50 {java['p50']:g} s / Rust p50 {rust['p50']:g} s",
                }
            )
    return latency, speedup
