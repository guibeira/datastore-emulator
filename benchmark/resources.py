"""CPU and memory of the emulator container while a benchmark runs.

`StatsSampler` streams `docker stats` for one container in the background;
`stop()` summarizes the samples (mean and peak CPU in percent of one core,
peak and final memory in MB). `resource_entries` shapes the summaries into
github-action-benchmark entries, one chart per metric with both emulators.
"""

import json
import re
import statistics
import subprocess
import threading

_UNITS = {
    "B": 1,
    "kB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12,
    "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4,
}
_BYTES = re.compile(r"^\s*([0-9.]+)\s*([A-Za-z]+)\s*$")
_JSON = re.compile(r"\{.*\}")
_MB = 1024**2


def parse_percent(text):
    return float(text.strip().rstrip("%"))


def parse_bytes(text):
    match = _BYTES.match(text)
    if not match:
        raise ValueError(f"not a size: {text!r}")
    return float(match.group(1)) * _UNITS[match.group(2)]


def parse_stats_line(line):
    """One `docker stats --format '{{json .}}'` line, which comes wrapped in
    terminal control sequences; None when it holds no sample."""
    match = _JSON.search(line)
    if not match:
        return None
    record = json.loads(match.group(0))
    used = record["MemUsage"].split("/")[0]
    return {"cpu": parse_percent(record["CPUPerc"]), "mem": parse_bytes(used)}


def summarize_samples(samples):
    if not samples:
        return None
    return {
        "samples": len(samples),
        "cpu_mean_percent": statistics.fmean(s["cpu"] for s in samples),
        "cpu_peak_percent": max(s["cpu"] for s in samples),
        "mem_peak_mb": max(s["mem"] for s in samples) / _MB,
        "mem_end_mb": samples[-1]["mem"] / _MB,
    }


class StatsSampler:
    """Runs `docker stats` for `container` until stopped; `command` replaces
    the docker invocation in tests."""

    def __init__(self, container, command=None):
        self.command = command or ["docker", "stats", "--format", "{{json .}}", container]
        self.samples = []
        self.process = None
        self.reader = None

    def start(self):
        self.process = subprocess.Popen(
            self.command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True
        )
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()

    def _read(self):
        for line in self.process.stdout:
            sample = parse_stats_line(line)
            if sample is not None:
                self.samples.append(sample)

    def stop(self):
        if self.process is None:
            return None
        self.process.kill()
        self.process.wait()
        self.reader.join(timeout=5)
        return summarize_samples(self.samples)


def resource_entries(resources_by_target, extra):
    entries = []
    for target, summary in resources_by_target.items():
        if not summary:
            continue
        for name, unit, key in (
            ("CPU ({}) - mean %", "%", "cpu_mean_percent"),
            ("CPU ({}) - peak %", "%", "cpu_peak_percent"),
            ("Memory ({}) - peak MB", "MB", "mem_peak_mb"),
            ("Memory ({}) - end MB", "MB", "mem_end_mb"),
        ):
            entries.append({"name": name.format(target), "unit": unit, "value": round(summary[key], 2), "extra": extra})
    return entries
