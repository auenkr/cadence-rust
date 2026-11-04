#!/usr/bin/env python3
"""Sample Docker drop-in service CPU/RSS during an SDK benchmark interval."""
import argparse
import json
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


def parse_bytes(text):
    m = re.fullmatch(r"\s*([0-9.]+)\s*([KMGT]i?B)?\s*", text, re.I)
    if not m:
        raise ValueError(f"unrecognized Docker memory value: {text!r}")
    scale = {None: 1, "b": 1, "kb": 1000, "kib": 1024, "mb": 1000**2, "mib": 1024**2, "gb": 1000**3, "gib": 1024**3, "tb": 1000**4, "tib": 1024**4}
    return int(float(m.group(1)) * scale[m.group(2).lower() if m.group(2) else None])


def snapshot():
    out = subprocess.check_output(["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"], text=True)
    services = {}
    for line in out.splitlines():
        name, cpu, mem = line.split("\t", 2)
        used = mem.split("/", 1)[0].strip()
        services[name] = {"cpu_percent": float(cpu.rstrip("%")), "rss_bytes": parse_bytes(used)}
    return services


def collect(duration, interval, output, services_filter):
    start_wall = datetime.now(timezone.utc).isoformat()
    start = time.monotonic()
    previous = None
    totals, peaks, samples = {}, {}, 0
    while True:
        now = time.monotonic()
        current = snapshot()
        if services_filter:
            missing = set(services_filter) - current.keys()
            if missing:
                raise RuntimeError("requested Docker containers not running: " + ", ".join(sorted(missing)))
            current = {k: current[k] for k in services_filter}
        if previous is not None:
            dt = now - previous[0]
            for name, data in current.items():
                before = previous[1].get(name, data)["cpu_percent"]
                totals[name] = totals.get(name, 0.0) + (before + data["cpu_percent"]) * 0.5 * dt / 100
        for name, data in current.items():
            peaks[name] = max(peaks.get(name, 0), data["rss_bytes"])
        samples += 1
        previous = (now, current)
        elapsed = now - start
        if elapsed >= duration:
            break
        time.sleep(min(interval, duration - elapsed))
    elapsed = time.monotonic() - start
    result = {"schema_version": 1, "started_at_utc": start_wall, "duration_seconds": elapsed, "sampling_interval_seconds": interval, "samples": samples, "method": "docker stats polling; CPU seconds are trapezoidal interval estimates from sampled container CPU percent; RSS is sampled Docker memory usage", "containers": {name: {"estimated_cpu_seconds": totals.get(name, 0.0), "average_cpu_cores": totals.get(name, 0.0) / elapsed, "peak_rss_bytes": rss} for name, rss in sorted(peaks.items())}}
    Path(output).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--duration", type=float, required=True, help="sampling interval duration in seconds; start immediately before measured SDK run")
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--output", required=True)
    ap.add_argument("--containers", nargs="*", help="Docker container names to include; default samples all running containers")
    a = ap.parse_args()
    if a.duration <= 0 or a.interval <= 0:
        ap.error("duration and interval must be positive")
    collect(a.duration, a.interval, a.output, a.containers)


if __name__ == "__main__":
    main()
