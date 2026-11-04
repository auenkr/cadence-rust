#!/usr/bin/env python3
"""Collect cumulative cgroup CPU and memory measurements for Docker containers."""
import argparse
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


def parse_cgroups(entries):
    groups = {}
    for row in entries:
        _, controllers, relative = row.split(":", 2)
        for controller in controllers.split(","):
            groups[controller] = relative.lstrip("/")
    return groups


def _container_cgroups():
    raw = subprocess.check_output(["docker", "inspect", "--format", "{{.Name}}\t{{.State.Pid}}", *subprocess.check_output(["docker", "ps", "-q"], text=True).split()], text=True)
    result = {}
    for line in raw.splitlines():
        name, pid = line.lstrip("/").split("\t", 1)
        if int(pid) <= 0:
            continue
        entries = Path(f"/proc/{pid}/cgroup").read_text().splitlines()
        groups = parse_cgroups(entries)
        # Cgroup v2 stores cpu.stat and memory.current under the unified path.
        unified = Path("/sys/fs/cgroup") / groups.get("", "")
        if "" in groups and (unified / "cpu.stat").exists() and (unified / "memory.current").exists():
            result[name] = (unified, "v2")
            continue
        cpu = Path("/sys/fs/cgroup/cpuacct") / groups.get("cpuacct", "")
        memory = Path("/sys/fs/cgroup/memory") / groups.get("memory", "")
        if "cpuacct" in groups and "memory" in groups and (cpu / "cpuacct.usage").exists() and (memory / "memory.usage_in_bytes").exists():
            result[name] = ((cpu, memory), "v1")
    return result


def snapshot(containers):
    current = {}
    for name, (paths, version) in containers.items():
        if version == "v2":
            cpu_stat = dict(line.split() for line in (paths / "cpu.stat").read_text().splitlines())
            cpu_ns = int(cpu_stat["usage_usec"]) * 1000
            memory_bytes = int((paths / "memory.current").read_text())
        else:
            cpu, memory = paths
            cpu_ns = int((cpu / "cpuacct.usage").read_text())
            memory_bytes = int((memory / "memory.usage_in_bytes").read_text())
        current[name] = {"cpu_ns": cpu_ns, "memory_bytes": memory_bytes}
    return current


def cpu_seconds(before_ns, after_ns):
    if after_ns < before_ns:
        raise ValueError("cgroup CPU counter reset during measurement")
    return (after_ns - before_ns) / 1_000_000_000


def collect(interval, output, names, start_event=None, end_event=None, duration=None):
    if bool(start_event) != bool(end_event):
        raise ValueError("--start-event and --end-event must be provided together")
    if not start_event and duration is None:
        raise ValueError("provide marker events or --duration")
    selected = _container_cgroups()
    if names:
        missing = set(names) - selected.keys()
        if missing:
            raise RuntimeError("containers missing accessible cgroup counters: " + ", ".join(sorted(missing)))
        selected = {n: selected[n] for n in names}
    if not selected:
        raise RuntimeError("no running Docker containers with accessible cgroup counters")

    def wait_event(path):
        marker = Path(path)
        while not marker.exists():
            time.sleep(1)
        return None

    if start_event:
        wait_event(start_event)
    started_at = datetime.now(timezone.utc).isoformat()
    started = time.monotonic()
    first = snapshot(selected)
    peak = dict((name, data["memory_bytes"]) for name, data in first.items())
    samples = 1
    while True:
        if end_event and Path(end_event).exists():
            break
        elapsed = time.monotonic() - started
        if duration is not None and elapsed >= duration:
            break
        time.sleep(min(interval, max(0.01, duration - elapsed)) if duration is not None else interval)
        now = snapshot(selected)
        for name, data in now.items():
            peak[name] = max(peak[name], data["memory_bytes"])
        last = now
        samples += 1
    last = snapshot(selected)
    samples += 1
    for name, data in last.items():
        peak[name] = max(peak[name], data["memory_bytes"])
    elapsed = time.monotonic() - started
    containers_result = {}
    for name, (_, version) in selected.items():
        try:
            consumed_cpu_seconds = cpu_seconds(first[name]["cpu_ns"], last[name]["cpu_ns"])
        except ValueError as error:
            raise RuntimeError(f"{error} for {name}") from error
        containers_result[name] = {"cpu_seconds": consumed_cpu_seconds, "average_cpu_cores": consumed_cpu_seconds / elapsed,
                                   "peak_cgroup_memory_bytes": peak[name], "cgroup_version": version}
    result = {"schema_version": 2, "started_at_utc": started_at, "duration_seconds": elapsed,
              "sampling_interval_seconds": interval, "samples": samples,
              "cpu_method": "exact delta of cumulative cgroup cpu usage counters",
              "memory_method": "peak sampled cgroup memory usage; includes charged cache and is not RSS",
              "interval_method": "start/end marker files" if start_event else "fixed duration",
              "containers": containers_result}
    Path(output).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--duration", type=float, help="duration in seconds (alternative to marker files)")
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--output", required=True)
    ap.add_argument("--containers", nargs="*", help="Docker container names; default is all accessible running containers")
    ap.add_argument("--start-event", help="wait for this marker file before the measured interval")
    ap.add_argument("--end-event", help="stop the measured interval when this marker file appears")
    a = ap.parse_args()
    if a.duration is not None and a.duration <= 0 or a.interval <= 0:
        ap.error("duration and interval must be positive")
    try:
        collect(a.interval, a.output, a.containers, a.start_event, a.end_event, a.duration)
    except ValueError as error:
        ap.error(str(error))


if __name__ == "__main__":
    main()
