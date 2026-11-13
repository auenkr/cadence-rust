#!/usr/bin/env python3
"""Collect cumulative cgroup CPU and memory measurements for Docker containers."""
import argparse
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

PIDS = {}


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
        PIDS[name] = int(pid)
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
        # Docker containers each have their own network namespace. Read from the
        # service init PID; namespace totals include all its processes.
        net = Path(f"/proc/{PIDS[name]}/net/dev").read_text().splitlines()[2:]
        receive = transmit = 0
        for line in net:
            _, counters = line.split(":", 1)
            values = counters.split()
            receive += int(values[0]); transmit += int(values[8])
        read_bytes = write_bytes = rss = 0
        if version == "v2":
            for line in (paths / "io.stat").read_text().splitlines():
                fields = dict(item.split("=") for item in line.split()[1:])
                read_bytes += int(fields.get("rbytes", 0))
                write_bytes += int(fields.get("wbytes", 0))
            # Sum resident pages for processes in this container's cgroup.
            # Shared pages can be counted more than once; cgroup memory is also retained.
            for pid in (paths / "cgroup.procs").read_text().splitlines():
                try:
                    rss += int(Path(f"/proc/{pid}/statm").read_text().split()[1]) * os.sysconf("SC_PAGE_SIZE")
                except FileNotFoundError:
                    pass  # process exited between membership and statm reads
        current[name] = {"cpu_ns": cpu_ns, "memory_bytes": memory_bytes,
                         "rss_bytes": rss if version == "v2" else None,
                         "network_receive_bytes": receive, "network_transmit_bytes": transmit,
                         "disk_read_bytes": read_bytes if version == "v2" else None,
                         "disk_write_bytes": write_bytes if version == "v2" else None}
    return current


def cpu_seconds(before_ns, after_ns):
    if after_ns < before_ns:
        raise ValueError("cgroup CPU counter reset during measurement")
    return (after_ns - before_ns) / 1_000_000_000


def collect(interval, output, names, start_event=None, end_event=None, duration=None, ready_event=None, event_timeout=180):
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
        deadline = time.monotonic() + event_timeout
        while not marker.exists():
            if time.monotonic() >= deadline:
                raise RuntimeError(f"timed out waiting for measurement marker: {path}")
            time.sleep(min(interval, 0.1))
        return None

    if start_event:
        wait_event(start_event)
    started_at = datetime.now(timezone.utc).isoformat()
    started = time.monotonic()
    first = snapshot(selected)
    if ready_event:
        Path(ready_event).write_text(started_at, encoding="utf-8")
    peak = dict((name, data["memory_bytes"]) for name, data in first.items())
    rss_peak = {name: data["rss_bytes"] for name,data in first.items()}
    samples = 1
    while True:
        if end_event and Path(end_event).exists():
            break
        elapsed = time.monotonic() - started
        if end_event and elapsed >= event_timeout:
            raise RuntimeError("timed out waiting for measurement end marker")
        if duration is not None and elapsed >= duration:
            break
        time.sleep(min(interval, max(0.01, duration - elapsed)) if duration is not None else interval)
        now = snapshot(selected)
        for name, data in now.items():
            peak[name] = max(peak[name], data["memory_bytes"])
            if data["rss_bytes"] is not None:
                rss_peak[name] = max(rss_peak[name], data["rss_bytes"])
        last = now
        samples += 1
    last = snapshot(selected)
    samples += 1
    for name, data in last.items():
        peak[name] = max(peak[name], data["memory_bytes"])
        if data["rss_bytes"] is not None:
            rss_peak[name] = max(rss_peak[name], data["rss_bytes"])
    elapsed = time.monotonic() - started
    containers_result = {}
    for name, (_, version) in selected.items():
        try:
            consumed_cpu_seconds = cpu_seconds(first[name]["cpu_ns"], last[name]["cpu_ns"])
        except ValueError as error:
            raise RuntimeError(f"{error} for {name}") from error
        containers_result[name] = {"cpu_seconds": consumed_cpu_seconds, "average_cpu_cores": consumed_cpu_seconds / elapsed,
                                   "peak_cgroup_memory_bytes": peak[name], "cgroup_version": version,
                                   "initial_cpu_ns": first[name]["cpu_ns"], "final_cpu_ns": last[name]["cpu_ns"],
                                   "peak_summed_process_rss_bytes": rss_peak[name]}
        for metric in ("network_receive_bytes", "network_transmit_bytes", "disk_read_bytes", "disk_write_bytes"):
            before, after = first[name][metric], last[name][metric]
            if before is not None and after < before:
                raise RuntimeError(f"counter reset: {name}/{metric}")
            containers_result[name][metric] = after - before if before is not None else None
    result = {"schema_version": 2, "started_at_utc": started_at, "duration_seconds": elapsed,
              "ended_at_utc": datetime.now(timezone.utc).isoformat(),
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
    ap.add_argument("--ready-event", help="write after baseline capture; SDK waits for this before measured traffic")
    ap.add_argument("--event-timeout", type=float, default=180, help="maximum seconds waiting for each marker")
    a = ap.parse_args()
    if a.duration is not None and a.duration <= 0 or a.interval <= 0 or a.event_timeout <= 0:
        ap.error("duration and interval must be positive")
    try:
        collect(a.interval, a.output, a.containers, a.start_event, a.end_event, a.duration, a.ready_event, a.event_timeout)
    except ValueError as error:
        ap.error(str(error))


if __name__ == "__main__":
    main()
