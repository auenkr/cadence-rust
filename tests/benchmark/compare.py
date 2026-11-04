#!/usr/bin/env python3
"""Validate SDK benchmark runs and produce paired JSON/Markdown reports."""
import argparse
import json
import math
import statistics
from pathlib import Path

PARAMS = ("requests", "warmup", "concurrency", "payload_bytes", "transport", "sticky_execution")


def read_run(path, arm):
    if isinstance(path, dict):
        run = path
        source = "run object"
    else:
        with open(path, encoding="utf-8") as f:
            run = json.load(f)
        source = path
    required = ("label", "requests", "warmup", "concurrency", "payload_bytes", "successful", "failed", "elapsed_seconds", "successful_workflows_per_second", "latency_ms", "successful_latency_samples_ms", "transport", "sticky_execution")
    missing = [k for k in required if k not in run]
    if missing:
        raise ValueError(f"{source}: missing fields: {', '.join(missing)}")
    if run["label"] != arm:
        raise ValueError(f"{source}: expected label {arm!r}, got {run['label']!r}")
    if run["failed"] != 0 or run["successful"] != run["requests"]:
        raise ValueError(f"{source}: failed or incomplete run ({run['successful']}/{run['requests']} successful, {run['failed']} failed)")
    if len(run["successful_latency_samples_ms"]) != run["successful"] or not _positive_finite(run["elapsed_seconds"]):
        raise ValueError(f"{source}: incomplete latency samples or invalid elapsed time")
    samples = run["successful_latency_samples_ms"]
    if any(not _nonnegative_finite(x) for x in samples):
        raise ValueError(f"{source}: latency samples must be finite and nonnegative")
    if not _positive_finite(run["successful_workflows_per_second"]):
        raise ValueError(f"{source}: throughput must be finite and positive")
    if any(not _nonnegative_finite(run["latency_ms"].get(f"p{q}")) for q in (50, 95, 99)):
        raise ValueError(f"{source}: latency percentiles must be finite and nonnegative")
    expected_rate = run["successful"] / run["elapsed_seconds"]
    if not math.isclose(run["successful_workflows_per_second"], expected_rate, rel_tol=1e-5):
        raise ValueError(f"{source}: throughput does not match successful/elapsed")
    expected = {f"p{q}": percentile(samples, q / 100) for q in (50, 95, 99)}
    if any(not math.isclose(run["latency_ms"][key], value, rel_tol=1e-5, abs_tol=1e-9) for key, value in expected.items()):
        raise ValueError(f"{source}: latency percentiles do not match raw samples")
    return run


def _nonnegative_finite(value):
    return isinstance(value, (int, float)) and math.isfinite(value) and value >= 0


def _positive_finite(value):
    return isinstance(value, (int, float)) and math.isfinite(value) and value > 0


def percentile(values, p):
    vals = sorted(values)
    return vals[int((len(vals) - 1) * p)]


def summarize(runs):
    return {
        "repetitions": len(runs),
        "median_throughput_workflows_per_second": statistics.median(r["successful_workflows_per_second"] for r in runs),
        "median_latency_ms": {f"p{q}": statistics.median(r["latency_ms"][f"p{q}"] for r in runs) for q in (50, 95, 99)},
        "runs": runs,
    }


def _read_resources(paths, runs, arm):
    if paths is None:
        return [r.get("resources") for r in runs]
    if len(paths) != len(runs):
        raise ValueError(f"{arm} resources require one file per run")
    return [p if isinstance(p, dict) else json.loads(Path(p).read_text(encoding="utf-8")) if p else None for p in paths]


def _resource_summary(go_resources, rust_resources):
    all_resources = go_resources + rust_resources
    if not any(all_resources):
        return {"status": "not_collected"}
    if any(x is None for x in all_resources):
        return {"status": "incomplete", "reason": "resource samples are required for every run in both arms"}
    def validate(samples):
        expected = None
        for sample in samples:
            containers = sample.get("containers", {})
            if not containers or sample.get("schema_version") != 2:
                raise ValueError("resource sample must use schema_version 2 and contain container metrics")
            names = set(containers)
            if expected is None:
                expected = names
            elif names != expected:
                raise ValueError("resource samples must contain the same containers")
            if not _positive_finite(sample.get("duration_seconds")):
                raise ValueError("resource sample duration must be finite and positive")
            for name, metrics in containers.items():
                if (not _nonnegative_finite(metrics.get("cpu_seconds"))
                        or not _nonnegative_finite(metrics.get("average_cpu_cores"))
                        or not _positive_finite(metrics.get("peak_cgroup_memory_bytes"))):
                    raise ValueError(f"invalid resource metrics for {name}")
        return expected
    gnames, rnames = validate(go_resources), validate(rust_resources)
    if gnames != rnames:
        return {"status": "incomparable", "reason": "Go and Rust resource container sets differ"}
    arms = {}
    for arm, samples in (("go", go_resources), ("rust", rust_resources)):
        arms[arm] = {name: {metric: statistics.median(s["containers"][name][metric] for s in samples)
                            for metric in ("cpu_seconds", "average_cpu_cores", "peak_cgroup_memory_bytes")}
                     for name in sorted(gnames)}
    return {"status": "comparable", "arms": arms,
            "rust_minus_go": {name: {metric: arms["rust"][name][metric] - arms["go"][name][metric]
                                      for metric in arms["go"][name]} for name in sorted(gnames)}}


def compare(go_paths, rust_paths, go_metadata=None, rust_metadata=None, go_resources=None, rust_resources=None):
    if not go_paths or len(go_paths) != len(rust_paths):
        raise ValueError("Go and Rust require the same nonzero number of runs")
    go = [read_run(p, "go") for p in go_paths]
    rust = [read_run(p, "rust") for p in rust_paths]
    for i, (g, r) in enumerate(zip(go, rust), 1):
        differences = [f"{k}: Go={g[k]!r}, Rust={r[k]!r}" for k in PARAMS if g[k] != r[k]]
        if differences:
            raise ValueError(f"pair {i} has mismatched workload parameters: {'; '.join(differences)}")
    for arm, runs in (("Go", go), ("Rust", rust)):
        for index, run in enumerate(runs[1:], 2):
            differences = [f"{k}: run 1={runs[0][k]!r}, run {index}={run[k]!r}" for k in PARAMS if runs[0][k] != run[k]]
            if differences:
                raise ValueError(f"{arm} repetitions have mismatched workload parameters: {'; '.join(differences)}")
    gres = _read_resources(go_resources, go, "Go")
    rres = _read_resources(rust_resources, rust, "Rust")
    gs, rs = summarize(go), summarize(rust)
    gthrough, rthrough = gs["median_throughput_workflows_per_second"], rs["median_throughput_workflows_per_second"]
    return {
        "schema_version": 1,
        "scope": "combined Frontend+Matching end-to-end SDK workflow; same Go History/system Worker and persistence required",
        "workload": {k: go[0][k] for k in PARAMS},
        "arms": {"go": {**gs, "metadata": go_metadata}, "rust": {**rs, "metadata": rust_metadata}},
        "median_deltas": {
            "throughput_workflows_per_second": rthrough - gthrough,
            "throughput_percent": (rthrough / gthrough - 1) * 100 if gthrough else None,
            "latency_ms": {f"p{q}": rs["median_latency_ms"][f"p{q}"] - gs["median_latency_ms"][f"p{q}"] for q in (50, 95, 99)},
        },
        "paired_runs": [{"go_throughput": g["successful_workflows_per_second"], "rust_throughput": r["successful_workflows_per_second"], "go_latency_ms": g["latency_ms"], "rust_latency_ms": r["latency_ms"]} for g, r in zip(go, rust)],
        "resources": _resource_summary(gres, rres),
    }


def markdown(report):
    w = report["workload"]
    delta = report["median_deltas"]
    lines = ["# Frontend + Matching benchmark", "", f"Scope: {report['scope']}.", "", f"Workload: {w['requests']} measured workflows/run, {w['warmup']} warmup, concurrency {w['concurrency']}, payload {w['payload_bytes']} bytes, {w['transport']}.", "", "| Arm | Runs | Median throughput (workflows/s) | Median p50 (ms) | Median p95 (ms) | Median p99 (ms) |", "|---|---:|---:|---:|---:|---:|"]
    for arm in ("go", "rust"):
        s = report["arms"][arm]
        p = s["median_latency_ms"]
        lines.append(f"| {arm} | {s['repetitions']} | {s['median_throughput_workflows_per_second']:.3f} | {p['p50']:.3f} | {p['p95']:.3f} | {p['p99']:.3f} |")
    resource = report.get("resources", {"status": "not_collected"})
    lines += ["", f"Rust vs Go median throughput: {delta['throughput_percent']:+.2f}% ({delta['throughput_workflows_per_second']:+.3f} workflows/s). Latency deltas (Rust − Go): " + ", ".join(f"p{q} {delta['latency_ms'][f'p{q}']:+.3f} ms" for q in (50, 95, 99)) + ".", "", f"Resource comparison status: {resource['status']}."]
    if resource["status"] == "comparable":
        lines += ["", "## Median resources per run", "", "| Container | Go CPU (s) | Rust CPU (s) | Rust − Go CPU (s) | Go peak cgroup memory (MiB) | Rust peak cgroup memory (MiB) | Rust − Go memory (MiB) |", "|---|---:|---:|---:|---:|---:|---:|"]
        for name, go_values in resource["arms"]["go"].items():
            rust_values = resource["arms"]["rust"][name]
            delta_values = resource["rust_minus_go"][name]
            lines.append(f"| {name} | {go_values['cpu_seconds']:.3f} | {rust_values['cpu_seconds']:.3f} | {delta_values['cpu_seconds']:+.3f} | {go_values['peak_cgroup_memory_bytes'] / 1048576:.2f} | {rust_values['peak_cgroup_memory_bytes'] / 1048576:.2f} | {delta_values['peak_cgroup_memory_bytes'] / 1048576:+.2f} |")
    elif resource.get("reason"):
        lines.append(f"Resource details unavailable: {resource['reason']}.")
    lines += ["", "Raw run JSON, including successful latency samples, is preserved in the comparison JSON. These are closed-loop end-to-end measurements; SDK retries are included and client queue time is excluded. Memory reports cgroup usage, which includes charged cache and is not RSS.", ""]
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--go", nargs="+", required=True, help="Go run JSON paths in repetition order")
    ap.add_argument("--rust", nargs="+", required=True, help="paired Rust run JSON paths")
    ap.add_argument("--go-metadata", help="JSON deployment metadata (source/binary hashes/config/host)")
    ap.add_argument("--rust-metadata", help="JSON deployment metadata (source/binary hashes/config/host)")
    ap.add_argument("--go-resources", nargs="+", help="resource JSON paths in same order as Go runs")
    ap.add_argument("--rust-resources", nargs="+", help="resource JSON paths in same order as Rust runs")
    ap.add_argument("--json", required=True, help="comparison JSON output")
    ap.add_argument("--markdown", required=True, help="Markdown report output")
    ap.add_argument("--html", help="optional standalone HTML chart report output")
    args = ap.parse_args()
    load = lambda p: json.loads(Path(p).read_text(encoding="utf-8")) if p else None
    report = compare(args.go, args.rust, load(args.go_metadata), load(args.rust_metadata), args.go_resources, args.rust_resources)
    Path(args.json).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    Path(args.markdown).write_text(markdown(report), encoding="utf-8")
    if args.html:
        from render_report import render_html
        Path(args.html).write_text(render_html(report), encoding="utf-8")


if __name__ == "__main__":
    main()
