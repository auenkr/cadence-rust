#!/usr/bin/env python3
"""Validate SDK benchmark runs and produce paired JSON/Markdown reports."""
import argparse
import json
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
    if len(run["successful_latency_samples_ms"]) != run["successful"] or run["elapsed_seconds"] <= 0:
        raise ValueError(f"{source}: incomplete latency samples or invalid elapsed time")
    return run


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


def compare(go_paths, rust_paths, go_metadata=None, rust_metadata=None):
    if not go_paths or len(go_paths) != len(rust_paths):
        raise ValueError("Go and Rust require the same nonzero number of runs")
    go = [read_run(p, "go") for p in go_paths]
    rust = [read_run(p, "rust") for p in rust_paths]
    for i, (g, r) in enumerate(zip(go, rust), 1):
        differences = [f"{k}: Go={g[k]!r}, Rust={r[k]!r}" for k in PARAMS if g[k] != r[k]]
        if differences:
            raise ValueError(f"pair {i} has mismatched workload parameters: {'; '.join(differences)}")
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
    }


def markdown(report):
    w = report["workload"]
    delta = report["median_deltas"]
    lines = ["# Frontend + Matching benchmark", "", f"Scope: {report['scope']}.", "", f"Workload: {w['requests']} measured workflows/run, {w['warmup']} warmup, concurrency {w['concurrency']}, payload {w['payload_bytes']} bytes, {w['transport']}.", "", "| Arm | Runs | Median throughput (workflows/s) | Median p50 (ms) | Median p95 (ms) | Median p99 (ms) |", "|---|---:|---:|---:|---:|---:|"]
    for arm in ("go", "rust"):
        s = report["arms"][arm]
        p = s["median_latency_ms"]
        lines.append(f"| {arm} | {s['repetitions']} | {s['median_throughput_workflows_per_second']:.3f} | {p['p50']:.3f} | {p['p95']:.3f} | {p['p99']:.3f} |")
    lines += ["", f"Rust vs Go median throughput: {delta['throughput_percent']:+.2f}% ({delta['throughput_workflows_per_second']:+.3f} workflows/s). Latency deltas (Rust − Go): " + ", ".join(f"p{q} {delta['latency_ms'][f'p{q}']:+.3f} ms" for q in (50, 95, 99)) + ".", "", "Raw run JSON, including successful latency samples, is preserved in the comparison JSON. These are closed-loop end-to-end measurements; SDK retries are included and client queue time is excluded. Resource comparisons require per-service CPU/RSS samples and equivalent deployment metadata.", ""]
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--go", nargs="+", required=True, help="Go run JSON paths in repetition order")
    ap.add_argument("--rust", nargs="+", required=True, help="paired Rust run JSON paths")
    ap.add_argument("--go-metadata", help="JSON deployment metadata (source/binary hashes/config/host)")
    ap.add_argument("--rust-metadata", help="JSON deployment metadata (source/binary hashes/config/host)")
    ap.add_argument("--json", required=True, help="comparison JSON output")
    ap.add_argument("--markdown", required=True, help="Markdown report output")
    args = ap.parse_args()
    load = lambda p: json.loads(Path(p).read_text(encoding="utf-8")) if p else None
    report = compare(args.go, args.rust, load(args.go_metadata), load(args.rust_metadata))
    Path(args.json).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    Path(args.markdown).write_text(markdown(report), encoding="utf-8")


if __name__ == "__main__":
    main()
