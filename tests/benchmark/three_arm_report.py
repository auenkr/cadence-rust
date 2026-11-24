#!/usr/bin/env python3
"""Three-arm (go / control / rust candidate) paired report: per-concurrency medians and candidate/control ratios."""
import json
import statistics
from pathlib import Path

ARMS = ("go", "control", "rust")


def _runs(out, arm, conc, reps):
    runs = []
    for rep in range(reps):
        p = Path(out) / f"result-{rep:02d}-{arm}-c{conc}.json"
        run = json.loads(p.read_text())
        if run.get("failed") != 0 or run.get("successful") != run.get("requests"):
            raise ValueError(f"{p}: failed or incomplete run")
        runs.append(run)
    return runs


def build(out, reps, concurrency):
    report = {"arms": list(ARMS), "repetitions": reps, "concurrency": {}}
    for conc in concurrency:
        by = {a: _runs(out, a, conc, reps) for a in ARMS}
        entry = {}
        for a, runs in by.items():
            entry[a] = {
                "median_throughput": statistics.median(r["successful_workflows_per_second"] for r in runs),
                "median_p50_ms": statistics.median(r["latency_ms"]["p50"] for r in runs),
                "median_p99_ms": statistics.median(r["latency_ms"]["p99"] for r in runs),
                "throughput_per_rep": [r["successful_workflows_per_second"] for r in runs],
            }
        tp = [c["successful_workflows_per_second"] / k["successful_workflows_per_second"]
              for c, k in zip(by["rust"], by["control"])]
        p50 = [c["latency_ms"]["p50"] / k["latency_ms"]["p50"] for c, k in zip(by["rust"], by["control"])]
        entry["candidate_over_control"] = {
            "throughput_ratio_median": statistics.median(tp), "throughput_ratio_min": min(tp),
            "throughput_ratio_max": max(tp), "throughput_ratio_per_rep": tp,
            "p50_latency_ratio_median": statistics.median(p50),
            "p50_latency_ratio_min": min(p50), "p50_latency_ratio_max": max(p50)}
        report["concurrency"][str(conc)] = entry
    return report


def markdown(report):
    lines = ["# Three-arm paired benchmark", "",
             f"Repetitions: {report['repetitions']} (interleaved, rotated order, same runner)", "",
             "| conc | go tput | control tput | candidate tput | cand/control tput (median [min,max]) | cand/control p50 latency (median [min,max]) |",
             "|---|---|---|---|---|---|"]
    for conc, e in report["concurrency"].items():
        r = e["candidate_over_control"]
        lines.append(f"| {conc} | {e['go']['median_throughput']:.1f} | {e['control']['median_throughput']:.1f} | "
                     f"{e['rust']['median_throughput']:.1f} | {r['throughput_ratio_median']:.3f} "
                     f"[{r['throughput_ratio_min']:.3f}, {r['throughput_ratio_max']:.3f}] | "
                     f"{r['p50_latency_ratio_median']:.3f} [{r['p50_latency_ratio_min']:.3f}, {r['p50_latency_ratio_max']:.3f}] |")
    lines += ["", "Ratio spread (min/max across reps) is the confidence indication: a change is resolvable only if it lies outside that range."]
    return "\n".join(lines) + "\n"


def write(out, reps, concurrency):
    report = build(out, reps, concurrency)
    Path(out, "three-arm-report.json").write_text(json.dumps(report, indent=2) + "\n")
    Path(out, "three-arm-report.md").write_text(markdown(report))
    return report
