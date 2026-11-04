#!/usr/bin/env python3
"""Render a validated compare.py JSON document as a dependency-free HTML report."""
import html
import json
import math
import sys
from pathlib import Path


def _number(value, label):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{label} must be a finite number")
    return float(value)


def _validate(report):
    if not isinstance(report, dict) or report.get("schema_version") != 1:
        raise ValueError("expected comparison JSON schema_version 1")
    for key in ("scope", "workload", "arms", "paired_runs", "resources"):
        if key not in report:
            raise ValueError(f"comparison JSON missing {key!r}")
    if not isinstance(report["scope"], str) or not isinstance(report["workload"], dict):
        raise ValueError("scope must be text and workload must be an object")
    workload = report["workload"]
    for key in ("requests", "warmup", "concurrency", "payload_bytes", "transport"):
        if key not in workload:
            raise ValueError(f"workload missing {key!r}")
    arms = report["arms"]
    if not isinstance(arms, dict):
        raise ValueError("arms must be an object")
    for arm in ("go", "rust"):
        if arm not in arms or not isinstance(arms[arm], dict):
            raise ValueError(f"arms missing {arm!r} summary")
        summary = arms[arm]
        if not isinstance(summary.get("runs"), list) or not summary["runs"]:
            raise ValueError(f"{arm} runs must be a nonempty list")
        summary["_throughput"] = _number(summary.get("median_throughput_workflows_per_second"), f"{arm} median throughput")
        summary["_latency"] = {}
        for q in (50, 95, 99):
            summary["_latency"][q] = _number(summary.get("median_latency_ms", {}).get(f"p{q}"), f"{arm} p{q}")
        for index, run in enumerate(summary["runs"], 1):
            if not isinstance(run, dict):
                raise ValueError(f"{arm} run {index} must be an object")
            _number(run.get("successful_workflows_per_second"), f"{arm} run {index} throughput")
            for q in (50, 95, 99):
                _number(run.get("latency_ms", {}).get(f"p{q}"), f"{arm} run {index} p{q}")
    if len(arms["go"]["runs"]) != len(arms["rust"]["runs"]):
        raise ValueError("Go and Rust run counts differ")
    paired = report["paired_runs"]
    if not isinstance(paired, list) or len(paired) != len(arms["go"]["runs"]):
        raise ValueError("paired_runs must match the arm run count")
    resources = report["resources"]
    if not isinstance(resources, dict) or resources.get("status") not in ("comparable", "incomplete", "incomparable", "not_collected"):
        raise ValueError("resources has an unknown or missing status")
    if resources["status"] == "comparable":
        for arm in ("go", "rust"):
            if not isinstance(resources.get("arms", {}).get(arm), dict):
                raise ValueError(f"comparable resources missing {arm} values")
            for container, metrics in resources["arms"][arm].items():
                for metric in ("cpu_seconds", "peak_cgroup_memory_bytes"):
                    _number(metrics.get(metric), f"{arm} {container} {metric}")
    return report


def _esc(value):
    return html.escape(str(value), quote=True)


def _chart(title, labels, values, unit, color):
    width, height = 720, 220
    left, right, top, bottom = 58, 16, 28, 40
    plot_w, plot_h = width - left - right, height - top - bottom
    ceiling = max(values) * 1.15 if max(values) > 0 else 1
    bars = []
    slot = plot_w / max(1, len(values))
    for index, (label, value) in enumerate(zip(labels, values)):
        bar_h = value / ceiling * plot_h
        x = left + slot * index + slot * .2
        y = top + plot_h - bar_h
        bars.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{slot * .6:.1f}" height="{bar_h:.1f}" rx="4" fill="{color}"/><text class="value" x="{x + slot * .3:.1f}" y="{max(top + 12, y - 6):.1f}" text-anchor="middle">{value:.2f}</text><text x="{x + slot * .3:.1f}" y="{height - 12}" text-anchor="middle">{_esc(label)}</text>')
    return f'<section class="chart"><h3>{_esc(title)}</h3><svg viewBox="0 0 {width} {height}" role="img" aria-label="{_esc(title)}"><line x1="{left}" y1="{top + plot_h}" x2="{width-right}" y2="{top+plot_h}" stroke="#9aa7b7"/><text x="8" y="{top+8}" class="axis">{_esc(unit)}</text>{"".join(bars)}</svg></section>'


def render_html(document):
    report = _validate(json.loads(json.dumps(document)))
    arms, workload, resources = report["arms"], report["workload"], report["resources"]
    labels = ["Go", "Rust"]
    charts = [_chart("Median throughput", labels, [arms[a]["_throughput"] for a in ("go", "rust")], "workflows/s", "#3284d6")]
    for q in (50, 95, 99):
        charts.append(_chart(f"Median latency p{q}", labels, [arms[a]["_latency"][q] for a in ("go", "rust")], "ms", "#805ad5"))
    paired_html = "".join(f'<tr><td>{i}</td><td>{arms["go"]["runs"][i-1]["successful_workflows_per_second"]:.3f}</td><td>{arms["rust"]["runs"][i-1]["successful_workflows_per_second"]:.3f}</td><td>{_number(pair.get("rust_throughput"), "paired Rust throughput") - _number(pair.get("go_throughput"), "paired Go throughput"):+.3f}</td></tr>' for i, pair in enumerate(report["paired_runs"], 1))
    if resources["status"] == "comparable":
        rows = []
        for name, g in resources["arms"]["go"].items():
            r = resources["arms"]["rust"][name]
            rows.append(f'<tr><td>{_esc(name)}</td><td>{g["cpu_seconds"]:.3f}</td><td>{r["cpu_seconds"]:.3f}</td><td>{g["peak_cgroup_memory_bytes"]/1048576:.2f}</td><td>{r["peak_cgroup_memory_bytes"]/1048576:.2f}</td></tr>')
        resource_section = '<h2>Median resources per run</h2><p>Peak cgroup memory includes charged cache; it is not RSS.</p><table><thead><tr><th>Container</th><th>Go CPU (s)</th><th>Rust CPU (s)</th><th>Go peak cgroup memory (MiB)</th><th>Rust peak cgroup memory (MiB)</th></tr></thead><tbody>' + "".join(rows) + "</tbody></table>"
    else:
        reason = resources.get("reason", "No resource samples were collected for both arms.")
        resource_section = f'<h2>Resources</h2><p>Comparison status: <strong>{_esc(resources["status"])}</strong>. {_esc(reason)}</p>'
    metadata = []
    for arm in ("go", "rust"):
        value = arms[arm].get("metadata")
        if value is not None:
            metadata.append(f'<section><h3>{arm.title()} deployment metadata</h3><pre>{_esc(json.dumps(value, indent=2, sort_keys=True))}</pre></section>')
    return f'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Cadence Frontend + Matching benchmark</title><style>
*{{box-sizing:border-box}}body{{margin:0;background:#f4f7fb;color:#182334;font:16px/1.5 system-ui,sans-serif}}main{{max-width:1100px;margin:32px auto;padding:0 20px}}header,.panel{{background:white;border:1px solid #dce4ee;border-radius:12px;padding:22px;margin-bottom:18px}}h1{{margin:0 0 8px}}h2{{margin-top:0}}.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(330px,1fr));gap:16px}}.chart{{background:#fff;border:1px solid #dce4ee;border-radius:12px;padding:16px;overflow:hidden}}.chart h3{{margin:0}}svg{{width:100%;height:auto}}svg text{{font:12px system-ui,sans-serif;fill:#46576b}}svg .value{{font-weight:700;fill:#182334}}svg .axis{{font-size:10px}}table{{width:100%;border-collapse:collapse}}td,th{{padding:9px;border-bottom:1px solid #e5ebf2;text-align:right}}td:first-child,th:first-child{{text-align:left}}pre{{white-space:pre-wrap;overflow-wrap:anywhere;background:#f4f7fb;padding:12px;border-radius:8px}}.muted{{color:#52647a}}</style></head><body><main><header><h1>Frontend + Matching benchmark</h1><p>{_esc(report['scope'])}</p><p class="muted">Workload: {workload['requests']} measured workflows/run; {workload['warmup']} warmup; concurrency {workload['concurrency']}; payload {workload['payload_bytes']} bytes; transport {_esc(workload['transport'])}; {len(arms['go']['runs'])} paired runs.</p></header><div class="grid">{"".join(charts)}</div><section class="panel"><h2>Paired run throughput</h2><table><thead><tr><th>Pair</th><th>Go workflows/s</th><th>Rust workflows/s</th><th>Rust − Go</th></tr></thead><tbody>{paired_html}</tbody></table><p>Closed-loop end-to-end SDK measurements include retries and exclude client queue time. The scope uses the existing Go History and the shared system Worker/persistence described above.</p></section><section class="panel">{resource_section}</section>{''.join(f'<section class="panel">{item}</section>' for item in metadata)}<p class="muted">Values are read from the comparison JSON; no measurements are inferred or generated by this report.</p></main></body></html>'''


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 2:
        raise SystemExit("usage: render_report.py comparison.json report.html")
    try:
        report = json.loads(Path(args[0]).read_text(encoding="utf-8"))
        Path(args[1]).write_text(render_html(report), encoding="utf-8")
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        raise SystemExit(f"render_report.py: {exc}") from exc


if __name__ == "__main__":
    main()
