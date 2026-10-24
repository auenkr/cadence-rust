#!/usr/bin/env python3
"""Compare Go Matching metric definitions/allowlists with Rust name inventory."""

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
GO_DEFS = ROOT / "go/common/metrics/defs.go"
GO_CONFIG = ROOT / "go/common/metrics/config.go"
GO_MATCHING = ROOT / "go/service/matching"
RUST_CRATES = ROOT / "rust/crates"
RUST_MIGRATION = ROOT / "rust/crates/cadence-service-core/src/matching_metrics_migration.rs"


def allowlist(source: str, go_name: str, rust_name: str, rust_source: str):
    go_section = source.split(f"var {go_name}", 1)[1].split("\n}", 1)[0]
    go_names = set(re.findall(r'"([a-z0-9_]+)"\s*:\s*\{\}', go_section))
    rust_section = rust_source.split(f"pub const {rust_name}", 1)[1].split("\n];", 1)[0]
    rust_names = set(re.findall(r'"([a-z0-9_]+)"', rust_section))
    return go_names, rust_names


def main():
    go_defs = GO_DEFS.read_text()
    go_config = GO_CONFIG.read_text()
    rust_migration = RUST_MIGRATION.read_text()
    rust_sources = "\n".join(path.read_text(errors="ignore") for path in RUST_CRATES.rglob("*.rs"))

    mismatches = []
    for go_name, rust_name in [
        ("HistogramMigrationMetrics", "HISTOGRAM_MIGRATION_NAMES"),
        ("GaugeMigrationMetrics", "GAUGE_MIGRATION_NAMES"),
        ("CounterMigrationMetrics", "COUNTER_MIGRATION_NAMES"),
    ]:
        go_names, rust_names = allowlist(go_config, go_name, rust_name, rust_migration)
        missing, extra = go_names - rust_names, rust_names - go_names
        print(f"{go_name}: Go={len(go_names)} Rust={len(rust_names)} missing={sorted(missing)} extra={sorted(extra)}")
        mismatches.extend((go_name, name) for name in missing | extra)

    definitions_start = go_defs.index("MetricDefs =")
    matching = go_defs[definitions_start:].split("\n\tMatching: {", 1)[1].split("\n\t},\n\tWorker:", 1)[0]
    definitions = []
    for line in matching.splitlines():
        found = re.search(r'^\s*([A-Za-z0-9]+):\s*\{metricName: "([a-z0-9_]+)"([^}]*)\}', line)
        if found:
            ident, name, attributes = found.groups()
            metric_type = re.search(r"metricType: (Counter|Gauge|Timer|Histogram)", attributes)
            rollup = re.search(r'metricRollupName: "([a-z0-9_]+)"', attributes)
            definitions.append((ident, name, metric_type.group(1) if metric_type else "Counter", rollup.group(1) if rollup else None))

    go_files = [path for path in GO_MATCHING.rglob("*.go") if not path.name.endswith("_test.go")]
    rust_metric_names = set(re.findall(r'"([a-z0-9_]+)"', rust_sources))
    active, catalog_only, missing = [], [], []
    for ident, name, metric_type, rollup in definitions:
        has_production_callsite = any(re.search(rf"\b{re.escape(ident)}\b", path.read_text(errors="ignore")) for path in go_files)
        (active if has_production_callsite else catalog_only).append((ident, name, metric_type, rollup))
        if has_production_callsite and name not in rust_metric_names:
            missing.append((ident, name))

    counts = {kind: sum(1 for _, _, actual, _ in active if actual == kind) for kind in ("Counter", "Gauge", "Timer", "Histogram")}
    print(f"Matching definitions={len(definitions)}; Go production callsites={len(active)}; catalog-only={len(catalog_only)}")
    print(f"Active definition types={counts}; active names missing from Rust source inventory={missing}")
    print("Catalog-only Go definitions=" + ", ".join(f"{ident}:{name}" for ident, name, _, _ in catalog_only))
    if mismatches or missing:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
