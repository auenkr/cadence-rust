#!/usr/bin/env python3
"""Compare checked-in provider contract data with pinned Go implementations."""
import json
import pathlib
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/ringpop_bootstrap_provider.json"
DNS_FIXTURE = ROOT / "tests/fixtures/ringpop_dns_provider.json"

overlay_test = ROOT / "tests/go-overlay/common/peerprovider/ringpopprovider/bootstrap_oracle_test.go"
go_test_path = ROOT / "go/common/peerprovider/ringpopprovider/bootstrap_oracle_test.go"
with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as overlay:
    json.dump({"Replace": {str(go_test_path): str(overlay_test)}}, overlay)
    overlay.flush()
    proc = subprocess.run(
        ["go", "test", f"-overlay={overlay.name}", "./common/peerprovider/ringpopprovider", "-run", "^Test(BootstrapProviderActualOracle|DiscoveryDNSActualOracle|SystemLookupHostOracle)$", "-count=1", "-v"],
        cwd=ROOT / "go", text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
sys.stdout.write(proc.stdout)
if proc.returncode:
    raise SystemExit(proc.returncode)
line = next((line for line in proc.stdout.splitlines() if line.startswith("BOOTSTRAP_PROVIDER_ORACLE=")), None)
if line is None:
    raise SystemExit("actual Go provider oracle emitted no result")
actual = json.loads(line.split("=", 1)[1])
expected = json.loads(FIXTURE.read_text())
if actual != expected:
    print("Go provider output differs from checked-in fixture", file=sys.stderr)
    print(json.dumps({"expected": expected, "actual": actual}, indent=2), file=sys.stderr)
    raise SystemExit(1)
print(f"actual Go provider output matches {FIXTURE.relative_to(ROOT)}")
dns_line = next((line for line in proc.stdout.splitlines() if line.startswith("DNS_PROVIDER_ORACLE=")), None)
if dns_line is None:
    raise SystemExit("actual Go DNS provider oracle emitted no result")
dns_actual = json.loads(dns_line.split("=", 1)[1])
dns_expected = json.loads(DNS_FIXTURE.read_text())
if dns_actual != dns_expected:
    print("Go DNS provider output differs from checked-in fixture", file=sys.stderr)
    print(json.dumps({"expected": dns_expected, "actual": dns_actual}, indent=2), file=sys.stderr)
    raise SystemExit(1)
print(f"actual Go DNS provider output matches {DNS_FIXTURE.relative_to(ROOT)}")
