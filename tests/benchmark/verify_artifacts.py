#!/usr/bin/env python3
"""Verify provenance and completeness of paired Frontend+Matching benchmark artifacts."""
import argparse
import datetime as dt
import json
import math
import re
import shlex
import sys
from pathlib import Path


class InvalidEvidence(ValueError):
    pass


def _read(path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise InvalidEvidence(f"{path.name}: cannot read valid JSON ({exc})") from exc


def _finite_nonnegative(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0


def _utc(value, where):
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("timezone missing")
        return parsed.astimezone(dt.timezone.utc)
    except (AttributeError, TypeError, ValueError) as exc:
        raise InvalidEvidence(f"{where}: invalid UTC timestamp {value!r}") from exc


def _source(path, expected):
    values = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            values[key] = value.strip()
    if values.get("head") != expected:
        raise InvalidEvidence(f"source.txt head {values.get('head')!r} does not match expected {expected!r}")
    if not re.fullmatch(r"[0-9a-fA-F]{40,64}", expected):
        raise InvalidEvidence("--expected-head must be a full Git object SHA")
    for key in ("go_image", "rust_image"):
        if not values.get(key):
            raise InvalidEvidence(f"source.txt missing {key}")
    return values


def _commands(out):
    path = out / "commands.json"
    try:
        events = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise InvalidEvidence(f"commands.json: cannot read valid command evidence ({exc})") from exc
    if not isinstance(events, list) or not events:
        raise InvalidEvidence("commands.json must contain completed command records")
    by_name = {}
    for event in events:
        if not isinstance(event, dict):
            raise InvalidEvidence("commands.json contains a non-object command record")
        name = event.get("name")
        if (not isinstance(name, str) or not isinstance(event.get("command"), str) or not event["command"]
                or not isinstance(event.get("exit_code"), int) or isinstance(event.get("exit_code"), bool)
                or not isinstance(event.get("log"), str) or not event["log"]
                or not isinstance(event.get("started_unix"), (int, float))
                or isinstance(event.get("started_unix"), bool) or not math.isfinite(event["started_unix"])
                or not _finite_nonnegative(event.get("elapsed_seconds"))):
            raise InvalidEvidence("commands.json contains an incomplete command record")
        if name in by_name:
            raise InvalidEvidence(f"commands.json duplicates command {name}")
        by_name[name] = event
    failed = [name for name, e in by_name.items()
              if (name.startswith("run-") or name.startswith("collector-")) and e["exit_code"] != 0]
    if failed:
        raise InvalidEvidence("nonzero command/collector exits: " + ", ".join(failed))
    if by_name.get("cleanup", {}).get("exit_code") != 0 or "cleanup" not in by_name:
        raise InvalidEvidence("final cleanup command is missing or failed")
    _require_compose_down(by_name["cleanup"], "cleanup")
    return by_name


def _require_compose_down(event, name):
    try:
        tokens = shlex.split(event["command"])
    except ValueError as exc:
        raise InvalidEvidence(f"{name}: malformed shell command evidence") from exc
    tail = tokens[-7:]
    if len(tail) != 7 or not tail[0].endswith("docker") or tail[1:] != ["compose", "-p", "cadence-fm-benchmark", "down", "-v", "--remove-orphans"]:
        raise InvalidEvidence(f"{name}: command is not the required compose down -v cleanup")


def _event_tokens(event, name):
    try:
        return shlex.split(event["command"])
    except ValueError as exc:
        raise InvalidEvidence(f"{name}: malformed shell command evidence") from exc


def _validate_compose_config(path, expected_images):
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise InvalidEvidence(f"{path.name}: missing resolved Compose configuration") from exc
    service = None
    images = {}
    for line in lines:
        match = re.match(r"^  ([a-zA-Z0-9_-]+):\s*$", line)
        if match:
            service = match.group(1)
            continue
        if service and line.startswith("    image:"):
            images[service] = line.split(":", 1)[1].strip().strip("\"'")
    for service, image in expected_images.items():
        if images.get(service) != image:
            raise InvalidEvidence(f"{path.name}: {service} image does not match expected {image}")


def _flag(tokens, flag, where):
    try:
        return tokens[tokens.index(flag) + 1]
    except (ValueError, IndexError) as exc:
        raise InvalidEvidence(f"{where}: command missing {flag}") from exc


def _check_hash_file(path, label):
    try:
        lines = [line.split()[0].lower() for line in path.read_text(encoding="ascii").splitlines() if line.split()]
    except (OSError, IndexError, UnicodeError) as exc:
        raise InvalidEvidence(f"{label}: missing or empty hash record") from exc
    if not lines or any(not re.fullmatch(r"[0-9a-f]{64}", value) for value in lines):
        raise InvalidEvidence(f"{label}: expected SHA256 hex digest records")
    return lines


def verify(out_root, expected_head, tolerance=1.0):
    root = Path(out_root)
    if not root.is_dir():
        raise InvalidEvidence(f"artifact root does not exist: {root}")
    out = root / "paired-benchmark" if (root / "paired-benchmark").is_dir() else root
    source = _source(out / "source.txt", expected_head)
    host = _read(out / "host.json")
    if not isinstance(host, dict):
        raise InvalidEvidence("host.json must contain a JSON object")
    reps, concurrency = host.get("repetitions"), host.get("concurrency")
    if not isinstance(reps, int) or isinstance(reps, bool) or reps < 1:
        raise InvalidEvidence("host.json repetitions must be a positive integer")
    if (not isinstance(concurrency, list) or not concurrency
            or any(not isinstance(c, int) or isinstance(c, bool) or c < 1 for c in concurrency)
            or len(set(concurrency)) != len(concurrency)):
        raise InvalidEvidence("host.json concurrency must be a nonempty list of positive integers")
    commands = _commands(out)
    for key, minimum in (("requests", 1), ("warmup", 0), ("payload_bytes", 0)):
        value = host.get(key)
        if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
            raise InvalidEvidence(f"host.json {key} must be an integer >= {minimum}")
    warnings = []
    if reps < 5:
        warnings.append(f"smoke evidence: {reps} repetitions; fewer than 5 is not a full benchmark")

    go_hashes = _check_hash_file(root / "go-binary.sha256", "go-binary.sha256")
    rust_hashes = _check_hash_file(root / "rust-binary.sha256", "rust-binary.sha256")
    drivers_hashes = _check_hash_file(root / "drivers.sha256", "drivers.sha256")
    if len(rust_hashes) < 2 or rust_hashes[0] != rust_hashes[-1]:
        raise InvalidEvidence("Dockerfile-recorded Rust binary hash differs from installed image binary hash")
    go_hash, rust_hash = go_hashes[-1], rust_hashes[-1]

    # Import the canonical workload/sample validator shipped beside this verifier.
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    try:
        from compare import compare
    except ImportError:
        compare = None  # master-based development checkout; the integration branch supplies compare.py

    verified = []
    database_ids, database_id_by_stem, database_image_ids, volume_owners = set(), {}, set(), {}
    for conc in concurrency:
        go_results, rust_results, go_resources, rust_resources = [], [], [], []
        for rep in range(reps):
            for arm in ("go", "rust"):
                stem = f"{rep:02d}-{arm}"
                for teardown in (f"down-{stem}", f"down-after-{stem}"):
                    event = commands.get(teardown)
                    if not event:
                        raise InvalidEvidence(f"commands.json missing fresh-database teardown {teardown}")
                    if event["exit_code"] != 0:
                        raise InvalidEvidence(f"{teardown}: teardown command failed")
                    _require_compose_down(event, teardown)
                arm_tag = "cadence-rust:ci" if arm == "rust" else "cadence-go:ci"
                _validate_compose_config(out / f"compose-{stem}.yaml",
                                         {"frontend": arm_tag, "matching": arm_tag,
                                          "history": "cadence-go:ci", "worker": "cadence-go:ci",
                                          "mysql": "mysql:8.0"})
                identity = _read(out / f"identities-{stem}.json")
                if not isinstance(identity, dict):
                    raise InvalidEvidence(f"identities-{stem}.json must contain a JSON object")
                expected = {"frontend": source["go_image" if arm == "go" else "rust_image"],
                            "matching": source["go_image" if arm == "go" else "rust_image"],
                            "history": source["go_image"], "worker": source["go_image"]}
                names = set()
                ids = set()
                for service, image in expected.items():
                    item = identity.get(service)
                    if not isinstance(item, dict) or item.get("image_id") != image:
                        raise InvalidEvidence(f"{stem} {service}: immutable image_id does not match source.txt")
                    if item.get("image") != ("cadence-rust:ci" if image == source["rust_image"] else "cadence-go:ci"):
                        raise InvalidEvidence(f"{stem} {service}: image tag contradicts arm/source")
                    if (not isinstance(item.get("container_name"), str) or not item["container_name"]
                            or not isinstance(item.get("container_id"), str) or not item["container_id"]):
                        raise InvalidEvidence(f"{stem} {service}: missing container identity")
                    names.add(item["container_name"].lstrip("/"))
                    ids.add(item["container_id"])
                if len(names) != 4 or len(ids) != 4:
                    raise InvalidEvidence(f"{stem}: duplicate service container names or IDs")
                mysql = identity.get("mysql")
                if not isinstance(mysql, dict) or not isinstance(mysql.get("Id"), str) or not mysql["Id"]:
                    raise InvalidEvidence(f"{stem}: missing MySQL inspect identity")
                if (not isinstance(mysql.get("Config"), dict)
                        or mysql["Config"].get("Image") != "mysql:8.0"
                        or not isinstance(mysql["Config"].get("Env"), list)
                        or "MYSQL_ROOT_PASSWORD=root" not in mysql["Config"]["Env"]):
                    raise InvalidEvidence(f"{stem}: MySQL inspect image is missing or is not mysql:8.0")
                if not isinstance(mysql.get("Image"), str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", mysql["Image"]):
                    raise InvalidEvidence(f"{stem}: MySQL immutable image ID is missing")
                database_image_ids.add(mysql["Image"])
                db_id = mysql["Id"]
                if stem in database_id_by_stem and database_id_by_stem[stem] != db_id:
                    raise InvalidEvidence(f"{stem}: MySQL identity changes across concurrency levels")
                if stem not in database_id_by_stem and db_id in database_ids:
                    raise InvalidEvidence(f"{stem}: MySQL container Id is reused across arms/repetitions")
                database_id_by_stem[stem] = db_id
                database_ids.add(db_id)
                if "Mounts" not in mysql:
                    raise InvalidEvidence(f"{stem}: MySQL inspect record is missing Mounts")
                mounts = mysql["Mounts"]
                if not isinstance(mounts, list) or any(not isinstance(mount, dict) for mount in mounts):
                    raise InvalidEvidence(f"{stem}: MySQL Mounts must be a list of objects")
                for mount in mounts:
                    if not all(isinstance(mount.get(k), str) and mount.get(k) for k in ("Type", "Source", "Destination")):
                        raise InvalidEvidence(f"{stem}: MySQL mount record lacks type/source/destination")
                    sources = ((mount.get("Name"), mount.get("Source")) if mount.get("Type") == "volume"
                               else (mount.get("Source"),))
                    for source_name in filter(None, sources):
                        prior = volume_owners.get(source_name)
                        if prior is not None and prior != stem:
                            raise InvalidEvidence(f"{stem}: MySQL data volume is shared with {prior}")
                        volume_owners[source_name] = stem
                result_path = out / f"result-{stem}-c{conc}.json"
                resource_path = out / f"resources-{stem}-c{conc}.json"
                result = _read(result_path)
                resource = _read(resource_path)
                if not isinstance(result, dict):
                    raise InvalidEvidence(f"{result_path.name}: expected a JSON object")
                if not isinstance(resource, dict) or not isinstance(resource.get("containers"), dict):
                    raise InvalidEvidence(f"{resource_path.name}: expected object with a containers object")
                if (not isinstance(result.get("failed"), int) or isinstance(result.get("failed"), bool)
                        or not isinstance(result.get("errors"), list)
                        or len(result["errors"]) != result["failed"]):
                    raise InvalidEvidence(f"{result_path.name}: error list/count is missing or contradictory")
                for command_name in (f"run-{stem}-c{conc}", f"collector-{stem}-c{conc}"):
                    if command_name not in commands:
                        raise InvalidEvidence(f"commands.json missing {command_name}")
                run_name, collector_name = f"run-{stem}-c{conc}", f"collector-{stem}-c{conc}"
                run_tokens, collector_tokens = _event_tokens(commands[run_name], run_name), _event_tokens(commands[collector_name], collector_name)
                if not any(token.endswith("cadence-benchmark") for token in run_tokens):
                    raise InvalidEvidence(f"{run_name}: command does not invoke cadence-benchmark")
                expected_flags = {"-label": arm, "-requests": str(host["requests"]),
                                  "-warmup": str(host["warmup"]), "-concurrency": str(conc),
                                  "-payload-bytes": str(host["payload_bytes"])}
                for flag, value in expected_flags.items():
                    if _flag(run_tokens, flag, run_name) != value:
                        raise InvalidEvidence(f"{run_name}: {flag} contradicts host/run metadata")
                if not _flag(run_tokens, "-output", run_name).endswith(f"result-{stem}-c{conc}.json"):
                    raise InvalidEvidence(f"{run_name}: output path does not identify its result artifact")
                marker = f"measurement-{stem}-c{conc}"
                for flag, suffix in (("-measurement-start-file", ".start"),
                                     ("-measurement-end-file", ".end"),
                                     ("-collector-ready-file", ".ready")):
                    if not _flag(run_tokens, flag, run_name).endswith(marker + suffix):
                        raise InvalidEvidence(f"{run_name}: {flag} does not match this measurement")
                if not any(token.endswith("collect_resources.py") for token in collector_tokens):
                    raise InvalidEvidence(f"{collector_name}: command does not invoke collect_resources.py")
                if not _flag(collector_tokens, "--output", collector_name).endswith(f"resources-{stem}-c{conc}.json"):
                    raise InvalidEvidence(f"{collector_name}: output path does not identify its resource artifact")
                for flag, suffix in (("--start-event", ".start"), ("--end-event", ".end"), ("--ready-event", ".ready")):
                    if not _flag(collector_tokens, flag, collector_name).endswith(marker + suffix):
                        raise InvalidEvidence(f"{collector_name}: {flag} does not match this measurement")
                container_flag = collector_tokens.index("--containers") + 1 if "--containers" in collector_tokens else len(collector_tokens)
                if set(collector_tokens[container_flag:]) != names:
                    raise InvalidEvidence(f"{collector_name}: collected container names disagree with inspect identities")
                if {x.lstrip("/") for x in resource["containers"]} != names:
                    raise InvalidEvidence(f"{resource_path.name}: collected container names disagree with identities")
                for key, expected_value in (("requests", host["requests"]), ("warmup", host["warmup"]),
                                            ("concurrency", conc), ("payload_bytes", host["payload_bytes"])):
                    if result.get(key) != expected_value:
                        raise InvalidEvidence(f"{result_path.name}: {key} contradicts host or filename metadata")
                started = _utc(resource.get("started_at_utc"), resource_path.name)
                ended = _utc(resource.get("ended_at_utc"), resource_path.name)
                measured_start = _utc(result.get("measured_start_utc"), result_path.name)
                measured_end = _utc(result.get("measured_end_utc"), result_path.name)
                if started > ended or measured_start > measured_end:
                    raise InvalidEvidence(f"{stem} c{conc}: measurement timestamps are reversed")
                if abs((started - measured_start).total_seconds()) > tolerance or abs((ended - measured_end).total_seconds()) > tolerance:
                    raise InvalidEvidence(f"{stem} c{conc}: SDK measurement timestamps outside {tolerance}s marker tolerance")
                duration = resource.get("duration_seconds")
                if not _finite_nonnegative(duration) or duration <= 0:
                    raise InvalidEvidence(f"{resource_path.name}: invalid duration")
                timestamp_duration = (ended - started).total_seconds()
                if abs(duration - timestamp_duration) > tolerance:
                    raise InvalidEvidence(f"{resource_path.name}: duration contradicts collector timestamps")
                for name, metrics in resource["containers"].items():
                    if not isinstance(metrics, dict):
                        raise InvalidEvidence(f"{resource_path.name}: metrics for {name} must be an object")
                    cpu, initial, final = metrics.get("cpu_seconds"), metrics.get("initial_cpu_ns"), metrics.get("final_cpu_ns")
                    if any(not _finite_nonnegative(metrics.get(k)) for k in ("cpu_seconds", "average_cpu_cores", "peak_cgroup_memory_bytes")):
                        raise InvalidEvidence(f"{resource_path.name}: non-finite/negative metric for {name}")
                    if isinstance(initial, bool) or isinstance(final, bool) or not isinstance(initial, int) or not isinstance(final, int) or final < initial:
                        raise InvalidEvidence(f"{resource_path.name}: invalid cgroup CPU counters for {name}")
                    counter_seconds = (final - initial) / 1_000_000_000
                    if not math.isclose(cpu, counter_seconds, rel_tol=1e-6, abs_tol=1e-6):
                        raise InvalidEvidence(f"{resource_path.name}: CPU seconds disagree with cgroup counter delta for {name}")
                    if not math.isclose(metrics["average_cpu_cores"], cpu / duration, rel_tol=1e-5, abs_tol=1e-6):
                        raise InvalidEvidence(f"{resource_path.name}: average CPU cores disagree with CPU seconds/duration for {name}")
                (go_results if arm == "go" else rust_results).append(result_path)
                (go_resources if arm == "go" else rust_resources).append(resource_path)
        if compare:
            try:
                report = compare(go_results, rust_results, host, host, go_resources, rust_resources)
            except (ValueError, KeyError, TypeError) as exc:
                raise InvalidEvidence(f"c{conc}: workload/sample/resource comparison failed: {exc}") from exc
            if report["resources"]["status"] != "comparable":
                raise InvalidEvidence(f"c{conc}: resource samples are not comparable")
        else:
            # Minimal fallback for synthetic development checkouts where the
            # integration branch's canonical compare.py is not present.
            for arm, paths in (("go", go_results), ("rust", rust_results)):
                runs = [_read(p) for p in paths]
                for run in runs:
                    samples = run.get("successful_latency_samples_ms")
                    if (run.get("label") != arm or run.get("failed") != 0
                            or run.get("successful") != run.get("requests")
                            or not isinstance(samples, list) or len(samples) != run.get("successful")
                            or any(not _finite_nonnegative(x) for x in samples)):
                        raise InvalidEvidence(f"c{conc}: invalid raw workload/error/sample evidence for {arm}")
                if runs and any(any(r.get(key) != runs[0].get(key) for key in
                                    ("requests", "warmup", "concurrency", "payload_bytes", "transport", "sticky_execution")) for r in runs[1:]):
                    raise InvalidEvidence(f"c{conc}: {arm} repetitions have mismatched workload")
            params = ("requests", "warmup", "concurrency", "payload_bytes", "transport", "sticky_execution")
            for go_path, rust_path in zip(go_results, rust_results):
                go_run, rust_run = _read(go_path), _read(rust_path)
                if any(go_run.get(key) != rust_run.get(key) for key in params):
                    raise InvalidEvidence(f"c{conc}: paired Go/Rust workloads differ")
            resource_sets = [set(_read(p)["containers"]) for p in go_resources + rust_resources]
            if any(names != resource_sets[0] for names in resource_sets[1:]):
                raise InvalidEvidence(f"c{conc}: resource samples are not comparable")
        verified.append({"concurrency": conc, "paired_repetitions": reps, "status": "valid"})
    if len(database_image_ids) != 1:
        raise InvalidEvidence("MySQL image ID differs across arms or repetitions")
    return {"schema_version": 1, "valid": True, "expected_head": expected_head,
            "repetitions": reps, "concurrency": concurrency, "binaries": {"go_sha256": go_hash, "rust_sha256": rust_hash},
            "drivers_sha256": drivers_hashes, "verified": verified, "warnings": warnings}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, help="downloaded out/paired-benchmark directory")
    parser.add_argument("--expected-head", required=True, help="immutable integration commit SHA")
    parser.add_argument("--marker-tolerance-seconds", type=float, default=1.0)
    args = parser.parse_args(argv)
    if args.marker_tolerance_seconds < 0 or not math.isfinite(args.marker_tolerance_seconds):
        parser.error("marker tolerance must be finite and nonnegative")
    try:
        report = verify(args.out, args.expected_head, args.marker_tolerance_seconds)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    except (InvalidEvidence, OSError, KeyError, TypeError, AttributeError) as exc:
        print(json.dumps({"schema_version": 1, "valid": False, "error": str(exc)}, indent=2, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
