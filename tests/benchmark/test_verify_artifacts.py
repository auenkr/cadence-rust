"""Synthetic verifier fixtures; none of these files represent benchmark results."""
import datetime as dt
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from verify_artifacts import InvalidEvidence, verify


HEAD = "a" * 40
GO = "sha256:" + "1" * 64
RUST = "sha256:" + "2" * 64


def dump(path, value):
    path.write_text(json.dumps(value), encoding="utf-8")


class VerifyArtifactsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.out = Path(self.temp.name)
        (self.out / "source.txt").write_text(f"head={HEAD}\ngo_image={GO}\nrust_image={RUST}\n", encoding="utf-8")
        dump(self.out / "host.json", {"repetitions": 2, "concurrency": [1],
                                       "requests": 1, "warmup": 0, "payload_bytes": 0})
        (self.out / "go-binary.sha256").write_text("1" * 64 + "  cadence-go\n")
        (self.out / "rust-binary.sha256").write_text("2" * 64 + "  Dockerfile\n" + "2" * 64 + "  installed-image\n")
        (self.out / "drivers.sha256").write_text("3" * 64 + "  driver\n")
        events = []
        for rep in range(2):
          for arm in ("go", "rust"):
            stem = f"{rep:02d}-{arm}"
            services = {}
            for service in ("frontend", "matching", "history", "worker"):
                image_id = RUST if arm == "rust" and service in ("frontend", "matching") else GO
                services[service] = {"image": "cadence-rust:ci" if image_id == RUST else "cadence-go:ci",
                                     "image_id": image_id, "container_id": f"id-{stem}-{service}",
                                     "container_name": service}
            services["mysql"] = {"Id": f"db-{stem}", "Image": "sha256:" + "3" * 64,
                                 "Config": {"Image": "mysql:8.0", "Env": ["MYSQL_ROOT_PASSWORD=root"]},
                                 "Mounts": []}
            dump(self.out / f"identities-{stem}.json", services)
            service_tag = "cadence-rust:ci" if arm == "rust" else "cadence-go:ci"
            config = "services:\n" + "".join(
                f"  {service}:\n    image: {image}\n" for service, image in
                (("frontend", service_tag), ("matching", service_tag), ("history", "cadence-go:ci"),
                 ("worker", "cadence-go:ci"), ("mysql", "mysql:8.0")))
            (self.out / f"compose-{stem}.yaml").write_text(config, encoding="utf-8")
            start = dt.datetime.now(dt.timezone.utc)
            end = start + dt.timedelta(seconds=1)
            run = {"label": arm, "requests": 1, "warmup": 0, "concurrency": 1, "payload_bytes": 0,
                   "successful": 1, "failed": 0, "elapsed_seconds": 1.0,
                   "successful_workflows_per_second": 1.0, "latency_ms": {"p50": 1.0, "p95": 1.0, "p99": 1.0},
                   "successful_latency_samples_ms": [1.0], "transport": "tchannel", "sticky_execution": False,
                   "errors": [], "measured_start_utc": start.isoformat(), "measured_end_utc": end.isoformat()}
            dump(self.out / f"result-{stem}-c1.json", run)
            metrics = {name: {"cpu_seconds": 0.5, "average_cpu_cores": 0.5, "peak_cgroup_memory_bytes": 10,
                              "initial_cpu_ns": 10, "final_cpu_ns": 500000010} for name in
                       ("frontend", "matching", "history", "worker")}
            dump(self.out / f"resources-{stem}-c1.json", {"schema_version": 2, "started_at_utc": start.isoformat(),
                 "ended_at_utc": end.isoformat(), "duration_seconds": 1.0, "containers": metrics})
            for prefix in ("run", "collector"):
                name = f"{prefix}-{stem}-c1"
                command = (f"python out/cadence-benchmark -label {arm} -requests 1 -warmup 0 -concurrency 1 "
                           f"-payload-bytes 0 -measurement-start-file measurement-{stem}-c1.start "
                           f"-measurement-end-file measurement-{stem}-c1.end -collector-ready-file measurement-{stem}-c1.ready "
                           f"-output result-{stem}-c1.json" if prefix == "run" else
                           f"python tests/benchmark/collect_resources.py --start-event measurement-{stem}-c1.start "
                           f"--end-event measurement-{stem}-c1.end --ready-event measurement-{stem}-c1.ready "
                           f"--output resources-{stem}-c1.json --containers frontend matching history worker")
                events.append({"name": name, "command": command, "exit_code": 0,
                               "log": "synthetic.log", "started_unix": 1, "elapsed_seconds": 1})
            for prefix in ("down", "down-after"):
                events.append({"name": f"{prefix}-{stem}",
                               "command": "docker compose -p cadence-fm-benchmark down -v --remove-orphans",
                               "exit_code": 0, "log": "synthetic.log", "started_unix": 1, "elapsed_seconds": 1})
        events.append({"name": "cleanup", "command": "docker compose -p cadence-fm-benchmark down -v --remove-orphans", "exit_code": 0,
                       "log": "synthetic.log", "started_unix": 1, "elapsed_seconds": 1})
        dump(self.out / "commands.json", events)

    def tearDown(self):
        self.temp.cleanup()

    def test_valid_synthetic_fixture_and_smoke_warning(self):
        result = verify(self.out, HEAD)
        self.assertTrue(result["valid"])
        self.assertTrue(any("smoke" in warning for warning in result["warnings"]))

    def test_multiple_concurrency_levels_reuse_run_identity_safely(self):
        for rep in range(2):
            for arm in ("go", "rust"):
                stem = f"{rep:02d}-{arm}"
                result_path = self.out / f"result-{stem}-c1.json"
                result = json.loads(result_path.read_text())
                result["concurrency"] = 2
                dump(self.out / f"result-{stem}-c2.json", result)
                (self.out / f"resources-{stem}-c2.json").write_text(
                    (self.out / f"resources-{stem}-c1.json").read_text(), encoding="utf-8")
        host = json.loads((self.out / "host.json").read_text())
        host["concurrency"] = [1, 2]
        dump(self.out / "host.json", host)
        events = json.loads((self.out / "commands.json").read_text())
        for rep in range(2):
            for arm in ("go", "rust"):
                stem = f"{rep:02d}-{arm}"
                common = ("-label " + arm + " -requests 1 -warmup 0 -concurrency 2 -payload-bytes 0 "
                          f"-measurement-start-file measurement-{stem}-c2.start "
                          f"-measurement-end-file measurement-{stem}-c2.end "
                          f"-collector-ready-file measurement-{stem}-c2.ready")
                events.extend([
                    {"name": f"run-{stem}-c2", "command": f"python out/cadence-benchmark {common} -output result-{stem}-c2.json",
                     "exit_code": 0, "log": "fixture.log", "started_unix": 1, "elapsed_seconds": 1},
                    {"name": f"collector-{stem}-c2",
                     "command": f"python tests/benchmark/collect_resources.py --start-event measurement-{stem}-c2.start "
                                f"--end-event measurement-{stem}-c2.end --ready-event measurement-{stem}-c2.ready "
                                f"--output resources-{stem}-c2.json --containers frontend matching history worker",
                     "exit_code": 0, "log": "fixture.log", "started_unix": 1, "elapsed_seconds": 1},
                ])
        dump(self.out / "commands.json", events)
        verified = verify(self.out, HEAD)
        self.assertEqual([v["concurrency"] for v in verified["verified"]], [1, 2])

    def test_missing_paired_resource_fails(self):
        (self.out / "resources-00-rust-c1.json").unlink()
        with self.assertRaises(InvalidEvidence):
            verify(self.out, HEAD)

    def test_bad_immutable_identity_fails(self):
        path = self.out / "identities-00-rust.json"
        identity = json.loads(path.read_text())
        identity["matching"]["image_id"] = GO
        dump(path, identity)
        with self.assertRaises(InvalidEvidence):
            verify(self.out, HEAD)

    def test_timestamp_mismatch_fails(self):
        path = self.out / "result-00-go-c1.json"
        run = json.loads(path.read_text())
        run["measured_start_utc"] = "2020-01-01T00:00:00Z"
        dump(path, run)
        with self.assertRaises(InvalidEvidence):
            verify(self.out, HEAD)

    def test_failed_final_cleanup_fails(self):
        path = self.out / "commands.json"
        events = json.loads(path.read_text())
        events[-1]["exit_code"] = 1
        dump(path, events)
        with self.assertRaises(InvalidEvidence):
            verify(self.out, HEAD)

    def test_non_object_command_and_boolean_exit_code_fail(self):
        path = self.out / "commands.json"
        events = json.loads(path.read_text())
        events[0] = []
        dump(path, events)
        with self.assertRaises(InvalidEvidence):
            verify(self.out, HEAD)
        events = json.loads(path.read_text())
        events[0] = {"name": "run-00-go-c1", "command": "fixture", "exit_code": False,
                     "log": "fixture.log", "started_unix": 1, "elapsed_seconds": 1}
        dump(path, events)
        with self.assertRaises(InvalidEvidence):
            verify(self.out, HEAD)

    def test_missing_collector_and_shared_mysql_volume_fail(self):
        path = self.out / "commands.json"
        events = json.loads(path.read_text())
        events[:] = [event for event in events if event["name"] != "collector-01-rust-c1"]
        dump(path, events)
        with self.assertRaises(InvalidEvidence):
            verify(self.out, HEAD)
        events.append({"name": "collector-01-rust-c1", "command": "fixture", "exit_code": 0,
                       "log": "fixture.log", "started_unix": 1, "elapsed_seconds": 1})
        dump(path, events)
        for stem in ("00-go", "00-rust"):
            identity_path = self.out / f"identities-{stem}.json"
            identity = json.loads(identity_path.read_text())
            identity["mysql"]["Mounts"] = [{"Type": "volume", "Name": "shared-mysql-data",
                                              "Source": "/docker/volumes/shared-mysql-data",
                                              "Destination": "/var/lib/mysql"}]
            dump(identity_path, identity)
        with self.assertRaises(InvalidEvidence):
            verify(self.out, HEAD)

    def test_counter_delta_and_malformed_resource_fail(self):
        path = self.out / "resources-00-go-c1.json"
        resource = json.loads(path.read_text())
        resource["containers"]["frontend"]["final_cpu_ns"] += 2000
        dump(path, resource)
        with self.assertRaises(InvalidEvidence):
            verify(self.out, HEAD)
        resource["duration_seconds"] = 1.0
        resource["containers"]["frontend"]["average_cpu_cores"] = 0.9
        dump(path, resource)
        with self.assertRaises(InvalidEvidence):
            verify(self.out, HEAD)
        resource["containers"] = []
        dump(path, resource)
        with self.assertRaises(InvalidEvidence):
            verify(self.out, HEAD)

    def test_boolean_concurrency_fails(self):
        dump(self.out / "host.json", {"repetitions": 2, "concurrency": [True]})
        with self.assertRaises(InvalidEvidence):
            verify(self.out, HEAD)

    def test_result_concurrency_and_cleanup_command_are_bound_to_evidence(self):
        result_path = self.out / "result-00-rust-c1.json"
        result = json.loads(result_path.read_text())
        result["concurrency"] = 16
        dump(result_path, result)
        with self.assertRaises(InvalidEvidence):
            verify(self.out, HEAD)
        result["concurrency"] = 1
        dump(result_path, result)
        command_path = self.out / "commands.json"
        events = json.loads(command_path.read_text())
        events[-1]["command"] = "echo cleanup"
        dump(command_path, events)
        with self.assertRaises(InvalidEvidence):
            verify(self.out, HEAD)
        events = json.loads(command_path.read_text())
        next(event for event in events if event["name"] == "run-00-go-c1")["command"] = (
            "python out/cadence-benchmark -label go -requests 1 -warmup 0 -concurrency 1 "
            "-payload-bytes 0 -measurement-start-file measurement-00-go-c1.start "
            "-measurement-end-file measurement-00-go-c1.end -collector-ready-file measurement-00-go-c1.ready "
            "-output result-00-go-c1.json")
        dump(command_path, events)
        identity_path = self.out / "identities-00-go.json"
        identity = json.loads(identity_path.read_text())
        del identity["mysql"]["Mounts"]
        dump(identity_path, identity)
        with self.assertRaises(InvalidEvidence):
            verify(self.out, HEAD)

    def test_resource_duration_and_average_cpu_must_agree(self):
        path = self.out / "resources-00-go-c1.json"
        resource = json.loads(path.read_text())
        resource["duration_seconds"] = 5.0
        dump(path, resource)
        with self.assertRaises(InvalidEvidence):
            verify(self.out, HEAD)

    def test_mismatched_driver_command_and_missing_mysql_mount_record_fail(self):
        command_path = self.out / "commands.json"
        events = json.loads(command_path.read_text())
        next(event for event in events if event["name"] == "run-00-go-c1")["command"] = "echo fake benchmark"
        dump(command_path, events)
        with self.assertRaises(InvalidEvidence):
            verify(self.out, HEAD)

    def test_resolved_compose_configuration_and_teardown_must_match(self):
        path = self.out / "compose-00-rust.yaml"
        path.write_text(path.read_text().replace("image: cadence-rust:ci", "image: cadence-go:ci", 1), encoding="utf-8")
        with self.assertRaises(InvalidEvidence):
            verify(self.out, HEAD)
        # Restore the intended config, then show a nominally present but failed teardown is rejected.
        path.write_text("services:\n  frontend:\n    image: cadence-rust:ci\n  matching:\n    image: cadence-rust:ci\n"
                        "  history:\n    image: cadence-go:ci\n  worker:\n    image: cadence-go:ci\n"
                        "  mysql:\n    image: mysql:8.0\n", encoding="utf-8")
        commands_path = self.out / "commands.json"
        events = json.loads(commands_path.read_text())
        next(event for event in events if event["name"] == "down-after-00-rust")["exit_code"] = 1
        dump(commands_path, events)
        with self.assertRaises(InvalidEvidence):
            verify(self.out, HEAD)


if __name__ == "__main__":
    unittest.main()
