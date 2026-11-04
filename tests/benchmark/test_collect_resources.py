import unittest
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from collect_resources import collect, cpu_seconds, parse_cgroups


class ResourceCounterTests(unittest.TestCase):
    def test_ready_marker_follows_baseline_and_final_sample_is_included(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            start, end, ready = [base / name for name in ("start", "end", "ready")]
            start.touch()
            def end_measurement(_):
                self.assertTrue(ready.exists())
                end.touch()
            snapshots = [{"frontend": {"cpu_ns": cpu, "memory_bytes": memory}}
                         for cpu, memory in ((1000, 10), (2000, 20), (4000, 30))]
            with patch("collect_resources._container_cgroups", return_value={"frontend": (base, "v2")}), \
                 patch("collect_resources.snapshot", side_effect=snapshots), \
                 patch("collect_resources.time.sleep", side_effect=end_measurement):
                collect(.05, base / "out.json", ["frontend"], start, end, ready_event=ready)
            metrics = json.loads((base / "out.json").read_text())["containers"]["frontend"]
            self.assertEqual(metrics["initial_cpu_ns"], 1000)
            self.assertEqual(metrics["final_cpu_ns"], 4000)
            self.assertEqual(metrics["peak_cgroup_memory_bytes"], 30)
            self.assertEqual(metrics["cpu_seconds"], .000003)

    def test_unified_cgroup_uses_container_path(self):
        self.assertEqual(parse_cgroups(["0::/system.slice/docker-abc.scope"]),
                         {"": "system.slice/docker-abc.scope"})

    def test_v1_controller_names_are_not_hierarchy_ids(self):
        self.assertEqual(parse_cgroups(["5:cpu,cpuacct:/docker/abc", "6:memory:/docker/abc"]),
                         {"cpu": "docker/abc", "cpuacct": "docker/abc", "memory": "docker/abc"})

    def test_uses_exact_cumulative_counter_delta(self):
        self.assertEqual(cpu_seconds(1_250_000_000, 3_750_000_000), 2.5)

    def test_rejects_counter_reset(self):
        with self.assertRaisesRegex(ValueError, "counter reset"):
            cpu_seconds(3, 2)


if __name__ == "__main__":
    unittest.main()
