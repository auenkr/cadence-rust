import unittest

from compare import compare, markdown


def run(label, rate=10.0, concurrency=16, failed=0, successful=10):
    return {"label": label, "requests": 10, "warmup": 2, "concurrency": concurrency, "payload_bytes": 128,
            "successful": successful, "failed": failed, "elapsed_seconds": successful / rate,
            "successful_workflows_per_second": rate,
            "latency_ms": {"p50": 10.0, "p95": 10.0, "p99": 10.0},
            "successful_latency_samples_ms": [10.0] * successful,
            "transport": "tchannel", "sticky_execution": False}


class CompareTests(unittest.TestCase):
    def test_rejects_failed_run(self):
        with self.assertRaisesRegex(ValueError, "failed or incomplete"):
            compare([run("go", failed=1, successful=9)], [run("rust")])

    def test_rejects_mismatched_workload(self):
        with self.assertRaisesRegex(ValueError, "mismatched workload parameters"):
            compare([run("go")], [run("rust", concurrency=64)])

    def test_paired_median_aggregation_and_deltas(self):
        go = [run("go", rate=10), run("go", rate=20), run("go", rate=15)]
        rust = [run("rust", rate=15), run("rust", rate=30), run("rust", rate=25)]
        report = compare(go, rust)
        self.assertEqual(report["arms"]["go"]["median_throughput_workflows_per_second"], 15)
        self.assertEqual(report["arms"]["rust"]["median_throughput_workflows_per_second"], 25)
        self.assertAlmostEqual(report["median_deltas"]["throughput_percent"], 200 / 3)
        self.assertEqual(len(report["paired_runs"]), 3)
        self.assertIn("combined Frontend+Matching", markdown(report))

    def test_requires_equal_nonzero_repetition_counts(self):
        with self.assertRaisesRegex(ValueError, "same nonzero number"):
            compare([], [])
        with self.assertRaisesRegex(ValueError, "same nonzero number"):
            compare([run("go")], [run("rust"), run("rust")])

    def test_rejects_cross_repetition_workload_drift(self):
        with self.assertRaisesRegex(ValueError, "Go repetitions"):
            compare([run("go"), run("go", concurrency=32)], [run("rust"), run("rust", concurrency=32)])

    def test_rejects_inconsistent_summary_and_nonfinite_rates(self):
        bad = run("go")
        bad["successful_workflows_per_second"] = float("nan")
        with self.assertRaisesRegex(ValueError, "throughput"):
            compare([bad], [run("rust")])
        bad = run("go")
        bad["latency_ms"]["p95"] = 999
        with self.assertRaisesRegex(ValueError, "percentiles do not match"):
            compare([bad], [run("rust")])

    def test_resources_have_explicit_missing_and_complete_states(self):
        report = compare([run("go")], [run("rust")])
        self.assertEqual(report["resources"]["status"], "not_collected")
        go_res = {"schema_version": 2, "duration_seconds": 2, "containers": {"frontend": {"cpu_seconds": 2, "average_cpu_cores": 1, "peak_cgroup_memory_bytes": 100}}}
        rust_res = {"schema_version": 2, "duration_seconds": 2, "containers": {"frontend": {"cpu_seconds": 1, "average_cpu_cores": .5, "peak_cgroup_memory_bytes": 80}}}
        report = compare([run("go")], [run("rust")], go_resources=[go_res], rust_resources=[rust_res])
        self.assertEqual(report["resources"]["status"], "comparable")
        self.assertEqual(report["resources"]["rust_minus_go"]["frontend"]["cpu_seconds"], -1)


if __name__ == "__main__":
    unittest.main()
