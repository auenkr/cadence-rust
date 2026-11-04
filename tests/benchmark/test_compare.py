import unittest

from compare import compare, markdown


def run(label, rate=10.0, concurrency=16, failed=0, successful=10):
    return {"label": label, "requests": 10, "warmup": 2, "concurrency": concurrency, "payload_bytes": 128,
            "successful": successful, "failed": failed, "elapsed_seconds": 1.0,
            "successful_workflows_per_second": rate,
            "latency_ms": {"p50": 10.0, "p95": 20.0, "p99": 30.0},
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


if __name__ == "__main__":
    unittest.main()
