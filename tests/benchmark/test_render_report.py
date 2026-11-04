import importlib.util
from pathlib import Path
import unittest


MODULE = Path(__file__).with_name("render_report.py")
SPEC = importlib.util.spec_from_file_location("render_report", MODULE)
render_report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(render_report)


def comparison(resources=None, scope="combined Frontend + Matching <Go History>"):
    def arm(label, rate, p50, p95, p99):
        run = {"successful_workflows_per_second": rate, "latency_ms": {"p50": p50, "p95": p95, "p99": p99}}
        return {"runs": [run], "repetitions": 1,
                "median_throughput_workflows_per_second": rate,
                "median_latency_ms": {"p50": p50, "p95": p95, "p99": p99},
                "metadata": {"deployment": label}}
    return {"schema_version": 1, "scope": scope,
            "workload": {"requests": 200, "warmup": 20, "concurrency": 4,
                         "payload_bytes": 128, "transport": "grpc", "sticky_execution": False},
            "arms": {"go": arm("go", 10, 12, 22, 29), "rust": arm("rust", 11, 10, 19, 25)},
            "paired_runs": [{"go_throughput": 10, "rust_throughput": 11}],
            "resources": resources or {"status": "not_collected"}}


class RenderReportTests(unittest.TestCase):
    def test_html_escapes_scope_and_metadata(self):
        doc = comparison(scope='<script>alert("scope")</script>')
        doc["arms"]["go"]["metadata"] = {"host": '<img src=x onerror="bad">'}
        output = render_report.render_html(doc)
        self.assertNotIn("<script>alert", output)
        self.assertNotIn("<img src=x", output)
        self.assertIn("&lt;script&gt;", output)
        self.assertIn("&lt;img src=x", output)

    def test_missing_resources_are_explicit(self):
        output = render_report.render_html(comparison({"status": "incomplete", "reason": "samples < 2"}))
        self.assertIn("Comparison status: <strong>incomplete</strong>", output)
        self.assertIn("samples &lt; 2", output)
        self.assertNotIn("Median resources per run", output)

    def test_comparable_resource_values_are_reported(self):
        doc = comparison({"status": "comparable", "arms": {
            "go": {"frontend": {"cpu_seconds": 1.25, "average_cpu_cores": 0.5, "peak_cgroup_memory_bytes": 2 * 1048576}},
            "rust": {"frontend": {"cpu_seconds": 0.75, "average_cpu_cores": 0.3, "peak_cgroup_memory_bytes": 1.5 * 1048576}},
        }, "rust_minus_go": {}})
        output = render_report.render_html(doc)
        self.assertIn("1.250", output)
        self.assertIn("0.750", output)
        self.assertIn("2.00", output)
        self.assertIn("1.50", output)

    def test_invalid_document_fails_clearly(self):
        with self.assertRaisesRegex(ValueError, "schema_version 1"):
            render_report.render_html({"schema_version": 2})


if __name__ == "__main__":
    unittest.main()
