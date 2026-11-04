import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


spec = importlib.util.spec_from_file_location(
    "paired_runner", Path(__file__).resolve().parents[1] / "frontend-gate/benchmark/paired-run.py")
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class PairedRunnerTests(unittest.TestCase):
    def test_timed_out_commands_preserve_exit_and_command_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "out"
            output.mkdir()
            with patch.object(runner, "ROOT", root), patch.object(runner, "OUT", output), \
                 patch.object(runner, "EVENTS", []), \
                 patch.object(runner.subprocess, "run", side_effect=subprocess.TimeoutExpired("driver", 1)):
                with self.assertRaisesRegex(RuntimeError, "exited 124"):
                    runner.run(["driver", "--requests", "100"], "measured", timeout=1)
                self.assertEqual(runner.EVENTS[0]["exit_code"], 124)
                self.assertEqual(runner.EVENTS[0]["command"], "driver --requests 100")
                self.assertIn("timeout", (output / "measured.log").read_text())
                self.assertTrue((output / "commands.json").exists())


if __name__ == "__main__":
    unittest.main()
