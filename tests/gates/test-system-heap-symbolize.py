"""Regression coverage for batched, differential heap symbolization."""
import json
from pathlib import Path
import runpy
import sys
import tempfile
import unittest
from unittest import mock


class SymbolizationTest(unittest.TestCase):
    def test_batches_addresses_once_and_reports_growth_and_freed_stacks(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            for seconds, samples in [
                (180, [[64, [0x1011]]]),
                (1200, [[64, [0x1011]], [128, [0x1021]]]),
                (4000, []),
            ]:
                (directory / f'system-{seconds:04}.json').write_text(
                    json.dumps({'base': 0x1000, 'samples': samples}))
            symbols = '0x00000010\nfirst\nfixture.rs:1\n0x00000020\nsecond\nfixture.rs:2\n'
            script = Path(__file__).with_name('system-heap-symbolize.py')
            with mock.patch.object(sys, 'argv', [str(script), 'binary', str(directory)]), \
                    mock.patch('subprocess.check_output', return_value=symbols) as resolve:
                runpy.run_path(str(script), run_name='__main__')
            resolve.assert_called_once()
            self.assertEqual(resolve.call_args.args[0][-2:], ['0x10', '0x20'])
            growing = (directory / 'system-1200-stacks.txt').read_text()
            self.assertIn('delta_samples=+1', growing)
            self.assertIn('delta_requested_sample_bytes=+128', growing)
            self.assertIn('second\nfixture.rs:2', growing)
            freed = (directory / 'system-4000-stacks.txt').read_text()
            self.assertIn('delta_samples=-1', freed)
            self.assertIn('delta_requested_sample_bytes=-64', freed)


if __name__ == '__main__':
    unittest.main()
