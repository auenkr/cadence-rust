import unittest

from collect_resources import cpu_seconds


class ResourceCounterTests(unittest.TestCase):
    def test_uses_exact_cumulative_counter_delta(self):
        self.assertEqual(cpu_seconds(1_250_000_000, 3_750_000_000), 2.5)

    def test_rejects_counter_reset(self):
        with self.assertRaisesRegex(ValueError, "counter reset"):
            cpu_seconds(3, 2)


if __name__ == "__main__":
    unittest.main()
