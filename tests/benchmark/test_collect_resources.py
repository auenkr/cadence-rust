import unittest

from collect_resources import cpu_seconds, parse_cgroups


class ResourceCounterTests(unittest.TestCase):
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
