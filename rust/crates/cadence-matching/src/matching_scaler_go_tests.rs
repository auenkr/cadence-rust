//! Go parity ports of `tasklist/isolation_balancer_test.go`
//! (`TestAdjustWritePartitions`) and `tasklist/adaptive_scaler_test.go`
//! (`TestAdaptiveScalerRun`, `TestAdaptiveScalerLifecycle`) for
//! `matching_scaler`. Case names, cycle counts, and expected partition maps are
//! copied from Go. Go's gomock DescribeTaskList and TaskListPartitionConfig
//! scripts become explicit per-cycle inputs. Go's `run()` metric collection is
//! mirrored in `run_scaler_cycle` because the production collector is async and
//! registry-bound.
use std::collections::HashMap;

use super::{
    AggregateMetrics, IsolationBalancer, PartitionMap, PartitionMetrics, Scaler, ScalerConfig,
    ScalerDaemon,
};

/// Go `cycleInterval` for the balancer tests.
const CYCLE_NANOS: i64 = 15_000_000_000;
/// One-second sustains used by the adaptive scaler tests.
const SECOND_NANOS: i64 = 1_000_000_000;

fn parts(entries: Vec<(i32, Vec<&str>)>) -> PartitionMap {
    entries
        .into_iter()
        .map(|(id, groups)| (id, groups.into_iter().map(str::to_owned).collect()))
        .collect()
}

/// Go `partitions(n)`: `n` partitions with no isolation groups.
fn even(n: i32) -> PartitionMap {
    (0..n).map(|id| (id, Vec::new())).collect()
}

/// The same read and write layout, as Go's `ReadPartitions` and `WritePartitions`.
fn both(entries: Vec<(i32, Vec<&str>)>) -> (PartitionMap, PartitionMap) {
    (parts(entries.clone()), parts(entries))
}

/// Go `aggregatePartitionMetrics` with the isolation-group fields the balancer reads.
fn group_metrics(qps: &[(&str, f64)], pollers: &[(&str, bool)]) -> AggregateMetrics {
    AggregateMetrics {
        qps_by_group: qps.iter().map(|(group, value)| (group.to_string(), *value)).collect(),
        has_pollers_by_group: pollers.iter().map(|(group, polled)| (group.to_string(), *polled)).collect(),
        ..AggregateMetrics::default()
    }
}

/// Go metrics where every listed group also has pollers.
fn polled(qps: &[(&str, f64)]) -> AggregateMetrics {
    let pollers: Vec<(&str, bool)> = qps.iter().map(|(group, _)| (*group, true)).collect();
    group_metrics(qps, &pollers)
}

// ---------------------------------------------------------------------------
// isolation_balancer_test.go: TestAdjustWritePartitions
// ---------------------------------------------------------------------------

/// One Go `testCycle`. Unset metrics and partitions carry the previous
/// cycle's values. `expected: None` means no change: the output must equal the
/// input partitions.
#[derive(Default)]
struct BalancerCycle {
    metrics: Option<AggregateMetrics>,
    partitions: Option<PartitionMap>,
    expected: Option<PartitionMap>,
    groups_per_partition: Option<i64>,
}

/// Go `{}`: a cycle with no metrics, no partition change, and no expectation.
fn idle() -> BalancerCycle {
    BalancerCycle::default()
}

/// Go cycle with metrics and partitions but no expectation.
fn observe(metrics: AggregateMetrics, partitions: PartitionMap) -> BalancerCycle {
    BalancerCycle {
        metrics: Some(metrics),
        partitions: Some(partitions),
        ..BalancerCycle::default()
    }
}

/// Go cycle that only asserts the output for carried-forward inputs.
fn expect(expected: PartitionMap) -> BalancerCycle {
    BalancerCycle {
        expected: Some(expected),
        ..BalancerCycle::default()
    }
}

/// Go `cycleInterval` advances before each cycle. Groups per partition live
/// in dynamic config, so cases can change them between cycles.
fn run_balancer(groups_per_partition: i64, cycles: Vec<BalancerCycle>) {
    // Go `isolationConfig`: every isolation-group sustain is cycleInterval*4.
    // ScalerConfig::default() uses 120s for the downscale sustain, so set all four.
    let mut config = ScalerConfig {
        groups_per_partition,
        group_upscale_sustain_nanos: 4 * CYCLE_NANOS,
        group_downscale_sustain_nanos: 4 * CYCLE_NANOS,
        group_has_pollers_sustain_nanos: 4 * CYCLE_NANOS,
        group_no_pollers_sustain_nanos: 4 * CYCLE_NANOS,
        ..ScalerConfig::default()
    };
    let mut balancer = IsolationBalancer::default();
    let mut now = 0;
    for (index, cycle) in cycles.iter().enumerate() {
        now += CYCLE_NANOS;
        if let Some(groups) = cycle.groups_per_partition {
            config.groups_per_partition = groups;
        }
        let previous = &cycles[..=index];
        let metrics = previous
            .iter()
            .rev()
            .find_map(|c| c.metrics.as_ref())
            .expect("cycle 0 sets metrics");
        let input = previous
            .iter()
            .rev()
            .find_map(|c| c.partitions.as_ref())
            .expect("cycle 0 sets partitions");
        let (actual, changed) = balancer.adjust_write_partitions(&config, now, metrics, input);
        match &cycle.expected {
            Some(expected) => {
                assert!(changed, "cycle {index} - expected change");
                assert_eq!(&actual, expected, "cycle {index} - did not match expected");
            }
            None => {
                assert!(!changed, "cycle {index} - expected no change");
                assert_eq!(&actual, input, "cycle {index} - was unexpectedly changed");
            }
        }
    }
}

#[test]
fn matching_isolation_balancer_initial_balance() {
    run_balancer(
        2,
        vec![
            observe(polled(&[("a", 100.0), ("b", 99.0)]), even(2)),
            idle(),
            idle(),
            idle(),
            expect(parts(vec![(0, vec!["a", "b"]), (1, vec!["a", "b"])])),
        ],
    );
}

#[test]
fn matching_isolation_balancer_initial_balance_from_one_partition() {
    run_balancer(
        2,
        vec![
            observe(polled(&[("a", 100.0), ("b", 99.0)]), even(1)),
            idle(),
            idle(),
            idle(),
            BalancerCycle {
                partitions: Some(even(2)),
                expected: Some(parts(vec![(0, vec!["a", "b"]), (1, vec!["a", "b"])])),
                ..BalancerCycle::default()
            },
        ],
    );
}

#[test]
fn matching_isolation_balancer_initial_balance_from_one_partition_skewed() {
    run_balancer(
        2,
        vec![
            observe(polled(&[("a", 300.0), ("b", 75.0), ("c", 2.0), ("d", 1.0)]), even(1)),
            idle(),
            idle(),
            idle(),
            BalancerCycle {
                partitions: Some(even(2)),
                expected: Some(parts(vec![(0, vec!["a", "b"]), (1, vec!["a", "c", "d"])])),
                ..BalancerCycle::default()
            },
        ],
    );
}

#[test]
fn matching_isolation_balancer_one_partition_minimum() {
    run_balancer(
        1,
        vec![
            observe(polled(&[("a", 100.0), ("b", 99.0)]), even(2)),
            idle(),
            idle(),
            idle(),
            expect(parts(vec![(0, vec!["a"]), (1, vec!["b"])])),
        ],
    );
}

#[test]
fn matching_isolation_balancer_require_pollers() {
    run_balancer(
        2,
        vec![
            observe(group_metrics(&[("a", 100.0), ("b", 99.0)], &[("a", true)]), even(2)),
            idle(),
            idle(),
            idle(),
            expect(parts(vec![(0, vec!["a"]), (1, vec!["a"])])),
        ],
    );
}

#[test]
fn matching_isolation_balancer_fewer_groups_than_partitions() {
    run_balancer(
        2,
        vec![
            observe(polled(&[("a", 100.0), ("b", 99.0), ("c", 98.0), ("d", 97.0)]), even(6)),
            idle(),
            idle(),
            idle(),
            expect(parts(vec![
                (0, vec!["a", "d"]),
                (1, vec!["a", "d"]),
                (2, vec!["a", "d"]),
                (3, vec!["b", "c"]),
                (4, vec!["b", "c"]),
                (5, vec!["b", "c"]),
            ])),
        ],
    );
}

#[test]
fn matching_isolation_balancer_fewer_partitions_than_groups() {
    run_balancer(
        2,
        vec![
            observe(polled(&[("a", 100.0), ("b", 99.0), ("c", 98.0), ("d", 97.0)]), even(2)),
            idle(),
            idle(),
            idle(),
            expect(parts(vec![(0, vec!["a", "d"]), (1, vec!["b", "c"])])),
        ],
    );
}

#[test]
fn matching_isolation_balancer_fewer_active_groups_than_isolation_groups_per_partition() {
    run_balancer(
        4,
        vec![
            observe(polled(&[("a", 100.0), ("b", 99.0)]), even(2)),
            idle(),
            idle(),
            idle(),
            expect(parts(vec![(0, vec!["a", "b"]), (1, vec!["a", "b"])])),
        ],
    );
}

#[test]
fn matching_isolation_balancer_single_partition() {
    run_balancer(
        2,
        vec![
            observe(polled(&[("a", 100.0), ("b", 99.0)]), even(1)),
            idle(),
            idle(),
            idle(),
            idle(),
        ],
    );
}

#[test]
fn matching_isolation_balancer_single_group() {
    run_balancer(
        2,
        vec![
            observe(polled(&[("a", 100.0)]), even(2)),
            idle(),
            idle(),
            idle(),
            expect(parts(vec![(0, vec!["a"]), (1, vec!["a"])])),
        ],
    );
}

#[test]
fn matching_isolation_balancer_pollers_added() {
    run_balancer(
        2,
        vec![
            observe(
                polled(&[("a", 100.0), ("b", 99.0)]),
                parts(vec![(0, vec!["a", "b"]), (1, vec!["a", "b"])]),
            ),
            BalancerCycle {
                metrics: Some(group_metrics(
                    &[("a", 100.0), ("b", 99.0)],
                    &[("a", true), ("b", true), ("c", true)],
                )),
                ..BalancerCycle::default()
            },
            idle(),
            idle(),
            idle(),
            expect(parts(vec![(0, vec!["a", "b", "c"]), (1, vec!["a", "b", "c"])])),
        ],
    );
}

#[test]
fn matching_isolation_balancer_pollers_removed() {
    run_balancer(
        2,
        vec![
            observe(
                group_metrics(
                    &[("a", 100.0), ("b", 99.0)],
                    &[("a", true), ("b", true), ("c", true)],
                ),
                parts(vec![(0, vec!["a", "b", "c"]), (1, vec!["a", "b", "c"])]),
            ),
            BalancerCycle {
                metrics: Some(polled(&[("a", 101.0), ("b", 100.0)])),
                ..BalancerCycle::default()
            },
            idle(),
            idle(),
            idle(),
            expect(parts(vec![(0, vec!["a", "b"]), (1, vec!["a", "b"])])),
        ],
    );
}

#[test]
fn matching_isolation_balancer_scale_up_single_partition() {
    run_balancer(
        2,
        vec![
            observe(
                polled(&[("a", 100.0), ("b", 99.0), ("c", 98.0), ("d", 97.0)]),
                parts(vec![(0, vec!["a", "d"]), (1, vec!["b", "c"])]),
            ),
            BalancerCycle {
                metrics: Some(polled(&[("a", 101.0), ("b", 99.0), ("c", 98.0), ("d", 97.0)])),
                ..BalancerCycle::default()
            },
            idle(),
            idle(),
            idle(),
            expect(parts(vec![(0, vec!["a", "d"]), (1, vec!["a", "b", "c"])])),
        ],
    );
}

#[test]
fn matching_isolation_balancer_scale_up_multiple_partition() {
    run_balancer(
        2,
        vec![
            observe(
                polled(&[
                    ("a", 201.0),
                    ("b", 99.0),
                    ("c", 98.0),
                    ("d", 97.0),
                    ("e", 15.0),
                    ("f", 10.0),
                    ("g", 5.0),
                    ("h", 1.0),
                ]),
                parts(vec![
                    (0, vec!["a", "h"]),
                    (1, vec!["b", "g"]),
                    (2, vec!["c", "f"]),
                    (3, vec!["d", "e"]),
                ]),
            ),
            idle(),
            idle(),
            idle(),
            expect(parts(vec![
                (0, vec!["a", "h"]),
                (1, vec!["a", "b", "g"]),
                (2, vec!["a", "c", "f"]),
                (3, vec!["d", "e"]),
            ])),
        ],
    );
}

#[test]
fn matching_isolation_balancer_scale_up_multiple_groups() {
    run_balancer(
        2,
        vec![
            observe(
                polled(&[
                    ("a", 102.0),
                    ("b", 101.0),
                    ("c", 98.0),
                    ("d", 97.0),
                    ("e", 15.0),
                    ("f", 10.0),
                    ("g", 5.0),
                    ("h", 1.0),
                ]),
                parts(vec![
                    (0, vec!["a", "h"]),
                    (1, vec!["b", "g"]),
                    (2, vec!["c", "f"]),
                    (3, vec!["d", "e"]),
                ]),
            ),
            idle(),
            idle(),
            idle(),
            expect(parts(vec![
                (0, vec!["a", "b", "h"]),
                (1, vec!["a", "b", "g"]),
                (2, vec!["c", "f"]),
                (3, vec!["d", "e"]),
            ])),
        ],
    );
}

#[test]
fn matching_isolation_balancer_immediate_scale_up_on_new_partition() {
    run_balancer(
        2,
        vec![
            observe(
                polled(&[
                    ("a", 303.0),
                    ("b", 99.0),
                    ("c", 98.0),
                    ("d", 97.0),
                    ("e", 15.0),
                    ("f", 10.0),
                    ("g", 5.0),
                    ("h", 1.0),
                ]),
                parts(vec![
                    (0, vec!["a", "b", "g", "h"]),
                    (1, vec!["a", "c", "f"]),
                    (2, vec!["a", "d", "e"]),
                ]),
            ),
            idle(),
            idle(),
            idle(),
            // If we had the space "a" would have scaled up on this cycle.
            idle(),
            BalancerCycle {
                partitions: Some(parts(vec![
                    (0, vec!["a", "b", "g", "h"]),
                    (1, vec!["a", "c", "f"]),
                    (2, vec!["a", "d", "e"]),
                    // The adaptive scaler gave us another partition.
                    (3, vec![]),
                ])),
                expected: Some(parts(vec![
                    (0, vec!["a", "g", "h"]),
                    (1, vec!["a", "c", "f"]),
                    (2, vec!["a", "d", "e"]),
                    (3, vec!["a", "b"]),
                ])),
                ..BalancerCycle::default()
            },
        ],
    );
}

#[test]
fn matching_isolation_balancer_immediate_scale_up_on_new_partition_multiple() {
    run_balancer(
        2,
        vec![
            observe(
                polled(&[
                    ("a", 303.0),
                    ("b", 303.0),
                    ("c", 98.0),
                    ("d", 97.0),
                    ("e", 15.0),
                    ("f", 10.0),
                    ("g", 5.0),
                    ("h", 1.0),
                ]),
                parts(vec![
                    (0, vec!["a", "b", "c"]),
                    (1, vec!["a", "b", "d"]),
                    (2, vec!["a", "b", "e", "f", "g", "h"]),
                ]),
            ),
            idle(),
            idle(),
            idle(),
            // If we had the space "a" and "b" would have scaled up on this cycle.
            idle(),
            BalancerCycle {
                partitions: Some(parts(vec![
                    (0, vec!["a", "b", "c"]),
                    (1, vec!["a", "b", "d"]),
                    (2, vec!["a", "b", "e", "f", "g", "h"]),
                    // The adaptive scaler gave us another partition.
                    (3, vec![]),
                ])),
                expected: Some(parts(vec![
                    (0, vec!["a", "b", "c"]),
                    (1, vec!["a", "b", "d"]),
                    (2, vec!["a", "b", "e", "f", "g", "h"]),
                    (3, vec!["a", "b"]),
                ])),
                ..BalancerCycle::default()
            },
        ],
    );
}

#[test]
fn matching_isolation_balancer_scale_down_single_partition() {
    run_balancer(
        2,
        vec![
            observe(
                polled(&[("a", 75.0), ("b", 99.0), ("c", 98.0), ("d", 97.0)]),
                parts(vec![(0, vec!["a", "c"]), (1, vec!["a", "b", "d"])]),
            ),
            BalancerCycle {
                metrics: Some(polled(&[("a", 74.0), ("b", 99.0), ("c", 98.0), ("d", 97.0)])),
                ..BalancerCycle::default()
            },
            idle(),
            idle(),
            idle(),
            expect(parts(vec![(0, vec!["a", "c"]), (1, vec!["b", "d"])])),
        ],
    );
}

#[test]
fn matching_isolation_balancer_scale_down_multiple_partition() {
    run_balancer(
        2,
        vec![
            observe(
                polled(&[
                    ("a", 74.0),
                    ("b", 99.0),
                    ("c", 98.0),
                    ("d", 97.0),
                    ("e", 15.0),
                    ("f", 10.0),
                    ("g", 5.0),
                    ("h", 1.0),
                ]),
                parts(vec![
                    (0, vec!["a", "h"]),
                    (1, vec!["a", "b", "g"]),
                    (2, vec!["a", "c", "f"]),
                    (3, vec!["d", "e"]),
                ]),
            ),
            idle(),
            idle(),
            idle(),
            expect(parts(vec![
                (0, vec!["a", "h"]),
                (1, vec!["b", "g"]),
                (2, vec!["c", "f"]),
                (3, vec!["d", "e"]),
            ])),
        ],
    );
}

#[test]
fn matching_isolation_balancer_scale_down_multiple_groups() {
    run_balancer(
        2,
        vec![
            observe(
                polled(&[
                    ("a", 74.0),
                    ("b", 73.0),
                    ("c", 98.0),
                    ("d", 97.0),
                    ("e", 15.0),
                    ("f", 10.0),
                    ("g", 5.0),
                    ("h", 1.0),
                ]),
                parts(vec![
                    (0, vec!["a", "b", "h"]),
                    (1, vec!["a", "b", "g"]),
                    (2, vec!["c", "f"]),
                    (3, vec!["d", "e"]),
                ]),
            ),
            idle(),
            idle(),
            idle(),
            expect(parts(vec![
                (0, vec!["a", "h"]),
                (1, vec!["b", "g"]),
                (2, vec!["c", "f"]),
                (3, vec!["d", "e"]),
            ])),
        ],
    );
}

#[test]
fn matching_isolation_balancer_scale_down_single_partition_left() {
    run_balancer(
        2,
        vec![BalancerCycle {
            metrics: Some(polled(&[
                ("a", 74.0),
                ("b", 73.0),
                ("c", 98.0),
                ("d", 97.0),
                ("e", 15.0),
                ("f", 10.0),
                ("g", 5.0),
                ("h", 1.0),
            ])),
            partitions: Some(parts(vec![(0, vec!["a", "b", "h"])])),
            expected: Some(parts(vec![(0, vec![])])),
            ..BalancerCycle::default()
        }],
    );
}

#[test]
fn matching_isolation_balancer_scale_up_then_down() {
    run_balancer(
        2,
        vec![
            observe(
                polled(&[("a", 100.0), ("b", 99.0), ("c", 98.0), ("d", 97.0)]),
                parts(vec![(0, vec!["a", "d"]), (1, vec!["b", "c"])]),
            ),
            BalancerCycle {
                metrics: Some(polled(&[("a", 101.0), ("b", 99.0), ("c", 98.0), ("d", 97.0)])),
                ..BalancerCycle::default()
            },
            idle(),
            idle(),
            idle(),
            expect(parts(vec![(0, vec!["a", "d"]), (1, vec!["a", "b", "c"])])),
            BalancerCycle {
                partitions: Some(parts(vec![(0, vec!["a", "d"]), (1, vec!["a", "b", "c"])])),
                metrics: Some(polled(&[("a", 199.0), ("b", 99.0), ("c", 98.0), ("d", 97.0)])),
                ..BalancerCycle::default()
            },
            BalancerCycle {
                metrics: Some(polled(&[("a", 74.0), ("b", 99.0), ("c", 98.0), ("d", 97.0)])),
                ..BalancerCycle::default()
            },
            idle(),
            idle(),
            idle(),
            expect(parts(vec![(0, vec!["a", "d"]), (1, vec!["b", "c"])])),
        ],
    );
}

#[test]
fn matching_isolation_balancer_poller_added_then_removed() {
    run_balancer(
        2,
        vec![
            observe(
                polled(&[("a", 100.0), ("b", 99.0)]),
                parts(vec![(0, vec!["a"]), (1, vec!["a"])]),
            ),
            idle(),
            idle(),
            idle(),
            expect(parts(vec![(0, vec!["a", "b"]), (1, vec!["a", "b"])])),
            BalancerCycle {
                partitions: Some(parts(vec![(0, vec!["a", "b"]), (1, vec!["a", "b"])])),
                metrics: Some(group_metrics(&[("a", 100.0), ("b", 99.0)], &[("a", true)])),
                ..BalancerCycle::default()
            },
            idle(),
            idle(),
            idle(),
            expect(parts(vec![(0, vec!["a"]), (1, vec!["a"])])),
            BalancerCycle {
                partitions: Some(parts(vec![(0, vec!["a"]), (1, vec!["a"])])),
                ..BalancerCycle::default()
            },
        ],
    );
}

#[test]
fn matching_isolation_balancer_ensure_minimum_partitions() {
    run_balancer(
        2,
        vec![BalancerCycle {
            metrics: Some(polled(&[("a", 2.0), ("b", 1.0)])),
            partitions: Some(parts(vec![(0, vec!["a"]), (1, vec!["b"])])),
            expected: Some(parts(vec![(0, vec!["a", "b"]), (1, vec!["a", "b"])])),
            ..BalancerCycle::default()
        }],
    );
}

#[test]
fn matching_isolation_balancer_increase_minimum_partitions() {
    run_balancer(
        1,
        vec![
            observe(
                polled(&[("a", 2.0), ("b", 1.0)]),
                parts(vec![(0, vec!["a"]), (1, vec!["b"])]),
            ),
            BalancerCycle {
                groups_per_partition: Some(2),
                expected: Some(parts(vec![(0, vec!["a", "b"]), (1, vec!["a", "b"])])),
                ..BalancerCycle::default()
            },
        ],
    );
}

#[test]
fn matching_isolation_balancer_decrease_minimum_partitions() {
    run_balancer(
        2,
        vec![
            BalancerCycle {
                metrics: Some(polled(&[("a", 2.0), ("b", 1.0)])),
                partitions: Some(parts(vec![(0, vec!["a"]), (1, vec!["b"])])),
                expected: Some(parts(vec![(0, vec!["a", "b"]), (1, vec!["a", "b"])])),
                ..BalancerCycle::default()
            },
            BalancerCycle {
                groups_per_partition: Some(1),
                partitions: Some(parts(vec![(0, vec!["a", "b"]), (1, vec!["a", "b"])])),
                ..BalancerCycle::default()
            },
            idle(),
            idle(),
            idle(),
            // Has to go through a normal scale down operation.
            expect(parts(vec![(0, vec!["b"]), (1, vec!["a"])])),
        ],
    );
}

#[test]
fn matching_isolation_balancer_new_partition_increase_partitions_per_group() {
    run_balancer(
        2,
        vec![
            observe(
                polled(&[("a", 2.0), ("b", 1.0)]),
                parts(vec![(0, vec!["a", "b"]), (1, vec!["a", "b"])]),
            ),
            BalancerCycle {
                partitions: Some(parts(vec![(0, vec!["a", "b"]), (1, vec!["a", "b"]), (2, vec![])])),
                // The adaptive scaler gave us a new partition. Even though these groups do not need
                // more partitions, every partition must be assigned groups.
                expected: Some(parts(vec![
                    (0, vec!["a", "b"]),
                    (1, vec!["a", "b"]),
                    (2, vec!["a", "b"]),
                ])),
                ..BalancerCycle::default()
            },
        ],
    );
}

#[test]
fn matching_isolation_balancer_new_partition_move_groups() {
    run_balancer(
        2,
        vec![
            observe(
                // Even though the others are not polled, they are still active because they are assigned
                // to a partition and pollers have not been missing long enough to remove them.
                group_metrics(
                    &[("a", 99.0), ("b", 1.0), ("c", 2.0), ("d", 3.0), ("e", 4.0), ("f", 5.0), ("g", 6.0), ("h", 7.0)],
                    &[("a", true), ("b", true)],
                ),
                parts(vec![(0, vec!["a", "b"]), (1, vec!["c", "d", "e", "f", "g", "h"])]),
            ),
            BalancerCycle {
                partitions: Some(parts(vec![
                    (0, vec!["a", "b"]),
                    (1, vec!["c", "d", "e", "f", "g", "h"]),
                    (2, vec![]),
                ])),
                expected: Some(parts(vec![
                    (0, vec!["a", "b"]),
                    (1, vec!["c", "d", "e", "f"]),
                    (2, vec!["g", "h"]),
                ])),
                ..BalancerCycle::default()
            },
        ],
    );
}

#[test]
fn matching_isolation_balancer_new_partition_move_groups_from_multiple_partitions() {
    run_balancer(
        2,
        vec![
            observe(
                group_metrics(
                    &[("a", 99.0), ("b", 1.0), ("c", 2.0), ("d", 3.0), ("e", 4.0), ("f", 5.0)],
                    &[("a", true), ("b", true)],
                ),
                parts(vec![(0, vec!["a", "b", "c"]), (1, vec!["d", "e", "f"])]),
            ),
            BalancerCycle {
                partitions: Some(parts(vec![
                    (0, vec!["a", "b", "c"]),
                    (1, vec!["d", "e", "f"]),
                    (2, vec![]),
                ])),
                expected: Some(parts(vec![
                    (0, vec!["b", "c"]),
                    (1, vec!["e", "f"]),
                    (2, vec!["a", "d"]),
                ])),
                ..BalancerCycle::default()
            },
        ],
    );
}

// ---------------------------------------------------------------------------
// adaptive_scaler_test.go: TestAdaptiveScalerRun
// ---------------------------------------------------------------------------

/// One DescribeTaskList response, reduced to the fields Go's
/// `toAggregateMetrics` and `toPartitionMetrics` read.
#[derive(Default)]
struct Described {
    qps: f64,
    backlog: i64,
    empty: bool,
    write_ids: Vec<i32>,
    groups: Vec<(String, f64, bool)>,
}

impl Described {
    /// Go `toPartitionMetrics`: write membership comes from the response's
    /// PartitionConfig; the empty check depends on the dynamic config.
    fn partition_metrics(&self, id: i32, empty_check: bool) -> PartitionMetrics {
        PartitionMetrics {
            qps: self.qps,
            read_only: !self.write_ids.contains(&id),
            empty: if empty_check { self.empty } else { self.backlog == 0 },
        }
    }
}

/// Go `withPartitionsAndQPS(n, qps)`.
fn qps_resp(partitions: i32, qps: f64) -> Described {
    Described {
        qps,
        write_ids: (0..partitions).collect(),
        ..Described::default()
    }
}

/// Go `withPartitionsAndBacklog(numRead, numWrite, backlog)`. The read count
/// does not reach the scaler, so it is omitted.
fn backlog_resp(write: i32, backlog: i64) -> Described {
    Described {
        backlog,
        write_ids: (0..write).collect(),
        ..Described::default()
    }
}

/// Go `withConfigAndQPS(config, qpsByGroup)`: every listed group has one poller.
fn config_resp(write_ids: &[i32], groups: &[(&str, f64)]) -> Described {
    Described {
        qps: groups.iter().map(|(_, qps)| *qps).sum(),
        write_ids: write_ids.to_vec(),
        groups: groups
            .iter()
            .map(|(group, qps)| (group.to_string(), *qps, true))
            .collect(),
        ..Described::default()
    }
}

fn d(id: i32, resp: Described) -> (i32, Option<Described>) {
    (id, Some(resp))
}

/// Go `mockDescribeTaskListWithErr`.
fn fail(id: i32) -> (i32, Option<Described>) {
    (id, None)
}

/// One Go `run()` tick: the scripted TaskListPartitionConfig return (`None`
/// is Go's default `{0: {}}` maps), the DescribeTaskList responses by partition
/// (`None` is an RPC error), and the expected update.
#[derive(Default)]
struct ScalerCycle {
    current: Option<(PartitionMap, PartitionMap)>,
    describes: Vec<(i32, Option<Described>)>,
    expected: Option<(PartitionMap, PartitionMap)>,
}

fn tick(
    describes: Vec<(i32, Option<Described>)>,
    expected: Option<(PartitionMap, PartitionMap)>,
) -> ScalerCycle {
    ScalerCycle {
        current: None,
        describes,
        expected,
    }
}

fn tick_with(
    current: (PartitionMap, PartitionMap),
    describes: Vec<(i32, Option<Described>)>,
    expected: Option<(PartitionMap, PartitionMap)>,
) -> ScalerCycle {
    ScalerCycle {
        current: Some(current),
        describes,
        expected,
    }
}

struct ScalerCase {
    isolation: bool,
    empty_check: bool,
    min_write: i64,
    cycles: Vec<ScalerCycle>,
}

fn non_isolated(cycles: Vec<ScalerCycle>) -> ScalerCase {
    ScalerCase {
        isolation: false,
        empty_check: false,
        min_write: 1,
        cycles,
    }
}

fn isolated(cycles: Vec<ScalerCycle>) -> ScalerCase {
    ScalerCase {
        isolation: true,
        ..non_isolated(cycles)
    }
}

/// Go `TestAdaptiveScalerRun` drives `scaler.run()` once per cycle and advances
/// the mocked clock by one second after each. `describe_trailing` selects the
/// trailing-partition describe source: `true` serves the scripted describes,
/// as Go does; `false` reproduces the production `|_| None` closure.
fn run_scaler_case(case: ScalerCase, describe_trailing: bool) {
    let config = ScalerConfig {
        enabled: true,
        cache_enabled: true,
        isolation_assignment: case.isolation,
        empty_check: case.empty_check,
        min_write_partitions: case.min_write,
        upscale_sustain_nanos: SECOND_NANOS,
        downscale_sustain_nanos: SECOND_NANOS,
        group_upscale_sustain_nanos: SECOND_NANOS,
        group_downscale_sustain_nanos: SECOND_NANOS,
        group_has_pollers_sustain_nanos: SECOND_NANOS,
        group_no_pollers_sustain_nanos: SECOND_NANOS,
        ..ScalerConfig::default()
    };
    let mut scaler = Scaler::new();
    let mut now = 0;
    for (index, cycle) in case.cycles.into_iter().enumerate() {
        let current = cycle.current.unwrap_or_else(|| (even(1), even(1)));
        let describes: HashMap<i32, Option<Described>> = cycle.describes.into_iter().collect();
        let actual = run_scaler_cycle(&mut scaler, &config, now, &current, &describes, describe_trailing);
        assert_eq!(actual, cycle.expected, "cycle {index} - update mismatch");
        now += SECOND_NANOS;
    }
}

/// Go `scaler.run()` for one tick: collect metrics as Go does, then tick.
fn run_scaler_cycle(
    scaler: &mut Scaler,
    config: &ScalerConfig,
    now: i64,
    current: &(PartitionMap, PartitionMap),
    describes: &HashMap<i32, Option<Described>>,
    describe_trailing: bool,
) -> Option<(PartitionMap, PartitionMap)> {
    let (read, write) = current;
    let response = |id: i32| describes.get(&id).and_then(|resp| resp.as_ref());
    let metrics = if config.isolation_assignment {
        let mut metrics = AggregateMetrics {
            isolation_enabled: true,
            ..AggregateMetrics::default()
        };
        let mut ids: Vec<i32> = read.keys().copied().collect();
        ids.sort();
        for id in ids {
            assert!(
                describes.contains_key(&id),
                "cycle script has no DescribeTaskList for read partition {id}"
            );
            // Go fetchMetricsFromPartitions: one failed describe fails the tick, and run() resets the sustains.
            let Some(resp) = response(id) else {
                scaler.reset();
                return None;
            };
            metrics.total_qps += resp.qps;
            for (group, qps, polled) in &resp.groups {
                *metrics.qps_by_group.entry(group.clone()).or_insert(0.0) += *qps;
                *metrics.has_pollers_by_group.entry(group.clone()).or_insert(false) |= *polled;
            }
            metrics
                .by_partition
                .insert(id, resp.partition_metrics(id, config.empty_check));
        }
        metrics
    } else {
        // Go assumeEvenQPS: only the root partition is described up front.
        let root = response(0).expect("cycle script describes partition 0");
        AggregateMetrics {
            total_qps: root.qps * write.len() as f64,
            ..AggregateMetrics::default()
        }
    };
    if describe_trailing {
        return scaler.tick(config, now, read, write, &metrics, |id| {
            response(id).map(|resp| resp.partition_metrics(id, config.empty_check))
        });
    }
    let prepared = scaler.tick_prepare(config, now, write, &metrics);
    let (new_read, changed) = tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(super::adjust_read_partitions_async(
        &metrics,
        read,
        &prepared.write,
        |id| async move {
            response(id).map(|resp| resp.partition_metrics(id, config.empty_check))
        },
    ));
    scaler.tick_finish(prepared, read, &metrics, new_read, changed)
}

// Go TestAdaptiveScalerLifecycle (adaptive_scaler_test.go:87): Start and Stop are idempotent.
// Rust adds the observable state: one loop per start, and no restart after stop.
#[tokio::test]
async fn matching_adaptive_scaler_lifecycle_start_stop_is_idempotent() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    fn periodic(runs: std::sync::Arc<AtomicUsize>) -> impl std::future::Future<Output = ()> + Send + 'static {
        async move {
            runs.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<()>().await
        }
    }
    let runs = std::sync::Arc::new(AtomicUsize::new(0));
    let daemon = ScalerDaemon::default();
    assert!(!daemon.is_started());
    daemon.start(periodic(runs.clone()));
    daemon.start(periodic(runs.clone()));
    tokio::task::yield_now().await;
    assert!(daemon.is_started());
    assert_eq!(runs.load(Ordering::SeqCst), 1, "second Start must not spawn a loop");
    daemon.stop();
    daemon.stop();
    assert!(!daemon.is_started());
    daemon.start(periodic(runs.clone()));
    tokio::task::yield_now().await;
    assert!(!daemon.is_started(), "a stopped daemon must not restart");
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

#[test]
fn matching_adaptive_scaler_no_op() {
    run_scaler_case(non_isolated(vec![tick(vec![d(0, qps_resp(1, 0.0))], None)]), true);
}

#[test]
fn matching_adaptive_scaler_overload_start() {
    run_scaler_case(non_isolated(vec![tick(vec![d(0, qps_resp(1, 300.0))], None)]), true);
}

#[test]
fn matching_adaptive_scaler_overload_sustained() {
    run_scaler_case(
        non_isolated(vec![
            tick(vec![d(0, qps_resp(1, 300.0))], None),
            tick(vec![d(0, qps_resp(1, 300.0))], Some((even(2), even(2)))),
        ]),
        true,
    );
}

#[test]
fn matching_adaptive_scaler_overload_fluctuate() {
    run_scaler_case(
        non_isolated(vec![
            tick(vec![d(0, qps_resp(1, 300.0))], None),
            tick(vec![d(0, qps_resp(1, 100.0))], None),
            tick(vec![d(0, qps_resp(1, 300.0))], None),
            tick(vec![d(0, qps_resp(1, 100.0))], None),
        ]),
        true,
    );
}

#[test]
fn matching_adaptive_scaler_underload_start() {
    run_scaler_case(
        non_isolated(vec![tick_with(
            (even(10), even(10)),
            vec![d(0, qps_resp(10, 0.0))],
            None,
        )]),
        true,
    );
}

#[test]
fn matching_adaptive_scaler_underload_sustained() {
    run_scaler_case(
        non_isolated(vec![
            tick_with((even(10), even(10)), vec![d(0, qps_resp(10, 0.0))], None),
            tick_with(
                (even(10), even(10)),
                vec![d(0, qps_resp(10, 0.0)), d(9, qps_resp(10, 0.0))],
                Some((even(10), even(1))),
            ),
        ]),
        true,
    );
}

/// Shared by the drain cases: scale write 10 to 1, then drain read partitions.
fn underload_then_drain_cycles() -> Vec<ScalerCycle> {
    vec![
        tick_with((even(10), even(10)), vec![d(0, qps_resp(10, 0.0))], None),
        tick_with(
            (even(10), even(10)),
            vec![d(0, qps_resp(10, 0.0)), d(9, qps_resp(10, 0.0))],
            Some((even(10), even(1))),
        ),
        tick_with(
            (even(10), even(1)),
            vec![
                d(0, backlog_resp(1, 0)),
                d(9, backlog_resp(1, 0)),
                d(8, backlog_resp(1, 0)),
                d(7, backlog_resp(1, 0)),
                d(6, backlog_resp(1, 0)),
                d(5, backlog_resp(1, 0)),
                d(4, backlog_resp(1, 1)),
            ],
            Some((even(5), even(1))),
        ),
    ]
}

#[test]
fn matching_adaptive_scaler_underload_sustained_then_drain() {
    run_scaler_case(non_isolated(underload_then_drain_cycles()), true);
}

#[test]
fn matching_adaptive_scaler_production_drain_describes_trailing_partitions() {
    // Production path: trailing partitions come from an async describe source
    // (a fake in-process stand-in for the DescribeTaskList RPC), not `|_| None`.
    run_scaler_case(non_isolated(underload_then_drain_cycles()), false);
}

#[test]
fn matching_adaptive_scaler_underload_sustained_then_drain_require_empty() {
    run_scaler_case(
        ScalerCase {
            empty_check: true,
            ..non_isolated(vec![
                tick_with(
                    (even(3), even(3)),
                    vec![d(0, qps_resp(3, 0.0))],
                    None,
                ),
                // Partition 2 is checked but not drained: it has not received the update yet.
                tick_with(
                    (even(3), even(3)),
                    vec![d(0, qps_resp(3, 0.0)), d(2, qps_resp(3, 0.0))],
                    Some((even(3), even(1))),
                ),
                // 2 is drained because Empty is true, and BacklogCountHint is ignored.
                // 1 is not drained because Empty is false, even though BacklogCountHint is 0.
                tick_with(
                    (even(3), even(1)),
                    vec![
                        d(0, backlog_resp(1, 0)),
                        d(
                            2,
                            Described {
                                backlog: 1000,
                                empty: true,
                                write_ids: vec![0],
                                ..Described::default()
                            },
                        ),
                        d(
                            1,
                            Described {
                                backlog: 0,
                                empty: false,
                                write_ids: vec![0],
                                ..Described::default()
                            },
                        ),
                    ],
                    Some((even(2), even(1))),
                ),
            ])
        },
        true,
    );
}

#[test]
fn matching_adaptive_scaler_overload_but_no_fluctuation() {
    run_scaler_case(
        non_isolated(vec![
            tick(vec![d(0, qps_resp(1, 210.0))], None),
            tick(vec![d(0, qps_resp(1, 210.0))], Some((even(2), even(2)))),
            // Not overloaded with 1 partition, but avoid fluctuation, so do not scale down.
            tick_with((even(2), even(2)), vec![d(0, qps_resp(2, 190.0))], None),
            tick_with((even(2), even(2)), vec![d(0, qps_resp(2, 190.0))], None),
        ]),
        true,
    );
}

#[test]
fn matching_adaptive_scaler_isolation_aggregate_metrics_to_scale_up() {
    run_scaler_case(
        isolated(vec![
            tick_with(
                (even(2), even(2)),
                vec![d(0, qps_resp(2, 1.0)), d(1, qps_resp(2, 400.0))],
                None,
            ),
            tick_with(
                (even(2), even(2)),
                vec![d(0, qps_resp(2, 1.0)), d(1, qps_resp(2, 400.0))],
                Some((even(3), even(3))),
            ),
        ]),
        true,
    );
}

#[test]
fn matching_adaptive_scaler_isolation_aggregate_metrics_to_scale_down() {
    run_scaler_case(
        isolated(vec![
            tick_with(
                (even(3), even(3)),
                vec![
                    d(0, qps_resp(3, 200.0)),
                    d(1, qps_resp(3, 49.0)),
                    d(2, qps_resp(3, 50.0)),
                ],
                None,
            ),
            tick_with(
                (even(3), even(3)),
                vec![
                    d(0, qps_resp(3, 200.0)),
                    d(1, qps_resp(3, 49.0)),
                    d(2, qps_resp(3, 50.0)),
                ],
                Some((even(3), even(2))),
            ),
            tick_with(
                (even(3), even(2)),
                vec![
                    d(0, qps_resp(2, 200.0)),
                    d(1, qps_resp(2, 99.0)),
                    d(2, backlog_resp(2, 0)),
                ],
                Some((even(2), even(2))),
            ),
        ]),
        true,
    );
}

#[test]
fn matching_adaptive_scaler_isolation_scale_group_up() {
    let current = both(vec![(0, vec!["a", "b"]), (1, vec!["c", "d"])]);
    run_scaler_case(
        isolated(vec![
            tick_with(
                current.clone(),
                vec![
                    d(0, config_resp(&[0, 1], &[("a", 101.0), ("b", 20.0)])),
                    d(1, config_resp(&[0, 1], &[("c", 20.0), ("d", 20.0)])),
                ],
                None,
            ),
            tick_with(
                current,
                vec![
                    d(0, config_resp(&[0, 1], &[("a", 101.0), ("b", 20.0)])),
                    d(1, config_resp(&[0, 1], &[("c", 20.0), ("d", 20.0)])),
                ],
                Some(both(vec![(0, vec!["a", "b"]), (1, vec!["a", "c", "d"])])),
            ),
        ]),
        true,
    );
}

#[test]
fn matching_adaptive_scaler_isolation_scale_group_down() {
    let current = both(vec![(0, vec!["a", "b"]), (1, vec!["a", "c", "d"])]);
    run_scaler_case(
        isolated(vec![
            tick_with(
                current.clone(),
                vec![
                    d(0, config_resp(&[0, 1], &[("a", 74.0), ("b", 50.0)])),
                    d(1, config_resp(&[0, 1], &[("c", 50.0), ("d", 50.0)])),
                ],
                None,
            ),
            tick_with(
                current,
                vec![
                    d(0, config_resp(&[0, 1], &[("a", 74.0), ("b", 50.0)])),
                    d(1, config_resp(&[0, 1], &[("c", 50.0), ("d", 50.0)])),
                ],
                Some(both(vec![(0, vec!["a", "b"]), (1, vec!["c", "d"])])),
            ),
        ]),
        true,
    );
}

#[test]
fn matching_adaptive_scaler_isolation_scale_partitions_down() {
    let current = both(vec![(0, vec!["a", "b"]), (1, vec!["a", "c", "d"])]);
    run_scaler_case(
        isolated(vec![
            tick_with(
                current.clone(),
                vec![
                    d(0, config_resp(&[0, 1], &[("a", 74.0), ("b", 20.0)])),
                    d(1, config_resp(&[0, 1], &[("c", 20.0), ("d", 20.0)])),
                ],
                None,
            ),
            tick_with(
                current,
                vec![
                    d(0, config_resp(&[0, 1], &[("a", 74.0), ("b", 20.0)])),
                    d(1, config_resp(&[0, 1], &[("c", 20.0), ("d", 20.0)])),
                ],
                Some((
                    parts(vec![(0, vec![]), (1, vec!["a", "c", "d"])]),
                    parts(vec![(0, vec![])]),
                )),
            ),
            // Drain partition 1.
            tick_with(
                (
                    parts(vec![(0, vec![]), (1, vec!["c", "d"])]),
                    parts(vec![(0, vec![])]),
                ),
                vec![
                    d(0, config_resp(&[0, 1], &[("a", 74.0), ("b", 20.0)])),
                    d(1, config_resp(&[0], &[])),
                ],
                Some((parts(vec![(0, vec![])]), parts(vec![(0, vec![])]))),
            ),
        ]),
        true,
    );
}

#[test]
fn matching_adaptive_scaler_isolation_error_calling_describe_is_no_op() {
    run_scaler_case(
        isolated(vec![
            // Underload would normally pass the sustain period, but the error resets it.
            tick_with(
                (even(3), even(3)),
                vec![d(0, qps_resp(3, 0.0)), d(1, qps_resp(3, 0.0)), fail(2)],
                None,
            ),
            tick_with(
                (even(3), even(3)),
                vec![
                    d(0, qps_resp(3, 0.0)),
                    d(1, qps_resp(3, 0.0)),
                    d(2, qps_resp(3, 0.0)),
                ],
                None,
            ),
            // Now the scaler can scale down.
            tick_with(
                (even(3), even(3)),
                vec![
                    d(0, qps_resp(3, 0.0)),
                    d(1, qps_resp(3, 0.0)),
                    d(2, qps_resp(3, 0.0)),
                ],
                Some((even(3), even(1))),
            ),
        ]),
        true,
    );
}

#[test]
fn matching_adaptive_scaler_set_minimum_write_partitions_from_dynamic_config() {
    run_scaler_case(
        ScalerCase {
            min_write: 4,
            ..non_isolated(vec![tick(
                vec![d(0, qps_resp(1, 0.0))],
                Some((even(4), even(4))),
            )])
        },
        true,
    );
}

#[test]
fn matching_adaptive_scaler_clamp_malformed_minimum_write_partitions_to_one_on_downscale() {
    run_scaler_case(
        ScalerCase {
            min_write: -1,
            ..non_isolated(vec![
                tick_with((even(10), even(10)), vec![d(0, qps_resp(10, 0.0))], None),
                tick_with(
                    (even(10), even(10)),
                    vec![d(0, qps_resp(10, 0.0)), d(9, qps_resp(10, 0.0))],
                    Some((even(10), even(1))),
                ),
            ])
        },
        true,
    );
}

/// Differs from the former re-sorting loop. Go `ensureMinimumGroupsPerPartition`
/// sorts once ([P0, P1, P3, P2] by group count) and walks two pointers, so the
/// second donor is P2 (hi stays on P2 while it has more than the minimum).
/// Derivation: memory/evidence/matching-scaler-fix-20261023/notes.md.
#[test]
fn matching_isolation_balancer_ensure_minimum_single_pass_keeps_donor() {
    let sizes: HashMap<String, f64> = [
        ("g1", 1.0), ("g2", 2.0), ("g3", 3.0), ("g4", 4.0), ("g5", 5.0), ("g6", 7.0),
        ("h1", 10.0), ("h2", 11.0), ("h3", 12.0), ("h4", 13.0), ("k", 0.5),
    ]
    .into_iter()
    .map(|(group, size)| (group.to_owned(), size))
    .collect();
    let set = |groups: &[&str]| -> std::collections::HashSet<String> {
        groups.iter().map(|group| (*group).to_owned()).collect()
    };
    let mut by_partition: HashMap<i32, std::collections::HashSet<String>> = HashMap::from([
        (0, set(&[])),
        (1, set(&["k"])),
        (2, set(&["g1", "g2", "g3", "g4", "g5", "g6"])),
        (3, set(&["h1", "h2", "h3", "h4"])),
    ]);
    assert!(super::ensure_minimum_groups(2, &sizes, &mut by_partition));
    assert_eq!(by_partition[&0], set(&["g6", "g4"]));
    assert_eq!(by_partition[&1], set(&["k", "g5"]));
    assert_eq!(by_partition[&2], set(&["g1", "g2", "g3"]));
    assert_eq!(by_partition[&3], set(&["h1", "h2", "h3", "h4"]));
}
