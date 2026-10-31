//! Failover-manager workflow registration names from Go `failovermanager`.
pub const TASK_LIST: &str = "cadence-sys-failoverManager-tasklist";
pub const LEGACY_WORKFLOW_TYPE: &str = "cadence-sys-failoverManager-workflow";
pub const FAILOVER_V2_WORKFLOW_TYPE: &str = "cadence-sys-failover-v2-workflow";
pub const REBALANCE_WORKFLOW_TYPE: &str = "cadence-sys-rebalance-workflow";
pub const REBALANCE_V2_WORKFLOW_TYPE: &str = "cadence-sys-rebalance-v2-workflow";

#[cfg(test)]
mod tests {
    #[test]
    fn preserves_legacy_casing_and_v2_names() {
        assert_eq!(super::LEGACY_WORKFLOW_TYPE, "cadence-sys-failoverManager-workflow");
        assert_eq!(super::FAILOVER_V2_WORKFLOW_TYPE, "cadence-sys-failover-v2-workflow");
        assert_eq!(super::REBALANCE_WORKFLOW_TYPE, "cadence-sys-rebalance-workflow");
        assert_eq!(super::REBALANCE_V2_WORKFLOW_TYPE, "cadence-sys-rebalance-v2-workflow");
    }
}
