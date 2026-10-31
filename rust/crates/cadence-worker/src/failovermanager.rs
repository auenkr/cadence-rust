//! Failover-manager workflow registration names from Go `failovermanager`.
pub const TASK_LIST: &str = "cadence-sys-failoverManager-tasklist";
pub const LEGACY_WORKFLOW_TYPE: &str = "cadence-sys-failoverManager-workflow";
pub const FAILOVER_V2_WORKFLOW_TYPE: &str = "cadence-sys-failover-v2-workflow";
pub const REBALANCE_WORKFLOW_TYPE: &str = "cadence-sys-rebalance-workflow";
pub const REBALANCE_V2_WORKFLOW_TYPE: &str = "cadence-sys-rebalance-v2-workflow";
pub const FAILOVER_ACTIVITY: &str = "cadence-sys-failover-activity";
pub const GET_DOMAINS_ACTIVITY: &str = "cadence-sys-getDomains-activity";
pub const GET_REBALANCE_DOMAINS_ACTIVITY: &str = "cadence-sys-getRebalanceDomains-activity";
pub const FAILOVER_V2_ACTIVITY: &str = "cadence-sys-failover-v2-activity";
pub const GET_DOMAINS_FOR_FAILOVER_V2_ACTIVITY: &str =
    "cadence-sys-getDomainsForFailoverV2-activity";
pub const GET_DOMAINS_FOR_REBALANCE_V2_ACTIVITY: &str =
    "cadence-sys-getDomainsForRebalanceV2-activity";

pub fn registrations() -> [crate::runtime::WorkflowRegistration; 4] {
    use crate::runtime::WorkflowRegistration;
    [
        WorkflowRegistration {
            workflow_type: LEGACY_WORKFLOW_TYPE,
            task_list: TASK_LIST,
            activities: &[FAILOVER_ACTIVITY, GET_DOMAINS_ACTIVITY],
        },
        WorkflowRegistration {
            workflow_type: REBALANCE_WORKFLOW_TYPE,
            task_list: TASK_LIST,
            activities: &[GET_REBALANCE_DOMAINS_ACTIVITY, FAILOVER_ACTIVITY],
        },
        WorkflowRegistration {
            workflow_type: FAILOVER_V2_WORKFLOW_TYPE,
            task_list: TASK_LIST,
            activities: &[FAILOVER_V2_ACTIVITY, GET_DOMAINS_FOR_FAILOVER_V2_ACTIVITY],
        },
        WorkflowRegistration {
            workflow_type: REBALANCE_V2_WORKFLOW_TYPE,
            task_list: TASK_LIST,
            activities: &[
                FAILOVER_V2_ACTIVITY,
                GET_DOMAINS_FOR_REBALANCE_V2_ACTIVITY,
            ],
        },
    ]
}

#[cfg(test)]
mod tests {
    #[test]
    fn failover_and_rebalance_registrations_match_go() {
        let registrations = super::registrations();
        assert_eq!(registrations.len(), 4);
        assert_eq!(registrations[0].workflow_type, super::LEGACY_WORKFLOW_TYPE);
        assert_eq!(registrations[0].task_list, super::TASK_LIST);
        assert_eq!(
            registrations[0].activities,
            &[super::FAILOVER_ACTIVITY, super::GET_DOMAINS_ACTIVITY]
        );
        assert_eq!(registrations[1].workflow_type, super::REBALANCE_WORKFLOW_TYPE);
        assert_eq!(
            registrations[1].activities,
            &[super::GET_REBALANCE_DOMAINS_ACTIVITY, super::FAILOVER_ACTIVITY]
        );
        assert_eq!(registrations[2].workflow_type, super::FAILOVER_V2_WORKFLOW_TYPE);
        assert_eq!(
            registrations[2].activities,
            &[
                super::FAILOVER_V2_ACTIVITY,
                super::GET_DOMAINS_FOR_FAILOVER_V2_ACTIVITY
            ]
        );
        assert_eq!(registrations[3].workflow_type, super::REBALANCE_V2_WORKFLOW_TYPE);
        assert_eq!(
            registrations[3].activities,
            &[
                super::FAILOVER_V2_ACTIVITY,
                super::GET_DOMAINS_FOR_REBALANCE_V2_ACTIVITY
            ]
        );
    }

    #[test]
    fn preserves_legacy_casing_and_v2_names() {
        assert_eq!(super::LEGACY_WORKFLOW_TYPE, "cadence-sys-failoverManager-workflow");
        assert_eq!(super::FAILOVER_V2_WORKFLOW_TYPE, "cadence-sys-failover-v2-workflow");
        assert_eq!(super::REBALANCE_WORKFLOW_TYPE, "cadence-sys-rebalance-workflow");
        assert_eq!(super::REBALANCE_V2_WORKFLOW_TYPE, "cadence-sys-rebalance-v2-workflow");
    }
}
