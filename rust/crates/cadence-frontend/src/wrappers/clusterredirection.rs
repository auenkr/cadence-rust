//! Cluster redirection policy constants and pure routing decisions.
//!
//! The Go frontend has three selected API allowlists. Keep the versions
//! separate: they are configuration contracts and have evolved independently.

use std::collections::HashSet;

pub const POLICY_DEFAULT: &str = "";
pub const POLICY_NOOP: &str = "noop";
pub const POLICY_SELECTED_APIS_FORWARDING: &str = "selected-apis-forwarding";
pub const POLICY_SELECTED_APIS_FORWARDING_V2: &str = "selected-apis-forwarding-v2";
pub const POLICY_ALL_DOMAIN_APIS_FORWARDING: &str = "all-domain-apis-forwarding";
pub const POLICY_ALL_DOMAIN_APIS_FORWARDING_V2: &str = "all-domain-apis-forwarding-v2";
pub const POLICY_SELECTED_APIS_FORWARDING_V3: &str = "selected-apis-forwarding-v3";
pub const POLICY_ALL_DOMAIN_APIS_FORWARDING_V3: &str = "all-domain-apis-forwarding-v3";

const SELECTED_V1: &[&str] = &[
    "StartWorkflowExecution",
    "SignalWithStartWorkflowExecution",
    "SignalWorkflowExecution",
    "RequestCancelWorkflowExecution",
    "TerminateWorkflowExecution",
    "ResetWorkflowExecution",
    "CreateSchedule",
    "DeleteSchedule",
    "UpdateSchedule",
    "PauseSchedule",
    "UnpauseSchedule",
    "BackfillSchedule",
];

const SELECTED_V2_ADDITIONS: &[&str] = &[
    "RespondActivityTaskCanceled",
    "RespondActivityTaskCanceledByID",
    "RespondActivityTaskCompleted",
    "RespondActivityTaskCompletedByID",
    "RespondActivityTaskFailed",
    "RespondActivityTaskFailedByID",
];

const SELECTED_V3_ADDITIONS: &[&str] = &[
    "StartWorkflowExecutionAsync",
    "SignalWithStartWorkflowExecutionAsync",
];

const ALLOWED_ON_DEPRECATED_DOMAIN: &[&str] = &[
    "ListWorkflowExecutions",
    "CountWorkflowExecutions",
    "ScanWorkflowExecutions",
    "TerminateWorkflowExecution",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ForwardingPolicy {
    Noop,
    SelectedV1,
    SelectedV2,
    SelectedV3,
    AllDomainV1,
    AllDomainV2,
    AllDomainV3,
}

impl ForwardingPolicy {
    /// Resolve a Go configuration policy string. Unknown values are a startup
    /// configuration error in Rust rather than Go's generator panic.
    pub fn from_name(value: &str) -> Option<Self> {
        Some(match value {
            POLICY_DEFAULT | POLICY_NOOP => Self::Noop,
            POLICY_SELECTED_APIS_FORWARDING => Self::SelectedV1,
            POLICY_SELECTED_APIS_FORWARDING_V2 => Self::SelectedV2,
            POLICY_SELECTED_APIS_FORWARDING_V3 => Self::SelectedV3,
            POLICY_ALL_DOMAIN_APIS_FORWARDING => Self::AllDomainV1,
            POLICY_ALL_DOMAIN_APIS_FORWARDING_V2 => Self::AllDomainV2,
            POLICY_ALL_DOMAIN_APIS_FORWARDING_V3 => Self::AllDomainV3,
            _ => return None,
        })
    }

    pub fn is_all_domain(self) -> bool {
        matches!(self, Self::AllDomainV1 | Self::AllDomainV2 | Self::AllDomainV3)
    }

    pub fn allows_api(self, api: &str) -> bool {
        match self {
            Self::Noop => false,
            Self::SelectedV1 | Self::AllDomainV1 => SELECTED_V1.contains(&api),
            Self::SelectedV2 | Self::AllDomainV2 => {
                SELECTED_V1.contains(&api) || SELECTED_V2_ADDITIONS.contains(&api)
            }
            Self::SelectedV3 | Self::AllDomainV3 => {
                SELECTED_V1.contains(&api)
                    || SELECTED_V2_ADDITIONS.contains(&api)
                    || SELECTED_V3_ADDITIONS.contains(&api)
            }
        }
    }

    /// Go routes all APIs for all-domain policies when no target cluster is
    /// configured or when the configured target is the active cluster. In
    /// other cases it falls back to the selected API allowlist.
    pub fn routes_to_active_cluster(
        self,
        api: &str,
        strong_consistency: bool,
        active_active_domain: bool,
        configured_target: &str,
        active_cluster: &str,
    ) -> bool {
        if self == Self::Noop {
            return false;
        }
        if active_active_domain && strong_consistency {
            return true;
        }
        if self.is_all_domain()
            && (configured_target.is_empty() || configured_target == active_cluster)
        {
            return true;
        }
        self.allows_api(api)
    }
}

pub fn allowed_on_deprecated_domain(api: &str) -> bool {
    ALLOWED_ON_DEPRECATED_DOMAIN.contains(&api)
}

/// Construct the exact set when call sites need membership lookup repeatedly.
pub fn api_allowlist(policy: ForwardingPolicy) -> HashSet<&'static str> {
    match policy {
        ForwardingPolicy::Noop => HashSet::new(),
        ForwardingPolicy::SelectedV1 | ForwardingPolicy::AllDomainV1 => {
            SELECTED_V1.iter().copied().collect()
        }
        ForwardingPolicy::SelectedV2 | ForwardingPolicy::AllDomainV2 => SELECTED_V1
            .iter()
            .chain(SELECTED_V2_ADDITIONS)
            .copied()
            .collect(),
        ForwardingPolicy::SelectedV3 | ForwardingPolicy::AllDomainV3 => SELECTED_V1
            .iter()
            .chain(SELECTED_V2_ADDITIONS)
            .chain(SELECTED_V3_ADDITIONS)
            .copied()
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowlists_match_go_policy_versions() {
        assert_eq!(api_allowlist(ForwardingPolicy::SelectedV1).len(), 12);
        assert!(!ForwardingPolicy::SelectedV1.allows_api("RespondActivityTaskCompleted"));
        assert!(ForwardingPolicy::SelectedV2.allows_api("RespondActivityTaskCompletedByID"));
        assert!(!ForwardingPolicy::SelectedV2.allows_api("StartWorkflowExecutionAsync"));
        assert!(ForwardingPolicy::SelectedV3.allows_api("StartWorkflowExecutionAsync"));
        assert!(ForwardingPolicy::SelectedV3.allows_api("SignalWithStartWorkflowExecutionAsync"));
    }

    #[test]
    fn forwarding_falls_back_to_selected_allowlist() {
        assert!(ForwardingPolicy::AllDomainV2.routes_to_active_cluster(
            "DescribeWorkflowExecution", false, true, "cluster-b", "cluster-a"
        ));
        assert!(!ForwardingPolicy::AllDomainV2.routes_to_active_cluster(
            "DescribeWorkflowExecution", false, false, "cluster-b", "cluster-a"
        ));
        assert!(ForwardingPolicy::AllDomainV2.routes_to_active_cluster(
            "StartWorkflowExecution", false, false, "cluster-b", "cluster-a"
        ));
    }

    #[test]
    fn deprecated_domain_allowlist_is_exact() {
        assert!(allowed_on_deprecated_domain("ListWorkflowExecutions"));
        assert!(allowed_on_deprecated_domain("TerminateWorkflowExecution"));
        assert!(!allowed_on_deprecated_domain("DescribeWorkflowExecution"));
    }
}
