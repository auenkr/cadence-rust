//! Go `client/matching` load-balancer dynamic config for History's matching
//! client (`clientfactory.NewMatchingClientWithTimeout`). Partition counts,
//! cache flag and strategy are `...FilteredByTaskListInfo` (domain, task list,
//! task type), as in Go.
use std::sync::Arc;

use cadence_common::matching_lb::LbDynamicConfig;

use crate::config_collection::{DynamicClient, Filters};
use crate::engine::deps::DomainCache as _;
use crate::wiring::resources::domain_cache::HistoryDomainCache;

pub struct HistoryLbConfig {
    pub dynamic: Option<Arc<dyn DynamicClient>>,
    pub domains: Arc<HistoryDomainCache>,
}

/// Go `TaskListFilter` + `TaskTypeFilter` (+ `DomainFilter`).
pub(crate) fn task_list_filters(domain: &str, task_list: &str, task_type: i32) -> Filters {
    Filters {
        domain_name: Some(domain.to_owned()),
        task_list: Some(task_list.to_owned()),
        task_type: Some(task_type as i64),
        ..Default::default()
    }
}

impl HistoryLbConfig {
    fn get(&self, key: &str, filters: &Filters) -> Option<serde_json::Value> {
        self.dynamic.as_ref()?.get_value(key, filters)
    }

    fn tl(&self, key: &str, domain: &str, task_list: &str, task_type: i32) -> Option<serde_json::Value> {
        self.get(key, &task_list_filters(domain, task_list, task_type))
    }
}

impl LbDynamicConfig for HistoryLbConfig {
    fn domain_name(&self, domain_id: &str) -> Option<String> {
        self.domains.get_domain_name(domain_id).ok()
    }
    fn num_read_partitions(&self, domain: &str, task_list: &str, task_type: i32) -> i64 {
        self.tl("matching.numTasklistReadPartitions", domain, task_list, task_type).and_then(|v| v.as_i64()).unwrap_or(1)
    }
    fn num_write_partitions(&self, domain: &str, task_list: &str, task_type: i32) -> i64 {
        self.tl("matching.numTasklistWritePartitions", domain, task_list, task_type).and_then(|v| v.as_i64()).unwrap_or(1)
    }
    fn read_from_cache(&self, domain: &str, task_list: &str, task_type: i32) -> bool {
        self.tl("matching.enableGetNumberOfPartitionsFromCache", domain, task_list, task_type)
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }
    fn strategy(&self, domain: &str, task_list: &str, task_type: i32) -> String {
        self.tl("system.tasklistLoadBalancerStrategy", domain, task_list, task_type)
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_else(|| "weighted".to_owned())
    }
    fn isolation_assignment_enabled(&self, domain: &str) -> bool {
        let f = Filters { domain_name: Some(domain.to_owned()), ..Default::default() };
        self.get("matching.enablePartitionIsolationGroupAssignment", &f).and_then(|v| v.as_bool()).unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_list_filters_carry_task_type() {
        let f = task_list_filters("d", "tl", cadence_common::matching_lb::TASK_LIST_TYPE_ACTIVITY);
        assert_eq!(f.task_type, Some(1));
        assert_eq!(f.task_list.as_deref(), Some("tl"));
        assert_eq!(f.domain_name.as_deref(), Some("d"));
        let f = task_list_filters("d", "tl", cadence_common::matching_lb::TASK_LIST_TYPE_DECISION);
        assert_eq!(f.task_type, Some(0));
    }
}
