//! Go `client/matching` load-balancer dynamic config for History's matching
//! client (`clientfactory.NewMatchingClientWithTimeout`).
use std::sync::Arc;

use cadence_common::matching_lb::LbDynamicConfig;

use crate::config_collection::{DynamicClient, Filters};
use crate::engine::deps::DomainCache as _;
use crate::wiring::resources::domain_cache::HistoryDomainCache;

pub struct HistoryLbConfig {
    pub dynamic: Option<Arc<dyn DynamicClient>>,
    pub domains: Arc<HistoryDomainCache>,
}

impl HistoryLbConfig {
    fn value(&self, key: &str, domain: &str, task_list: Option<&str>) -> Option<serde_json::Value> {
        let filters = Filters {
            domain_name: Some(domain.to_owned()),
            task_list: task_list.map(str::to_owned),
            ..Default::default()
        };
        self.dynamic.as_ref()?.get_value(key, &filters)
    }

    fn int(&self, key: &str, domain: &str, task_list: &str, default: i64) -> i64 {
        self.value(key, domain, Some(task_list)).and_then(|v| v.as_i64()).unwrap_or(default)
    }
}

impl LbDynamicConfig for HistoryLbConfig {
    fn domain_name(&self, domain_id: &str) -> Option<String> {
        self.domains.get_domain_name(domain_id).ok()
    }
    fn num_read_partitions(&self, domain: &str, task_list: &str, _task_type: i32) -> i64 {
        self.int("matching.numTasklistReadPartitions", domain, task_list, 1)
    }
    fn num_write_partitions(&self, domain: &str, task_list: &str, _task_type: i32) -> i64 {
        self.int("matching.numTasklistWritePartitions", domain, task_list, 1)
    }
    fn read_from_cache(&self, domain: &str, task_list: &str, _task_type: i32) -> bool {
        self.value("matching.enableGetNumberOfPartitionsFromCache", domain, Some(task_list))
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }
    fn strategy(&self, domain: &str, task_list: &str, _task_type: i32) -> String {
        self.value("system.tasklistLoadBalancerStrategy", domain, Some(task_list))
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_else(|| "weighted".to_owned())
    }
    fn isolation_assignment_enabled(&self, domain: &str) -> bool {
        self.value("matching.enablePartitionIsolationGroupAssignment", domain, None)
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }
}
