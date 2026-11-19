//! Go `client/matching` load-balancer dynamic config for History's matching
//! client (`clientfactory.NewMatchingClientWithTimeout`). Partition counts,
//! cache flag and strategy are `...FilteredByTaskListInfo` (domain, task list,
//! task type), as in Go.
use std::sync::Arc;

use cadence_common::matching_lb::LbDynamicConfig;

use crate::config_collection::{DynKind, DynamicClient, Filters};
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

/// Typed LB flag reads (Go `GetIntValue`/`GetBoolValue`/`GetStringValue`).
pub(crate) struct LbFlags<'a>(pub Option<&'a dyn DynamicClient>);

impl LbFlags<'_> {
    fn get(&self, key: &str, filters: &Filters, kind: DynKind) -> Option<serde_json::Value> {
        self.0?.get_typed(key, filters, kind)
    }
    fn tl(&self, key: &str, kind: DynKind, domain: &str, task_list: &str, task_type: i32) -> Option<serde_json::Value> {
        self.get(key, &task_list_filters(domain, task_list, task_type), kind)
    }
    pub(crate) fn num_read_partitions(&self, domain: &str, task_list: &str, task_type: i32) -> i64 {
        self.tl("matching.numTasklistReadPartitions", DynKind::Int, domain, task_list, task_type).and_then(|v| v.as_i64()).unwrap_or(1)
    }
    pub(crate) fn num_write_partitions(&self, domain: &str, task_list: &str, task_type: i32) -> i64 {
        self.tl("matching.numTasklistWritePartitions", DynKind::Int, domain, task_list, task_type).and_then(|v| v.as_i64()).unwrap_or(1)
    }
    pub(crate) fn read_from_cache(&self, domain: &str, task_list: &str, task_type: i32) -> bool {
        self.tl("matching.enableGetNumberOfPartitionsFromCache", DynKind::Bool, domain, task_list, task_type)
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }
    pub(crate) fn strategy(&self, domain: &str, task_list: &str, task_type: i32) -> String {
        self.tl("system.tasklistLoadBalancerStrategy", DynKind::String, domain, task_list, task_type)
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_else(|| "weighted".to_owned())
    }
    pub(crate) fn isolation_assignment_enabled(&self, domain: &str) -> bool {
        let f = Filters { domain_name: Some(domain.to_owned()), ..Default::default() };
        self.get("matching.enablePartitionIsolationGroupAssignment", &f, DynKind::Bool).and_then(|v| v.as_bool()).unwrap_or(false)
    }
}

impl HistoryLbConfig {
    fn flags(&self) -> LbFlags<'_> {
        LbFlags(self.dynamic.as_deref())
    }
}

impl LbDynamicConfig for HistoryLbConfig {
    fn domain_name(&self, domain_id: &str) -> Option<String> {
        self.domains.get_domain_name(domain_id).ok()
    }
    fn num_read_partitions(&self, domain: &str, task_list: &str, task_type: i32) -> i64 {
        self.flags().num_read_partitions(domain, task_list, task_type)
    }
    fn num_write_partitions(&self, domain: &str, task_list: &str, task_type: i32) -> i64 {
        self.flags().num_write_partitions(domain, task_list, task_type)
    }
    fn read_from_cache(&self, domain: &str, task_list: &str, task_type: i32) -> bool {
        self.flags().read_from_cache(domain, task_list, task_type)
    }
    fn strategy(&self, domain: &str, task_list: &str, task_type: i32) -> String {
        self.flags().strategy(domain, task_list, task_type)
    }
    fn isolation_assignment_enabled(&self, domain: &str) -> bool {
        self.flags().isolation_assignment_enabled(domain)
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

    /// OpenFeature/Unleash-style client: object evaluation yields JSON strings;
    /// only the typed getters parse them.
    struct StringPayloadClient;
    impl DynamicClient for StringPayloadClient {
        fn get_value(&self, key: &str, _f: &Filters) -> Option<serde_json::Value> {
            Some(serde_json::Value::String(match key {
                "matching.numTasklistReadPartitions" => "4".into(),
                "matching.numTasklistWritePartitions" => "3".into(),
                "system.tasklistLoadBalancerStrategy" => "random".into(),
                _ => "true".into(),
            }))
        }
        fn get_typed(&self, key: &str, f: &Filters, kind: DynKind) -> Option<serde_json::Value> {
            let s = self.get_value(key, f)?.as_str()?.to_owned();
            Some(match kind {
                DynKind::Int => serde_json::Value::from(s.parse::<i64>().ok()?),
                DynKind::Bool => serde_json::Value::Bool(s.parse::<bool>().ok()?),
                DynKind::String => serde_json::Value::String(s),
                _ => return None,
            })
        }
    }

    #[test]
    fn lb_config_uses_typed_getters() {
        let client = StringPayloadClient;
        let c = LbFlags(Some(&client));
        assert_eq!(c.num_read_partitions("d", "tl", 1), 4);
        assert_eq!(c.num_write_partitions("d", "tl", 1), 3);
        assert!(c.read_from_cache("d", "tl", 1));
        assert!(c.isolation_assignment_enabled("d"));
        assert_eq!(c.strategy("d", "tl", 1), "random");
        let none = LbFlags(None);
        assert_eq!(none.num_read_partitions("d", "tl", 1), 1);
        assert_eq!(none.strategy("d", "tl", 1), "weighted");
        assert!(!none.read_from_cache("d", "tl", 1));
    }
}
