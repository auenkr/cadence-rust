//! State semantics of Go service/matching/poller/manager.go.
//! Callers own poll cancellation and synchronization; timestamps are Unix nanos.
use std::collections::{HashMap, VecDeque};
const TTL: i64 = 300_000_000_000;
const CAPACITY: usize = 5000;
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Info {
    pub identity: String,
    pub rate_per_second: f64,
    pub isolation_group: String,
}
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct HistoricalInfo {
    pub identity: String,
    pub rate_per_second: f64,
    pub last_access_time: i64,
}
struct Entry {
    info: Info,
    created: i64,
    outstanding: bool,
}
#[derive(Default)]
pub struct Manager {
    history: HashMap<String, Entry>,
    order: VecDeque<String>,
    outstanding: HashMap<String, Info>,
    cancellations: HashMap<String, std::sync::Arc<dyn Fn() + Send + Sync>>,
    group_count: HashMap<String, usize>,
    last_end: Option<i64>,
    group_end: HashMap<String, i64>,
}
impl Manager {
    fn put(&mut self, info: Info, now: i64, outstanding: bool) {
        let identity = info.identity.clone();
        self.order.retain(|key| key != &identity);
        if !self.history.contains_key(&identity) && self.history.len() == CAPACITY {
            if let Some(old) = self.order.pop_back() {
                self.history.remove(&old);
            }
        }
        self.order.push_front(identity.clone());
        self.history.insert(
            identity,
            Entry {
                info,
                created: now,
                outstanding,
            },
        );
    }
    pub fn start(&mut self, id: &str, info: Info, now: i64) {
        if !info.identity.is_empty() {
            self.put(info.clone(), now, !id.is_empty());
        }
        if !id.is_empty() {
            if !info.isolation_group.is_empty() {
                *self
                    .group_count
                    .entry(info.isolation_group.clone())
                    .or_default() += 1;
            }
            self.outstanding.insert(id.into(), info);
        }
    }
    pub fn set_cancel(&mut self, id: &str, cancel: std::sync::Arc<dyn Fn() + Send + Sync>) {
        if !id.is_empty() { self.cancellations.insert(id.into(), cancel); }
    }
    pub fn cancel(&self, id: &str) {
        if let Some(cancel) = self.cancellations.get(id) { cancel(); }
    }
    pub fn end(&mut self, id: &str, now: i64) {
        self.cancellations.remove(id);
        self.last_end = Some(now);
        if let Some(info) = self.outstanding.remove(id) {
            if !info.isolation_group.is_empty() {
                self.group_end.insert(info.isolation_group.clone(), now);
                *self
                    .group_count
                    .entry(info.isolation_group.clone())
                    .or_default() -= 1;
            }
            if !info.identity.is_empty() {
                self.put(info, now, false);
            }
        }
    }
    pub fn contains_poll(&self, id: &str) -> bool {
        self.outstanding.contains_key(id)
    }
    pub fn has_after(&self, after: i64) -> bool {
        !self.outstanding.is_empty() || self.last_end.is_some_and(|time| time > after)
    }
    pub fn has_group_after(&self, group: &str, after: i64) -> bool {
        self.group_count.get(group).copied().unwrap_or_default() > 0
            || self.group_end.get(group).is_some_and(|time| *time > after)
    }
    /// Go's cache Size does not eagerly evict TTL entries with ActivelyEvict=false.
    pub fn count(&self) -> usize {
        self.history.len()
    }
    /// Go cache `Size` evicts entries whose five-minute history TTL elapsed.
    pub fn count_at(&mut self, now: i64) -> usize {
        self.history
            .retain(|_, entry| now <= entry.created.saturating_add(TTL));
        self.order.retain(|key| self.history.contains_key(key));
        self.history.len()
    }
    fn recent(&mut self, now: i64, after: Option<i64>) -> Vec<&Entry> {
        self.history
            .retain(|_, entry| now <= entry.created.saturating_add(TTL));
        self.order.retain(|key| self.history.contains_key(key));
        self.order
            .iter()
            .filter_map(|key| self.history.get(key))
            .filter(|entry| after.is_none_or(|after| entry.outstanding || after < entry.created))
            .collect()
    }
    pub fn list(&mut self, now: i64) -> Vec<HistoricalInfo> {
        self.recent(now, None)
            .into_iter()
            .map(|entry| HistoricalInfo {
                identity: entry.info.identity.clone(),
                rate_per_second: entry.info.rate_per_second,
                last_access_time: entry.created,
            })
            .collect()
    }
    pub fn count_by_group(&mut self, now: i64, after: i64) -> HashMap<String, usize> {
        let mut groups = HashMap::new();
        for entry in self.recent(now, Some(after)) {
            if !entry.info.isolation_group.is_empty() {
                *groups
                    .entry(entry.info.isolation_group.clone())
                    .or_default() += 1;
            }
        }
        groups
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    #[test]
    fn original_go_poller_state_snapshots() {
        let fixtures: Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/matching-pollers.json"
        ))
        .unwrap();
        let mut manager = Manager::default();
        let mut now = 1_000_000_000;
        let mut cancelled = 0;
        let mut snapshots = Vec::new();
        for step in fixtures["steps"].as_array().unwrap() {
            let str = |key: &str| step[key].as_str().unwrap_or_default();
            match str("op") {
                "start" => manager.start(
                    str("id"),
                    Info {
                        identity: str("identity").into(),
                        rate_per_second: step["rate"].as_f64().unwrap(),
                        isolation_group: str("group").into(),
                    },
                    now,
                ),
                "end" => manager.end(str("id"), now),
                "cancel" => {
                    assert!(manager.contains_poll(str("id")));
                    cancelled += 1;
                }
                "advance" => now += step["nanos"].as_i64().unwrap(),
                "bulk" => {
                    for index in 0..step["count"].as_u64().unwrap() {
                        manager.start(
                            "",
                            Info {
                                identity: format!("bulk-{index}"),
                                rate_per_second: index as f64,
                                isolation_group: "bulk".into(),
                            },
                            now,
                        );
                    }
                }
                "snapshot" => {
                    let count_before = manager.count();
                    let list = manager.list(now);
                    let after = now + 1;
                    snapshots.push(json!({
                        "count_before": count_before, "count_after": manager.count(), "listed": list.len(),
                        "first": list.first().map(|entry| entry.identity.as_str()).unwrap_or_default(),
                        "last": list.last().map(|entry| entry.identity.as_str()).unwrap_or_default(),
                        "newest": list.first().map(|entry| entry.last_access_time).unwrap_or_default(),
                        "rate": list.first().map(|entry| entry.rate_per_second).unwrap_or_default(),
                        "cancelled": cancelled, "has_after": manager.has_after(after),
                        "groups": manager.count_by_group(now, after),
                        "a": manager.has_group_after("a", after), "b": manager.has_group_after("b", after), "c": manager.has_group_after("c", after),
                    }));
                }
                op => panic!("unknown operation {op}"),
            }
        }
        let mut expected = fixtures["snapshots"].clone();
        for snapshot in expected.as_array_mut().unwrap() {
            snapshot["rate"] = json!(snapshot["rate"].as_f64().unwrap());
        }
        assert_eq!(Value::Array(snapshots), expected);
    }

    #[test]
    fn history_size_counts_unique_identities_and_evicts_expired_entries() {
        let mut manager = Manager::default();
        let info = |identity: &str| Info {
            identity: identity.into(),
            rate_per_second: 1.0,
            isolation_group: String::new(),
        };
        manager.start("poll-1", info("same-worker"), 10);
        manager.start("poll-2", info("same-worker"), 20);
        manager.start("poll-3", info("other-worker"), 20);
        assert_eq!(manager.count_at(20), 2);
        assert_eq!(manager.count_at(20 + TTL), 2);
        assert_eq!(manager.count_at(21 + TTL), 0);
    }
}
