//! Byte-exact variant of [`crate::poller`] (Go `service/matching/poller/manager.go`).
//! Poll IDs, identities and isolation groups are Go strings: keys compare as raw
//! bytes, so `0xff`, `0xfe` and U+FFFD never alias. Behavior otherwise mirrors
//! the legacy `Manager`; timestamps are Unix nanos.
use crate::go_string::GoString;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

const TTL: i64 = 300_000_000_000;
const CAPACITY: usize = 5000;

type Callback = Arc<dyn Fn() + Send + Sync>;

#[derive(Clone, Debug, PartialEq)]
pub struct Info {
    pub identity: GoString,
    pub rate_per_second: f64,
    pub isolation_group: GoString,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HistoricalInfo {
    pub identity: GoString,
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
    history: HashMap<GoString, Entry>,
    order: VecDeque<GoString>,
    outstanding: HashMap<GoString, Info>,
    cancellations: HashMap<GoString, Callback>,
    group_count: HashMap<GoString, usize>,
    last_end: Option<i64>,
    group_end: HashMap<GoString, i64>,
    history_callback: Option<Callback>,
}

impl Manager {
    /// Go onHistoryUpdatedFunc: runs after every history-cache update from start/end.
    pub fn set_history_callback(&mut self, callback: Callback) {
        self.history_callback = Some(callback);
    }

    fn notify_history(&self) {
        if let Some(callback) = &self.history_callback {
            callback();
        }
    }

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

    pub fn start(&mut self, id: &[u8], info: Info, now: i64) {
        if !info.identity.is_empty() {
            self.put(info.clone(), now, !id.is_empty());
            self.notify_history();
        }
        if !id.is_empty() {
            if !info.isolation_group.is_empty() {
                *self
                    .group_count
                    .entry(info.isolation_group.clone())
                    .or_default() += 1;
            }
            self.outstanding.insert(GoString::from(id), info);
        }
    }

    pub fn set_cancel(&mut self, id: &[u8], cancel: Callback) {
        if !id.is_empty() {
            self.cancellations.insert(GoString::from(id), cancel);
        }
    }

    /// Go CancelPoll: true when the poll is outstanding (its cancel callback, if set, runs).
    pub fn cancel(&self, id: &[u8]) -> bool {
        if let Some(cancel) = self.cancellations.get(id) {
            cancel();
        }
        self.outstanding.contains_key(id)
    }

    pub fn end(&mut self, id: &[u8], now: i64) {
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
                self.notify_history();
            }
        }
    }

    pub fn contains_poll(&self, id: &[u8]) -> bool {
        self.outstanding.contains_key(id)
    }

    pub fn has_after(&self, after: i64) -> bool {
        !self.outstanding.is_empty() || self.last_end.is_some_and(|time| time > after)
    }

    pub fn has_group_after(&self, group: &[u8], after: i64) -> bool {
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

    pub fn count_by_group(&mut self, now: i64, after: i64) -> HashMap<GoString, usize> {
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    const KEYS: [&[u8]; 3] = [b"\xff", b"\xfe", "\u{fffd}".as_bytes()];

    fn info(identity: &[u8], group: &[u8]) -> Info {
        Info {
            identity: identity.into(),
            rate_per_second: 1.0,
            isolation_group: group.into(),
        }
    }

    #[test]
    fn poll_ids_do_not_alias() {
        let mut manager = Manager::default();
        manager.start(KEYS[0], info(b"", b""), 1);
        assert!(manager.contains_poll(KEYS[0]));
        assert!(!manager.contains_poll(KEYS[1]));
        assert!(!manager.contains_poll(KEYS[2]));
        let cancelled = Arc::new(AtomicUsize::new(0));
        let counter = cancelled.clone();
        manager.set_cancel(
            KEYS[0],
            Arc::new(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            }),
        );
        assert!(!manager.cancel(KEYS[1]));
        assert_eq!(cancelled.load(Ordering::SeqCst), 0);
        assert!(manager.cancel(KEYS[0]));
        assert_eq!(cancelled.load(Ordering::SeqCst), 1);
        manager.end(KEYS[1], 2);
        assert!(manager.contains_poll(KEYS[0]));
        manager.end(KEYS[0], 3);
        assert!(!manager.contains_poll(KEYS[0]));
    }

    #[test]
    fn identities_and_groups_are_distinct() {
        let mut manager = Manager::default();
        for (index, key) in KEYS.iter().enumerate() {
            manager.start(&[b'p', index as u8], info(key, key), 10);
        }
        assert_eq!(manager.count_at(10), 3);
        let listed = manager.list(10);
        assert_eq!(listed.len(), 3);
        for key in KEYS {
            assert!(listed.iter().any(|entry| entry.identity.as_bytes() == key));
        }
        let groups = manager.count_by_group(10, 0);
        assert_eq!(groups.len(), 3);
        assert!(groups.values().all(|count| *count == 1));
        assert!(manager.has_group_after(KEYS[1], 100));
        manager.end(&[b'p', 1], 20);
        assert!(!manager.has_group_after(KEYS[1], 100));
        assert!(manager.has_group_after(KEYS[1], 10));
        assert!(!manager.has_group_after(b"\xfd", 0));
        assert!(manager.has_group_after(KEYS[0], 100));
    }

    #[test]
    fn ttl_and_history_callback() {
        let mut manager = Manager::default();
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        manager.set_history_callback(Arc::new(move || {
            counter.fetch_add(1, Ordering::SeqCst);
        }));
        manager.start(b"a", info(KEYS[0], b""), 10);
        manager.start(b"b", info(KEYS[1], b""), 10);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(manager.count_at(10 + TTL), 2);
        assert_eq!(manager.count_at(11 + TTL), 0);
    }
}
