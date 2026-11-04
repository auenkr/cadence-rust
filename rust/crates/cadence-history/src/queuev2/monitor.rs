//! Port of queuev2/monitor.go

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use super::alert::{Alert, AlertAttributesQueuePendingTaskCount, AlertType};
use super::deps::{BoolFn, HistoryTaskCategory, IntFn};
use super::virtual_slice::{slice_key, VirtualSlice};

pub trait Monitor: Send + Sync {
    fn subscribe(&self, subscriber: mpsc::Sender<Alert>);
    fn unsubscribe(&self);
    fn get_total_pending_task_count(&self) -> i64;
    fn get_slice_pending_task_count(&self, slice: &Arc<dyn VirtualSlice>) -> i64;
    fn set_slice_pending_task_count(&self, slice: &Arc<dyn VirtualSlice>, count: i64);
    fn remove_slice(&self, slice: &Arc<dyn VirtualSlice>);
    fn resolve_alert(&self, alert_type: AlertType);
}

pub struct MonitorOptions {
    pub enable_pending_task_count_alert: BoolFn,
    pub critical_pending_task_count: IntFn,
}

struct MonitorState {
    subscriber: Option<mpsc::Sender<Alert>>,
    pending_alerts: HashSet<i32>,
    total_pending_task_count: i64,
    slice_pending_task_count: HashMap<usize, i64>,
}

pub struct MonitorImpl {
    #[allow(dead_code)]
    category: HistoryTaskCategory,
    options: MonitorOptions,
    state: Mutex<MonitorState>,
}

pub fn new_monitor(category: HistoryTaskCategory, options: MonitorOptions) -> Arc<dyn Monitor> {
    Arc::new(MonitorImpl {
        category,
        options,
        state: Mutex::new(MonitorState {
            subscriber: None,
            pending_alerts: HashSet::new(),
            total_pending_task_count: 0,
            slice_pending_task_count: HashMap::new(),
        }),
    })
}

impl Monitor for MonitorImpl {
    fn subscribe(&self, subscriber: mpsc::Sender<Alert>) {
        self.state.lock().unwrap().subscriber = Some(subscriber);
    }

    fn unsubscribe(&self) {
        self.state.lock().unwrap().subscriber = None;
    }

    fn get_total_pending_task_count(&self) -> i64 {
        self.state.lock().unwrap().total_pending_task_count
    }

    fn get_slice_pending_task_count(&self, slice: &Arc<dyn VirtualSlice>) -> i64 {
        *self
            .state
            .lock()
            .unwrap()
            .slice_pending_task_count
            .get(&slice_key(slice))
            .unwrap_or(&0)
    }

    fn set_slice_pending_task_count(&self, slice: &Arc<dyn VirtualSlice>, count: i64) {
        let mut st = self.state.lock().unwrap();
        let key = slice_key(slice);
        let current = *st.slice_pending_task_count.get(&key).unwrap_or(&0);
        st.total_pending_task_count += count - current;
        st.slice_pending_task_count.insert(key, count);

        let critical = (self.options.critical_pending_task_count)();
        if (self.options.enable_pending_task_count_alert)()
            && critical > 0
            && st.total_pending_task_count > critical
        {
            let alert = Alert {
                alert_type: AlertType::QueuePendingTaskCount,
                alert_attributes_queue_pending_task_count: Some(
                    AlertAttributesQueuePendingTaskCount {
                        current_pending_task_count: st.total_pending_task_count,
                        critical_pending_task_count: critical,
                    },
                ),
            };
            Self::send_alert_locked(&mut st, alert);
        }
    }

    fn remove_slice(&self, slice: &Arc<dyn VirtualSlice>) {
        let mut st = self.state.lock().unwrap();
        if let Some(c) = st.slice_pending_task_count.remove(&slice_key(slice)) {
            st.total_pending_task_count -= c;
        }
    }

    fn resolve_alert(&self, alert_type: AlertType) {
        self.state
            .lock()
            .unwrap()
            .pending_alerts
            .remove(&(alert_type as i32));
    }
}

impl MonitorImpl {
    fn send_alert_locked(st: &mut MonitorState, alert: Alert) {
        // deduplicate alerts
        let t = alert.alert_type as i32;
        if st.pending_alerts.contains(&t) {
            return;
        }
        if let Some(sub) = &st.subscriber {
            // do not block if subscriber is not ready
            if sub.try_send(alert).is_ok() {
                st.pending_alerts.insert(t);
            }
        }
    }
}
