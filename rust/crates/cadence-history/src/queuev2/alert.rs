//! Port of queuev2/alert.go

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AlertType {
    Unspecified = 0,
    QueuePendingTaskCount = 1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlertAttributesQueuePendingTaskCount {
    pub current_pending_task_count: i64,
    pub critical_pending_task_count: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Alert {
    pub alert_type: AlertType,
    pub alert_attributes_queue_pending_task_count: Option<AlertAttributesQueuePendingTaskCount>,
}
