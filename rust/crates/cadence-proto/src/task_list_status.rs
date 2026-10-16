//! Keep map-message presence compatible with pinned Go gogo/protobuf. Prost's
//! map encoder omits the value for default messages; Go decodes that as nil and
//! re-encodes a nil value, which protobufjs cannot convert to an object. Go's
//! own encoder emits an explicit zero-length message for nonnil zero metrics.
use crate::api::{IsolationGroupMetrics, TaskIdBlock};
use prost::{
    bytes::{Buf, BufMut},
    encoding::{DecodeContext, WireType},
    DecodeError, Message,
};
use std::collections::HashMap;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct TaskListStatus {
    pub backlog_count_hint: i64,
    pub read_level: i64,
    pub ack_level: i64,
    pub rate_per_second: f64,
    pub task_id_block: Option<TaskIdBlock>,
    pub isolation_group_metrics: HashMap<String, IsolationGroupMetrics>,
    pub new_tasks_per_second: f64,
    pub empty: bool,
}

#[derive(Clone, PartialEq, Message)]
struct MetricEntry {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(message, optional, tag = "2")]
    value: Option<IsolationGroupMetrics>,
}
#[derive(Clone, PartialEq, Message)]
struct WireStatus {
    #[prost(int64, tag = "1")]
    backlog_count_hint: i64,
    #[prost(int64, tag = "2")]
    read_level: i64,
    #[prost(int64, tag = "3")]
    ack_level: i64,
    #[prost(double, tag = "4")]
    rate_per_second: f64,
    #[prost(message, optional, tag = "5")]
    task_id_block: Option<TaskIdBlock>,
    #[prost(message, repeated, tag = "6")]
    isolation_group_metrics: Vec<MetricEntry>,
    #[prost(double, tag = "7")]
    new_tasks_per_second: f64,
    #[prost(bool, tag = "8")]
    empty: bool,
}
impl TaskListStatus {
    fn wire(&self) -> WireStatus {
        WireStatus {
            backlog_count_hint: self.backlog_count_hint,
            read_level: self.read_level,
            ack_level: self.ack_level,
            rate_per_second: self.rate_per_second,
            task_id_block: self.task_id_block,
            isolation_group_metrics: self
                .isolation_group_metrics
                .iter()
                .map(|(key, value)| MetricEntry {
                    key: key.clone(),
                    value: Some(*value),
                })
                .collect(),
            new_tasks_per_second: self.new_tasks_per_second,
            empty: self.empty,
        }
    }
    fn apply(&mut self, wire: WireStatus) {
        self.backlog_count_hint = wire.backlog_count_hint;
        self.read_level = wire.read_level;
        self.ack_level = wire.ack_level;
        self.rate_per_second = wire.rate_per_second;
        self.task_id_block = wire.task_id_block;
        self.isolation_group_metrics = wire
            .isolation_group_metrics
            .into_iter()
            .map(|entry| (entry.key, entry.value.unwrap_or_default()))
            .collect();
        self.new_tasks_per_second = wire.new_tasks_per_second;
        self.empty = wire.empty;
    }
}
impl Message for TaskListStatus {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        self.wire().encode_raw(buf);
    }
    fn encoded_len(&self) -> usize {
        self.wire().encoded_len()
    }
    fn clear(&mut self) {
        *self = Self::default();
    }
    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        let mut wire = self.wire();
        wire.merge_field(tag, wire_type, buf, ctx)?;
        self.apply(wire);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn zero_metrics_encode_explicit_message_presence() {
        let status = TaskListStatus {
            isolation_group_metrics: HashMap::from([(
                "a".into(),
                IsolationGroupMetrics::default(),
            )]),
            ..Default::default()
        };
        assert_eq!(
            status.encode_to_vec(),
            vec![0x32, 5, 0x0a, 1, b'a', 0x12, 0]
        );
        assert_eq!(
            TaskListStatus::decode(status.encode_to_vec().as_slice()).unwrap(),
            status
        );
        // Accept the legal absent-value form from other protobuf implementations.
        assert_eq!(
            TaskListStatus::decode(&[0x32, 3, 0x0a, 1, b'a'][..]).unwrap(),
            status
        );
    }

    #[test]
    fn populated_status_and_duplicate_map_entries_round_trip() {
        let status = TaskListStatus {
            backlog_count_hint: 17,
            read_level: 25,
            ack_level: 8,
            rate_per_second: 13.5,
            task_id_block: Some(TaskIdBlock {
                start_id: 100,
                end_id: 199,
            }),
            isolation_group_metrics: HashMap::from([(
                "a".into(),
                IsolationGroupMetrics {
                    new_tasks_per_second: 2.5,
                    poller_count: 3,
                },
            )]),
            new_tasks_per_second: 7.5,
            empty: true,
        };
        let mut bytes = status.encode_to_vec();
        assert_eq!(TaskListStatus::decode(bytes.as_slice()).unwrap(), status);
        // Protobuf maps use the last occurrence of a repeated key.
        bytes.extend_from_slice(&[0x32, 5, 0x0a, 1, b'a', 0x12, 0]);
        let mut replaced = status;
        replaced
            .isolation_group_metrics
            .insert("a".into(), IsolationGroupMetrics::default());
        assert_eq!(TaskListStatus::decode(bytes.as_slice()).unwrap(), replaced);
    }
}
