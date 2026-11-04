//! Go mutable-state checksum payload v1 and ThriftRW/IEEE CRC32 encoding.
use cadence_api::types::VersionHistories;
use crate::mutable_state::MutableStateBuilder;

pub const MUTABLE_STATE_CHECKSUM_PAYLOAD_V1: i32 = 1;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checksum { pub version: i32, pub flavor: i32, pub value: Vec<u8> }
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChecksumError {
    #[error("invalid checksum payload version {0}")] InvalidVersion(i32),
    #[error("unknown checksum flavor {0}")] InvalidFlavor(i32),
    #[error("checksum mismatch error")] Mismatch,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MutableStateChecksumPayload {
    pub cancel_requested: bool,
    pub state: i16,
    pub last_first_event_id: i64,
    pub next_event_id: i64,
    pub last_processed_event_id: i64,
    pub signal_count: i64,
    pub decision_attempt: i32,
    pub decision_version: i64,
    pub decision_scheduled_id: i64,
    pub decision_started_id: i64,
    pub pending_timer_started_ids: Vec<i64>,
    pub pending_activity_scheduled_ids: Vec<i64>,
    pub pending_signal_initiated_ids: Vec<i64>,
    pub pending_req_cancel_initiated_ids: Vec<i64>,
    pub pending_child_initiated_ids: Vec<i64>,
    pub sticky_task_list_name: String,
    pub version_histories: Option<VersionHistories>,
}
impl MutableStateChecksumPayload {
    pub fn from_mutable_state(ms: &MutableStateBuilder) -> Self {
        let info = &ms.execution_info;
        let number = |key: &str| info[key].as_i64().unwrap_or_default();
        let mut pending_timer_started_ids = ms.pending_timer_infos.values().map(|v| v["started_id"].as_i64().unwrap_or_default()).collect::<Vec<_>>();
        pending_timer_started_ids.sort_unstable();
        Self {
            cancel_requested: info["cancel_requested"].as_bool().unwrap_or_default(), state: number("state") as i16,
            last_first_event_id: number("last_first_event_id"), next_event_id: ms.next_event_id,
            last_processed_event_id: number("last_processed_event"), signal_count: number("signal_count"),
            decision_attempt: number("decision_attempt") as i32, decision_version: number("decision_version"),
            decision_scheduled_id: number("decision_schedule_id"), decision_started_id: number("decision_started_id"),
            pending_timer_started_ids,
            pending_activity_scheduled_ids: ms.pending_activity_infos.keys().copied().collect(),
            pending_signal_initiated_ids: ms.pending_signal_infos.keys().copied().collect(),
            pending_req_cancel_initiated_ids: ms.pending_request_cancel_infos.keys().copied().collect(),
            pending_child_initiated_ids: ms.pending_child_execution_infos.keys().copied().collect(),
            sticky_task_list_name: info["sticky_task_list"].as_str().unwrap_or_default().to_owned(),
            version_histories: ms.version_histories.clone(),
        }
    }
    /// Field IDs and optional-field presence follow the generated Go checksum
    /// schema. The 0x59 preamble is part of the checksum, not just the payload.
    pub fn thrift_bytes(&self) -> Vec<u8> {
        let mut out = vec![0x59];
        field(&mut out, 2, 10); out.push(u8::from(self.cancel_requested));
        field(&mut out, 6, 15); out.extend(self.state.to_be_bytes());
        for (id, value) in [(23, self.last_first_event_id), (24, self.next_event_id), (25, self.last_processed_event_id), (26, self.signal_count)] { i64_field(&mut out, id, value); }
        field(&mut out, 8, 35); out.extend(self.decision_attempt.to_be_bytes());
        for (id, value) in [(36, self.decision_version), (37, self.decision_scheduled_id), (38, self.decision_started_id)] { i64_field(&mut out, id, value); }
        for (id, values) in [(45, &self.pending_timer_started_ids), (46, &self.pending_activity_scheduled_ids), (47, &self.pending_signal_initiated_ids), (48, &self.pending_req_cancel_initiated_ids), (49, &self.pending_child_initiated_ids)] {
            field(&mut out, 15, id); out.push(10); out.extend((values.len() as i32).to_be_bytes());
            // Sorting here also makes explicitly constructed payloads deterministic.
            let mut values = values.clone(); values.sort_unstable();
            for value in values { out.extend(value.to_be_bytes()); }
        }
        field(&mut out, 11, 55); binary(&mut out, self.sticky_task_list_name.as_bytes());
        if let Some(histories) = &self.version_histories {
            field(&mut out, 12, 56);
            field(&mut out, 8, 10); out.extend(histories.current_version_history_index.to_be_bytes());
            field(&mut out, 15, 20); out.push(12); out.extend((histories.histories.len() as i32).to_be_bytes());
            for history in &histories.histories {
                field(&mut out, 11, 10); binary(&mut out, &history.branch_token);
                field(&mut out, 15, 20); out.push(12); out.extend((history.items.len() as i32).to_be_bytes());
                for item in &history.items { i64_field(&mut out, 10, item.event_id); i64_field(&mut out, 20, item.version); out.push(0); }
                out.push(0);
            }
            out.push(0);
        }
        out.push(0);
        out
    }
    pub fn generate(&self) -> Checksum {
        let mut crc = !0_u32;
        for byte in self.thrift_bytes() {
            crc ^= u32::from(byte);
            for _ in 0..8 { crc = (crc >> 1) ^ (0xedb8_8320_u32 & 0_u32.wrapping_sub(crc & 1)); }
        }
        Checksum { version: MUTABLE_STATE_CHECKSUM_PAYLOAD_V1, flavor: 1, value: (!crc).to_be_bytes().to_vec() }
    }
    pub fn verify(&self, checksum: &Checksum) -> Result<(), ChecksumError> {
        if checksum.version != MUTABLE_STATE_CHECKSUM_PAYLOAD_V1 { return Err(ChecksumError::InvalidVersion(checksum.version)); }
        if checksum.flavor != 1 { return Err(ChecksumError::InvalidFlavor(checksum.flavor)); }
        if self.generate().value != checksum.value { return Err(ChecksumError::Mismatch); }
        Ok(())
    }
}
fn field(out: &mut Vec<u8>, kind: u8, id: i16) { out.push(kind); out.extend(id.to_be_bytes()); }
fn i64_field(out: &mut Vec<u8>, id: i16, value: i64) { field(out, 10, id); out.extend(value.to_be_bytes()); }
fn binary(out: &mut Vec<u8>, value: &[u8]) { out.extend((value.len() as i32).to_be_bytes()); out.extend(value); }
