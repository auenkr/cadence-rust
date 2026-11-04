//! Thrift converters for history-local request/response types used by the
//! group-3 RPCs (field ids from go/idls/thrift/history.thrift and shared.thrift).
//! The traits live in `cadence-thrift`; the types are local to this crate, so
//! the orphan rule allows the impls.
use crate::engine::types::{
    Any, FailoverMarkerToken, GetFailoverInfoRequest, GetFailoverInfoResponse,
    NotifyFailoverMarkersRequest, RatelimitUpdateRequest, RatelimitUpdateResponse,
    ReplicateEventsV2Request, SyncActivityRequest, SyncShardStatusRequest,
};
use cadence_thrift::protocol::Error;
use cadence_thrift::value::{Fields, FromThrift, Struct, ToThrift};

type R<T> = Result<T, Error>;

impl FromThrift for ReplicateEventsV2Request {
    fn from_thrift(s: &Struct) -> R<Self> {
        Ok(Self {
            domain_uuid: s.req_string(10, "")?,
            workflow_execution: s.opt_struct(20)?,
            version_history_items: s.opt_list(30)?.unwrap_or_default(),
            events: s.opt_struct(40)?,
            new_run_events: s.opt_struct(60)?,
        })
    }
}

impl ToThrift for ReplicateEventsV2Request {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain_uuid);
        f.ostruct(20, self.workflow_execution.as_ref());
        f.olist(30, &self.version_history_items);
        f.ostruct(40, self.events.as_ref());
        f.ostruct(60, self.new_run_events.as_ref());
        f.finish()
    }
}

impl FromThrift for SyncShardStatusRequest {
    fn from_thrift(s: &Struct) -> R<Self> {
        Ok(Self {
            source_cluster: s.req_string(10, "")?,
            shard_id: s.i64_or_zero(20)?,
            timestamp: s.opt_i64(30)?,
        })
    }
}

impl ToThrift for SyncShardStatusRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.source_cluster);
        f.oi64(20, Some(self.shard_id));
        f.oi64(30, self.timestamp);
        f.finish()
    }
}

impl FromThrift for SyncActivityRequest {
    fn from_thrift(s: &Struct) -> R<Self> {
        Ok(Self {
            domain_id: s.req_string(10, "")?,
            workflow_id: s.req_string(20, "")?,
            run_id: s.req_string(30, "")?,
            version: s.i64_or_zero(40)?,
            scheduled_id: s.i64_or_zero(50)?,
            scheduled_time: s.opt_i64(60)?,
            started_id: s.i64_or_zero(70)?,
            started_time: s.opt_i64(80)?,
            last_heartbeat_time: s.opt_i64(90)?,
            details: s.bytes_or_empty(100)?,
            attempt: s.opt_i32(110)?.unwrap_or_default(),
            last_failure_reason: s.opt_string(120)?,
            last_worker_identity: s.req_string(130, "")?,
            last_failure_details: s.bytes_or_empty(140)?,
            last_failure_options: s.opt_struct(145)?,
            version_history: s.opt_struct(150)?,
        })
    }
}

impl ToThrift for SyncActivityRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain_id);
        f.sstr(20, &self.workflow_id);
        f.sstr(30, &self.run_id);
        f.oi64(40, Some(self.version));
        f.oi64(50, Some(self.scheduled_id));
        f.oi64(60, self.scheduled_time);
        f.oi64(70, Some(self.started_id));
        f.oi64(80, self.started_time);
        f.oi64(90, self.last_heartbeat_time);
        f.sbytes(100, &self.details);
        f.oi32(110, Some(self.attempt));
        f.ostr(120, &self.last_failure_reason);
        f.sstr(130, &self.last_worker_identity);
        f.sbytes(140, &self.last_failure_details);
        f.ostruct(145, self.last_failure_options.as_ref());
        f.ostruct(150, self.version_history.as_ref());
        f.finish()
    }
}

impl FromThrift for FailoverMarkerToken {
    fn from_thrift(s: &Struct) -> R<Self> {
        Ok(Self {
            shard_ids: s.opt_list(10)?.unwrap_or_default(),
            failover_marker: s.opt_struct(20)?,
        })
    }
}

impl ToThrift for FailoverMarkerToken {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.olist(10, &self.shard_ids);
        f.ostruct(20, self.failover_marker.as_ref());
        f.finish()
    }
}

impl FromThrift for NotifyFailoverMarkersRequest {
    fn from_thrift(s: &Struct) -> R<Self> {
        Ok(Self { failover_marker_tokens: s.opt_list(10)?.unwrap_or_default() })
    }
}

impl ToThrift for NotifyFailoverMarkersRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.olist(10, &self.failover_marker_tokens);
        f.finish()
    }
}

impl FromThrift for GetFailoverInfoRequest {
    fn from_thrift(s: &Struct) -> R<Self> {
        Ok(Self { domain_id: s.req_string(10, "")? })
    }
}

impl ToThrift for GetFailoverInfoRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.domain_id);
        f.finish()
    }
}

impl FromThrift for GetFailoverInfoResponse {
    fn from_thrift(s: &Struct) -> R<Self> {
        Ok(Self {
            completed_shard_count: s.opt_i32(10)?.unwrap_or_default(),
            pending_shards: s.opt_list(20)?.unwrap_or_default(),
        })
    }
}

impl ToThrift for GetFailoverInfoResponse {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.oi32(10, Some(self.completed_shard_count));
        f.olist(20, &self.pending_shards);
        f.finish()
    }
}

impl FromThrift for Any {
    fn from_thrift(s: &Struct) -> R<Self> {
        Ok(Self { value_type: s.req_string(10, "")?, value: s.bytes_or_empty(20)? })
    }
}

impl ToThrift for Any {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.sstr(10, &self.value_type);
        f.sbytes(20, &self.value);
        f.finish()
    }
}

impl FromThrift for RatelimitUpdateRequest {
    fn from_thrift(s: &Struct) -> R<Self> {
        Ok(Self { any: s.opt_struct(10)? })
    }
}

impl ToThrift for RatelimitUpdateRequest {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostruct(10, self.any.as_ref());
        f.finish()
    }
}

impl FromThrift for RatelimitUpdateResponse {
    fn from_thrift(s: &Struct) -> R<Self> {
        Ok(Self { any: s.opt_struct(10)? })
    }
}

impl ToThrift for RatelimitUpdateResponse {
    fn to_thrift(&self) -> Struct {
        let mut f = Fields::new();
        f.ostruct(10, self.any.as_ref());
        f.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt<T: FromThrift + ToThrift>(v: &T) -> T {
        T::from_thrift(&v.to_thrift()).unwrap()
    }

    #[test]
    fn sync_shard_status_roundtrip() {
        let v = SyncShardStatusRequest { source_cluster: "c".into(), shard_id: 7, timestamp: Some(99) };
        let b = rt(&v);
        assert_eq!((b.source_cluster.as_str(), b.shard_id, b.timestamp), ("c", 7, Some(99)));
    }

    #[test]
    fn sync_activity_roundtrip() {
        let v = SyncActivityRequest {
            domain_id: "d".into(),
            workflow_id: "w".into(),
            run_id: "r".into(),
            version: 3,
            scheduled_id: 5,
            scheduled_time: Some(10),
            started_id: 6,
            attempt: 2,
            details: vec![1, 2],
            last_failure_reason: Some("x".into()),
            last_worker_identity: "id".into(),
            ..Default::default()
        };
        let b = rt(&v);
        assert_eq!(b.domain_id, "d");
        assert_eq!(b.version, 3);
        assert_eq!(b.scheduled_time, Some(10));
        assert_eq!(b.details, vec![1, 2]);
        assert_eq!(b.last_failure_reason.as_deref(), Some("x"));
        assert_eq!(b.attempt, 2);
    }

    #[test]
    fn replicate_events_roundtrip() {
        let v = ReplicateEventsV2Request {
            domain_uuid: "d".into(),
            workflow_execution: Some(cadence_api::types::WorkflowExecution { workflow_id: "w".into(), run_id: "r".into() }),
            version_history_items: vec![cadence_api::types::VersionHistoryItem { event_id: 4, version: 2 }],
            ..Default::default()
        };
        let b = rt(&v);
        assert_eq!(b.domain_uuid, "d");
        assert_eq!(b.workflow_execution.unwrap().run_id, "r");
        assert_eq!(b.version_history_items.len(), 1);
        assert_eq!(b.version_history_items[0].event_id, 4);
    }

    #[test]
    fn failover_and_ratelimit_roundtrip() {
        let tok = FailoverMarkerToken { shard_ids: vec![1, 2], failover_marker: None };
        let n = NotifyFailoverMarkersRequest { failover_marker_tokens: vec![tok] };
        assert_eq!(rt(&n).failover_marker_tokens[0].shard_ids, vec![1, 2]);
        let g = GetFailoverInfoResponse { completed_shard_count: 3, pending_shards: vec![9] };
        let g2 = rt(&g);
        assert_eq!((g2.completed_shard_count, g2.pending_shards), (3, vec![9]));
        let r = RatelimitUpdateRequest { any: Some(Any { value_type: "t".into(), value: vec![1] }) };
        assert_eq!(rt(&r).any.unwrap().value, vec![1]);
        assert_eq!(rt(&GetFailoverInfoRequest { domain_id: "x".into() }).domain_id, "x");
    }
}
