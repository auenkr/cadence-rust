use super::test_support::MockHandler;
use super::*;
use cadence_proto::history::history_api_client::HistoryApiClient;
use cadence_proto::history as pb;
use cadence_thrift::protocol::{Reader, Writer};
use cadence_thrift::value::{read_struct, write_struct, Struct, Value};
use std::sync::atomic::Ordering;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn cfg(rps: f64) -> ServerConfig {
    let mut c = ServerConfig::new("127.0.0.1:0".parse().unwrap(), "127.0.0.1:0".parse().unwrap());
    c.rps = rps;
    c.shutdown_grace = Duration::from_secs(2);
    c
}

fn frame(kind: u8, id: u32, payload: Vec<u8>) -> Vec<u8> {
    let mut o = ((16 + payload.len()) as u16).to_be_bytes().to_vec();
    o.push(kind);
    o.push(0);
    o.extend_from_slice(&id.to_be_bytes());
    o.extend_from_slice(&[0; 8]);
    o.extend(payload);
    o
}

fn s16(o: &mut Vec<u8>, s: &str) {
    o.extend_from_slice(&(s.len() as u16).to_be_bytes());
    o.extend_from_slice(s.as_bytes());
}

async fn read(c: &mut tokio::net::TcpStream) -> (u8, u32, Vec<u8>) {
    let mut h = [0u8; 16];
    c.read_exact(&mut h).await.unwrap();
    let n = u16::from_be_bytes([h[0], h[1]]) as usize;
    let mut p = vec![0; n - 16];
    c.read_exact(&mut p).await.unwrap();
    (h[2], u32::from_be_bytes([h[4], h[5], h[6], h[7]]), p)
}

async fn connect(addr: std::net::SocketAddr) -> tokio::net::TcpStream {
    let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut p = 2u16.to_be_bytes().to_vec();
    p.extend_from_slice(&2u16.to_be_bytes());
    s16(&mut p, "host_port");
    s16(&mut p, "127.0.0.1:1");
    s16(&mut p, "process_name");
    s16(&mut p, "test");
    c.write_all(&frame(0x01, 1, p)).await.unwrap();
    let (k, _, _) = read(&mut c).await;
    assert_eq!(k, 0x02);
    c
}

/// Returns (frame kind, application-error flag, result struct).
async fn call(c: &mut tokio::net::TcpStream, id: u32, service: &str, proc_: &str, request: Option<Struct>) -> (u8, bool, Option<Struct>) {
    let mut p = vec![0u8];
    p.extend_from_slice(&5000u32.to_be_bytes());
    p.extend_from_slice(&[0; 25]);
    p.push(service.len() as u8);
    p.extend_from_slice(service.as_bytes());
    p.push(1);
    p.extend_from_slice(&[2, b'a', b's', 6]);
    p.extend_from_slice(b"thrift");
    p.push(0);
    let mut args = Struct::default();
    if let Some(r) = request {
        args.fields.push((1, Value::Struct(r)));
    }
    let mut w = Writer::new();
    write_struct(&mut w, &args);
    let a3 = w.into_bytes();
    for a in [proc_.as_bytes().to_vec(), vec![0, 0], a3] {
        p.extend_from_slice(&(a.len() as u16).to_be_bytes());
        p.extend_from_slice(&a);
    }
    c.write_all(&frame(0x03, id, p)).await.unwrap();
    let (k, rid, payload) = read(c).await;
    assert_eq!(rid, id);
    if k != 0x04 {
        return (k, false, None);
    }
    // flags, code, trace(25), nh, headers..., csum, args
    let app = payload[1] == 1;
    let mut i = 2 + 25;
    let nh = payload[i] as usize;
    i += 1;
    for _ in 0..nh {
        for _ in 0..2 {
            let n = payload[i] as usize;
            i += 1 + n;
        }
    }
    assert_eq!(payload[i], 0);
    i += 1;
    let mut args = Vec::new();
    while i < payload.len() {
        let n = u16::from_be_bytes([payload[i], payload[i + 1]]) as usize;
        args.push(payload[i + 2..i + 2 + n].to_vec());
        i += 2 + n;
    }
    let body = args.last().unwrap();
    let s = read_struct(&mut Reader::new(body)).unwrap();
    (k, app, Some(s))
}

fn shard_req(id: i32) -> Struct {
    let mut s = Struct::default();
    s.fields.push((10, Value::I32(id)));
    s
}

#[tokio::test]
async fn thrift_close_shard_and_health() {
    let h = Arc::new(MockHandler::default());
    let srv = HistoryServer::start(cfg(0.0), h.clone()).await.unwrap();
    assert!(h.started.load(Ordering::SeqCst));
    let mut c = connect(srv.thrift_addr).await;
    let (k, app, _) = call(&mut c, 2, "cadence-history", "HistoryService::CloseShard", Some(shard_req(7))).await;
    assert_eq!((k, app), (0x04, false));
    assert_eq!(h.closed_shard.load(Ordering::SeqCst), 7);
    let (_, app, res) = call(&mut c, 3, "cadence-history", "HistoryService::CloseShard", Some(shard_req(-1))).await;
    assert!(app);
    assert!(res.unwrap().get(1).is_some());
    let (_, app, res) = call(&mut c, 4, "cadence-history", "Meta::health", None).await;
    assert!(!app);
    assert!(res.unwrap().get(0).is_some());
    let (_, app, res) = call(&mut c, 5, "cadence-history", "HistoryService::ResetWorkflowExecution", None).await;
    assert!(app);
    assert!(res.unwrap().get(2).is_some());
    let (k, _, _) = call(&mut c, 6, "other", "HistoryService::CloseShard", Some(shard_req(1))).await;
    assert_eq!(k, 0xff);
    srv.shutdown().await;
    assert!(h.prepared.load(Ordering::SeqCst) && h.stopped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn thrift_rate_limited() {
    let h = Arc::new(MockHandler::default());
    let srv = HistoryServer::start(cfg(1.0), h.clone()).await.unwrap();
    let mut c = connect(srv.thrift_addr).await;
    let mut busy = 0;
    for i in 0..5 {
        let (_, app, _) = call(&mut c, 10 + i, "cadence-history", "HistoryService::RemoveTask", Some(shard_req(1))).await;
        busy += u32::from(app);
    }
    assert!(busy >= 3, "busy={busy}");
    srv.shutdown().await;
}

#[tokio::test]
async fn grpc_admin_rpcs_and_unimplemented() {
    let h = Arc::new(MockHandler::default());
    let srv = HistoryServer::start(cfg(0.0), h.clone()).await.unwrap();
    let mut c = HistoryApiClient::connect(format!("http://{}", srv.grpc_addr)).await.unwrap();
    c.close_shard(pb::CloseShardRequest { shard_id: 3 }).await.unwrap();
    assert_eq!(h.closed_shard.load(Ordering::SeqCst), 3);
    let err = c.close_shard(pb::CloseShardRequest { shard_id: -1 }).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    let r = c.describe_queue(pb::DescribeQueueRequest { shard_id: 2, cluster_name: "a".into(), task_type: 1 }).await.unwrap();
    assert_eq!(r.into_inner().processing_queue_states, vec!["2:a".to_string()]);
    c.remove_task(pb::RemoveTaskRequest::default()).await.unwrap();
    assert_eq!(h.removed.load(Ordering::SeqCst), 1);
    let err = c.reset_workflow_execution(pb::ResetWorkflowExecutionRequest::default()).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Internal);
    srv.shutdown().await;
    assert!(h.stopped.load(Ordering::SeqCst));
}

#[test]
fn limiter_and_error_mapping() {
    let l = limiter::RateLimiter::new(2.0);
    assert!(l.allow() && l.allow() && !l.allow());
    assert!(limiter::RateLimiter::new(0.0).allow());
    use crate::engine::deps::HistoryError;
    assert_eq!(errors::grpc_status(&HistoryError::entity_not_exists("x")).code(), tonic::Code::NotFound);
    assert_eq!(errors::grpc_status(&errors::busy()).code(), tonic::Code::ResourceExhausted);
    assert_eq!(errors::thrift_exception(&HistoryError::internal("x")).0, "InternalServiceError");
}

#[tokio::test]
async fn reset_workflow_execution_both_transports() {
    let h = Arc::new(MockHandler::default());
    let srv = HistoryServer::start(cfg(0.0), h.clone()).await.unwrap();
    // gRPC
    let mut g = HistoryApiClient::connect(format!("http://{}", srv.grpc_addr)).await.unwrap();
    let resp = g
        .reset_workflow_execution(pb::ResetWorkflowExecutionRequest {
            domain_id: "dom".into(),
            request: Some(cadence_proto::api::ResetWorkflowExecutionRequest { decision_finish_event_id: 9, ..Default::default() }),
        })
        .await
        .unwrap();
    assert_eq!(resp.into_inner().run_id, "dom:9");
    // Thrift request {10: domainUUID, 20: {40: decisionFinishEventId}}
    let mut c = connect(srv.thrift_addr).await;
    let mut req = Struct::default();
    req.fields.push((10, Value::Bytes(b"dom".to_vec())));
    let mut inner = Struct::default();
    inner.fields.push((40, Value::I64(5)));
    req.fields.push((20, Value::Struct(inner)));
    let (_, app, res) = call(&mut c, 7, "cadence-history", "HistoryService::ResetWorkflowExecution", Some(req)).await;
    assert!(!app);
    let ok = match res.unwrap().get(0) {
        Some(Value::Struct(s)) => s.clone(),
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(ok.get(10), Some(&Value::Bytes(b"dom:5".to_vec())));
    srv.shutdown().await;
}
