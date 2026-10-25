//! Exercises the production binary's typed SQL writer fatal path end to end.
use cadence_proto::{api as p, matching as m};
use std::{net::TcpListener, process::{Command, Stdio}, time::{Duration, Instant}};
use tonic::{transport::Channel, Request};

struct ChildGuard(Option<std::process::Child>);

impl ChildGuard {
    fn new(child: std::process::Child) -> Self { Self(Some(child)) }
    fn child_mut(&mut self) -> &mut std::process::Child { self.0.as_mut().expect("child still owned") }
    fn finish(mut self) -> std::process::Output { self.0.take().expect("child still owned").wait_with_output().expect("collect Matching child output") }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            if !matches!(child.try_wait(), Ok(Some(_))) {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

fn free_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    listener.local_addr().unwrap().port()
}

async fn matching_client(port: u16) -> m::matching_api_client::MatchingApiClient<Channel> {
    let endpoint = format!("http://127.0.0.1:{port}");
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Ok(client) = m::matching_api_client::MatchingApiClient::connect(endpoint.clone()).await {
                return client;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }).await.expect("standalone Matching gRPC listener did not start")
}

async fn wait_for_http(port: u16) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }).await.expect("standalone Frontend HTTP listener did not start");
}

fn add_request(domain_id: String, run_id: String) -> m::AddDecisionTaskRequest {
    m::AddDecisionTaskRequest {
        domain_id,
        workflow_execution: Some(p::WorkflowExecution { workflow_id: "fatal-process-test".into(), run_id }),
        task_list: Some(p::TaskList { name: "fatal-process-test".into(), kind: 1, ..Default::default() }),
        schedule_id: 1,
        ..Default::default()
    }
}

async fn register_domain(port: u16) -> String {
    let body = r#"{"name":"fatal-process-test","workflowExecutionRetentionPeriodInDays":3}"#;
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.expect("connect to standalone Frontend HTTP listener");
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    stream.write_all(format!(
        "POST /api/v1/domains/register HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(), body
    ).as_bytes()).await.expect("send domain registration request");
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.expect("read domain registration response");
    let response = String::from_utf8(response).expect("Frontend response is UTF-8");
    let (headers, json) = response.split_once("\r\n\r\n").expect("HTTP response contains a body");
    assert!(headers.starts_with("HTTP/1.1 200"), "domain registration failed: {response}");
    serde_json::from_str::<serde_json::Value>(json).expect("decode domain registration response")["domainInfo"]["uuid"]
        .as_str().expect("registered domain response has a UUID").to_owned()
}

#[tokio::test]
async fn configured_standalone_sqlite_malformed_run_id_exits_process_with_two() {
    let directory = tempfile::tempdir().unwrap();
    let grpc_port = free_port();
    let http_port = free_port();
    let tchannel_port = free_port();
    let child_log = directory.path().join("cadence-server.log");
    let stdout = std::fs::File::create(&child_log).expect("create child log");
    let stderr = stdout.try_clone().expect("clone child log handle");
    let child = Command::new(env!("CARGO_BIN_EXE_cadence-server"))
        .args(["start", "--data-dir"])
        .arg(directory.path())
        .args(["--services", "frontend,matching", "--port", &http_port.to_string(), "--matching-grpc-port", &grpc_port.to_string(), "--matching-tchannel-port", &tchannel_port.to_string()])
        .env("RUST_LOG", "error")
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .expect("launch actual cadence-server binary with Matching");
    let mut child = ChildGuard::new(child);

    wait_for_http(http_port).await;
    let domain_id = register_domain(http_port).await;
    let mut client = matching_client(grpc_port).await;
    let response = client.add_decision_task(Request::new(add_request(domain_id, "malformed-run-id".into()))).await;
    let error = response.expect_err("malformed Add must not receive successful acknowledgement");
    assert!(error.message().contains("invalid SQL Matching RunID"), "Add must reach the SQL writer: code={:?}, message={:?}; child log: {}", error.code(), error.message(), std::fs::read_to_string(&child_log).unwrap_or_default());

    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(status) = child.child_mut().try_wait().expect("wait for Matching child") { break status; }
        assert!(Instant::now() < deadline, "fatal writer notification did not terminate process; RPC error: {}; child log: {}", error.message(), std::fs::read_to_string(&child_log).unwrap_or_default());
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(status.code(), Some(2), "actual launcher must map typed SQL writer failure to exit 2");
    let output = child.finish();
    let stderr = std::fs::read_to_string(&child_log).unwrap_or_else(|_| String::from_utf8_lossy(&output.stderr).into_owned());
    assert!(stderr.contains("invalid SQL Matching RunID"), "fatal typed error marker must be visible: {stderr}");
}
