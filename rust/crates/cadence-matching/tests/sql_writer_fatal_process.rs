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

fn add_request(run_id: String) -> m::AddDecisionTaskRequest {
    m::AddDecisionTaskRequest {
        domain_id: "00000000-0000-4000-8000-000000000001".into(),
        workflow_execution: Some(p::WorkflowExecution { workflow_id: "fatal-process-test".into(), run_id }),
        task_list: Some(p::TaskList { name: "fatal-process-test".into(), kind: 1, ..Default::default() }),
        schedule_id: 1,
        ..Default::default()
    }
}

#[tokio::test]
async fn configured_standalone_sqlite_malformed_run_id_exits_process_with_two() {
    let directory = tempfile::tempdir().unwrap();
    let grpc_port = free_port();
    let http_port = free_port();
    let tchannel_port = free_port();
    let child = Command::new(env!("CARGO_BIN_EXE_cadence-matching"))
        .args(["start", "--data-dir"])
        .arg(directory.path())
        .args(["--port", &http_port.to_string(), "--matching-grpc-port", &grpc_port.to_string(), "--matching-tchannel-port", &tchannel_port.to_string()])
        .env("RUST_LOG", "error")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("launch actual cadence-matching binary");
    let mut child = ChildGuard::new(child);

    let mut client = matching_client(grpc_port).await;
    let response = client.add_decision_task(Request::new(add_request("malformed-run-id".into()))).await;
    assert!(response.is_err(), "malformed Add must not receive successful acknowledgement");

    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(status) = child.child_mut().try_wait().expect("wait for Matching child") { break status; }
        assert!(Instant::now() < deadline, "fatal writer notification did not terminate process");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(status.code(), Some(2), "actual launcher must map typed SQL writer failure to exit 2");
    let output = child.finish();
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid SQL Matching RunID"), "fatal typed error marker must be visible");
}
