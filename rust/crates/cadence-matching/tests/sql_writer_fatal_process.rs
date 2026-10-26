//! Exercises the standalone Matching binary's typed SQL writer fatal path.
use cadence_api::types::{DomainConfiguration, DomainInfo, DomainReplicationConfiguration};
use cadence_persistence::InMemoryDomainStore;
use cadence_proto::{api as p, matching as m};
use std::{net::TcpListener, process::{Command, Stdio}, time::{Duration, Instant}};
use tonic::{transport::{Channel, Endpoint}, Request};

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

async fn matching_client(port: u16, child: &mut ChildGuard, log: &std::path::Path) -> m::matching_api_client::MatchingApiClient<Channel> {
    let endpoint = Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect_timeout(Duration::from_secs(1));
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Some(status) = child.child_mut().try_wait().expect("check Matching child") {
                panic!("standalone Matching exited before gRPC became ready ({status}); child log: {}", std::fs::read_to_string(log).unwrap_or_default());
            }
            if let Ok(channel) = endpoint.clone().connect().await {
                return m::matching_api_client::MatchingApiClient::new(channel);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }).await.expect("standalone Matching gRPC listener did not start")
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

#[tokio::test]
async fn standalone_matching_sqlite_malformed_run_id_exits_process_with_two() {
    let directory = tempfile::tempdir().unwrap();
    let domain_id = "00000000-0000-4000-8000-000000000001".to_owned();
    // Seed the same durable domain snapshot the no-config standalone launcher
    // reads. This uses the persistence API and avoids Frontend/server behavior.
    let domains = InMemoryDomainStore::new();
    domains.create(
        DomainInfo { name: "fatal-process-test".into(), uuid: domain_id.clone(), ..Default::default() },
        DomainConfiguration { workflow_execution_retention_period_in_days: 3, ..Default::default() },
        DomainReplicationConfiguration::default(),
        false,
    ).expect("seed domain for Matching's domain cache");
    domains.save_to_file(directory.path().join("domains.json")).expect("persist seed domain");

    let grpc_port = free_port();
    let tchannel_port = free_port();
    let child_log = directory.path().join("cadence-matching.log");
    let stdout = std::fs::File::create(&child_log).expect("create child log");
    let stderr = stdout.try_clone().expect("clone child log handle");
    let child = Command::new(env!("CARGO_BIN_EXE_cadence-matching"))
        .args(["start", "--data-dir"])
        .arg(directory.path())
        .args(["--matching-grpc-port", &grpc_port.to_string(), "--matching-tchannel-port", &tchannel_port.to_string()])
        .env("RUST_LOG", "error")
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .expect("launch actual standalone cadence-matching binary");
    let mut child = ChildGuard::new(child);

    let mut client = matching_client(grpc_port, &mut child, &child_log).await;
    let mut request = Request::new(add_request(domain_id, "malformed-run-id".into()));
    request.set_timeout(Duration::from_secs(10));
    let response = tokio::time::timeout(Duration::from_secs(12), client.add_decision_task(request)).await
        .expect("AddDecisionTask RPC exceeded its deadline");
    let rpc_error = match response {
        Ok(_) => panic!("malformed Add must never receive successful acknowledgement"),
        Err(error) => error,
    };

    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(status) = child.child_mut().try_wait().expect("wait for Matching child") { break status; }
        assert!(Instant::now() < deadline, "fatal writer notification did not terminate process; RPC error: {}; child log: {}", rpc_error.message(), std::fs::read_to_string(&child_log).unwrap_or_default());
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let output = child.finish();
    let logs = std::fs::read_to_string(&child_log).unwrap_or_else(|_| String::from_utf8_lossy(&output.stderr).into_owned());
    assert_eq!(status.code(), Some(2), "standalone cadence-matching must map typed SQL writer failure to exit 2; RPC error: {}; child log: {logs}", rpc_error.message());
    assert!(logs.contains("invalid SQL Matching RunID"), "fatal typed error marker proves Add reached the SQL writer: RPC code={:?}, message={:?}; child log: {logs}", rpc_error.code(), rpc_error.message());
}
