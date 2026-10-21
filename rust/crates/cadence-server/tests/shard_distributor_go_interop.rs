//! Ignored mixed-language gate against the pinned Go shard-manager handlers.
//! Run through tests/run-shard-distributor-go-interop.sh after validation is
//! authorized; it is source-only during the current review lane.
use cadence_proto::sharddistributor as sd;
use cadence_server::{
    config::{ShardDistributorMatchingConfig, ShardDistributorNamespaceConfig},
    matching_shard_distributor::{
        BoxFuture, ShardDistributorExecutor, ShardProcessor, ShardProcessorFactory, ShardReport,
        TonicExecutorRpc, TonicSpectator,
    },
};
use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Write},
    process::{Child, ChildStdin, Command, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver},
        Arc,
    },
    time::Duration,
};
use tokio::time::Instant;

struct GoFixture {
    child: Child,
    input: ChildStdin,
    output: Receiver<String>,
    address: String,
}
impl GoFixture {
    fn start() -> Self {
        let go = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../go");
        let mut command = Command::new("go");
        command
            .args(["run", "./tests/shard-distributor-interoperability"])
            .current_dir(go)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        let mut child = command
            .spawn()
            .expect("launch pinned Go shard-manager fixture");
        let input = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (output_tx, output) = mpsc::channel();
        std::thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) | Err(_) => return,
                    Ok(_) if output_tx.send(line).is_err() => return,
                    Ok(_) => {}
                }
            }
        });
        let line = match read_json_line(&output) {
            Ok(line) => line,
            Err(error) => {
                kill_go_process_group(&mut child);
                panic!("Go fixture startup response failed: {error}");
            }
        };
        let address = line["address"]
            .as_str()
            .expect("Go fixture address")
            .to_string();
        Self {
            child,
            input,
            output,
            address,
        }
    }
    fn command(&mut self, value: serde_json::Value) -> serde_json::Value {
        writeln!(self.input, "{value}").unwrap();
        self.input.flush().unwrap();
        read_json_line(&self.output)
            .unwrap_or_else(|error| panic!("Go fixture command failed: {error}"))["result"]
            .clone()
    }
}
impl Drop for GoFixture {
    fn drop(&mut self) {
        let _ = writeln!(self.input, "{{\"op\":\"stop\"}}");
        let _ = self.input.flush();
        let _ = self.output.recv_timeout(Duration::from_secs(2));
        kill_go_process_group(&mut self.child);
    }
}
fn kill_go_process_group(child: &mut Child) {
    #[cfg(unix)]
    if let Ok(pid) = i32::try_from(child.id()) {
        // `go run` owns a compiler-launched child in the same process group.
        unsafe { libc::kill(-pid, libc::SIGKILL) };
    }
    let _ = child.kill();
    let _ = child.wait();
}
fn read_json_line(reader: &Receiver<String>) -> Result<serde_json::Value, String> {
    let line = reader
        .recv_timeout(Duration::from_secs(5))
        .map_err(|error| format!("timed out waiting for Go fixture output: {error}"))?;
    serde_json::from_str(&line)
        .map_err(|error| format!("Go fixture emitted invalid JSON {line:?}: {error}"))
}

#[derive(Default)]
struct Probe {
    starts: AtomicUsize,
    stops: AtomicUsize,
}
struct Processor(Arc<Probe>);
impl ShardProcessor for Processor {
    fn start(&self) -> BoxFuture<Result<(), String>> {
        self.0.starts.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
    fn stop(&self) -> BoxFuture<()> {
        self.0.stops.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {})
    }
    fn report(&self) -> ShardReport {
        ShardReport {
            status: sd::ShardStatus::Ready,
            load: 0.375,
        }
    }
    fn set_status(&self, _: sd::ShardStatus) {}
}
struct Factory(Arc<Probe>);
impl ShardProcessorFactory for Factory {
    fn create(&self, _: String) -> BoxFuture<Result<Arc<dyn ShardProcessor>, String>> {
        let processor: Arc<dyn ShardProcessor> = Arc::new(Processor(self.0.clone()));
        Box::pin(async move { Ok(processor) })
    }
}

async fn until(label: &str, mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("interop condition timed out: {label}"));
}

#[tokio::test]
#[ignore = "mixed Go/Rust wire gate; requires Go dependency cache and serialized validation lane"]
async fn rust_executor_and_spectator_interoperate_with_pinned_go_handlers() {
    let mut go = GoFixture::start();
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", go.address))
        .unwrap()
        .connect()
        .await
        .unwrap();
    assert_eq!(go.command(serde_json::json!({"op":"go-watch"})), "started");
    until("Go YARPC baseline subscriber", || {
        go.command(serde_json::json!({"op":"watchers"}))
            .as_u64()
            .is_some_and(|count| count > 0)
    })
    .await;
    assert_eq!(
        go.command(serde_json::json!({"op":"stop-go-watch"})),
        "stopped"
    );
    until("Go baseline unsubscribed", || {
        go.command(serde_json::json!({"op":"watchers"})).as_u64() == Some(0)
    })
    .await;
    let enabled: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(|| true);
    let spectator = TonicSpectator::new(channel.clone(), "matching", enabled.clone());
    spectator.start().await.unwrap();
    until("Rust YARPC subscriber", || {
        go.command(serde_json::json!({"op":"watchers"}))
            .as_u64()
            .is_some_and(|count| count > 0)
    })
    .await;
    let probe = Arc::new(Probe::default());
    let cfg = ShardDistributorMatchingConfig {
        namespaces: vec![ShardDistributorNamespaceConfig {
            namespace: "matching".into(),
            heartbeat_interval_nanos: 20_000_000,
            ttl_shard_nanos: 200_000_000,
            ..Default::default()
        }],
        ..Default::default()
    };
    let executor = ShardDistributorExecutor::new(
        &cfg,
        Arc::new(TonicExecutorRpc::from_channel(channel)),
        Arc::new(Factory(probe.clone())),
        enabled,
        HashMap::from([("integration".into(), "rust".into())]),
        None,
    )
    .unwrap();
    executor.start().await.unwrap();

    assert_eq!(go.command(serde_json::json!({"op":"assign", "namespace":"matching", "executor":executor.executor_id(), "shards":["shard-a"], "metadata":{"host":"go-fixture"}})), "ok");
    let owner = spectator
        .get_shard_owner("shard-a", Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(owner.executor_id, executor.executor_id());
    assert_eq!(
        owner.metadata.get("host").map(String::as_str),
        Some("go-fixture")
    );
    until("first executor processor start", || {
        probe.starts.load(Ordering::SeqCst) == 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(60)).await;
    let stats = go.command(serde_json::json!({"op":"stats", "namespace":"matching"}));
    assert_eq!(
        stats[executor.executor_id()]["Metadata"]["integration"],
        "rust"
    );
    assert_eq!(
        stats[executor.executor_id()]["ReportedShards"]["shard-a"]["ShardLoad"].as_f64(),
        Some(0.375)
    );

    go.command(serde_json::json!({"op":"reassign", "namespace":"matching", "executor":executor.executor_id(), "shards":["shard-b"], "metadata":{"host":"go-fixture"}}));
    until("reassigned processor lifecycle", || {
        probe.starts.load(Ordering::SeqCst) == 2 && probe.stops.load(Ordering::SeqCst) == 1
    })
    .await;
    let reassigned = spectator
        .get_shard_owner("shard-b", Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(reassigned.executor_id, executor.executor_id());
    until("reassignment state report", || {
        let state = go.command(serde_json::json!({"op":"stats", "namespace":"matching"}));
        state[executor.executor_id()]["ReportedShards"]["shard-b"]["Status"] == "ShardStatusREADY"
    })
    .await;

    go.command(serde_json::json!({"op":"lose", "namespace":"matching"}));
    until("lost assignment stops processor", || {
        probe.stops.load(Ordering::SeqCst) == 2
    })
    .await;
    executor.shutdown().await;
    spectator.shutdown().await;
    let heartbeat = go.command(serde_json::json!({"op":"stats", "namespace":"matching"}));
    assert_eq!(
        heartbeat[executor.executor_id()]["Status"],
        "ExecutorStatusDRAINING"
    );
    assert!(heartbeat[executor.executor_id()]["ReportedShards"]
        .get("shard-a")
        .is_none());
}
