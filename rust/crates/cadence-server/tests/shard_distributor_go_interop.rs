//! Ignored mixed-language gate against the pinned Go shard-manager handlers.
//! Run through tests/run-shard-distributor-review-gates.sh after validation is
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
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::time::Instant;

struct GoFixture {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    address: String,
}
impl GoFixture {
    fn start() -> Self {
        let go = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../go");
        let mut child = Command::new("go")
            .args(["run", "./tests/shard-distributor-interoperability"])
            .current_dir(go)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("launch pinned Go shard-manager fixture");
        let input = child.stdin.take().unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        let line = read_json_line(&mut output);
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
        read_json_line(&mut self.output)["result"].clone()
    }
}
impl Drop for GoFixture {
    fn drop(&mut self) {
        let _ = self.command(serde_json::json!({"op":"stop"}));
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn read_json_line(reader: &mut impl BufRead) -> serde_json::Value {
    let mut line = String::new();
    reader.read_line(&mut line).expect("read Go fixture output");
    serde_json::from_str(&line).expect("Go fixture emits JSON lines")
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

async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("interop condition timed out");
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
    let enabled: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(|| true);
    let spectator = TonicSpectator::new(channel.clone(), "matching", enabled.clone());
    spectator.start().await.unwrap();
    until(|| {
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
    until(|| probe.starts.load(Ordering::SeqCst) == 1).await;
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
    until(|| probe.starts.load(Ordering::SeqCst) == 2 && probe.stops.load(Ordering::SeqCst) == 1)
        .await;
    let reassigned = spectator
        .get_shard_owner("shard-b", Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(reassigned.executor_id, executor.executor_id());
    until(|| {
        let state = go.command(serde_json::json!({"op":"stats", "namespace":"matching"}));
        state[executor.executor_id()]["ReportedShards"]["shard-b"]["Status"] == "DONE"
    })
    .await;

    go.command(serde_json::json!({"op":"lose", "namespace":"matching"}));
    until(|| probe.stops.load(Ordering::SeqCst) == 2).await;
    executor.shutdown().await;
    spectator.shutdown().await;
    let heartbeat = go.command(serde_json::json!({"op":"stats", "namespace":"matching"}));
    assert_eq!(heartbeat[executor.executor_id()]["Status"], "DRAINING");
    assert!(heartbeat[executor.executor_id()]["ReportedShards"]
        .get("shard-a")
        .is_none());
}
