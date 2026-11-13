//! Advanced visibility writer: port of the write half of
//! `go/common/persistence/elasticsearch/es_visibility_store.go` (Kafka `indexer.Message`
//! producer), `go/common/messaging/kafka/producer_impl.go` (`*indexer.Message` branch) and
//! `go/common/persistence/visibility_hybrid_manager.go` (`chooseVisibilityManagerForWrite`).
//!
//! Wire format: Kafka record key = workflowID, value = ThriftRW (0x59 version prefix) encoded
//! `indexer.Message` (`go/idls/thrift/indexer.thrift`); partition = Sarama default hash partitioner.
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use cadence_persistence::history_contract::PersistenceError;
use cadence_thrift::value::{write_struct, Struct, Value};
use cadence_thrift::Writer;
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use rskafka::record::Record;
use tokio::sync::OnceCell;

use super::visibility::{VisibilityWriteRow, VisibilityWriter};

// Go `common/elasticsearch/defs.go` field names.
const WORKFLOW_TYPE: &str = "WorkflowType";
const START_TIME: &str = "StartTime";
const EXECUTION_TIME: &str = "ExecutionTime";
const CLOSE_TIME: &str = "CloseTime";
const CLOSE_STATUS: &str = "CloseStatus";
const HISTORY_LENGTH: &str = "HistoryLength";
const MEMO: &str = "Memo";
const ENCODING: &str = "Encoding";
const TASK_LIST: &str = "TaskList";
const IS_CRON: &str = "IsCron";
const NUM_CLUSTERS: &str = "NumClusters";
const CLUSTER_ATTRIBUTE_SCOPE: &str = "ClusterAttributeScope";
const CLUSTER_ATTRIBUTE_NAME: &str = "ClusterAttributeName";
const UPDATE_TIME: &str = "UpdateTime";
const SHARD_ID: &str = "ShardID";
const CRON_SCHEDULE: &str = "CronSchedule";
const EXECUTION_STATUS: &str = "ExecutionStatus";
const SCHEDULED_EXECUTION_TIME: &str = "ScheduledExecutionTime";

// indexer.thrift enums.
const MESSAGE_TYPE_INDEX: i32 = 0;
const MESSAGE_TYPE_DELETE: i32 = 1;
const MESSAGE_TYPE_CREATE: i32 = 2;
const OP_RECORD_STARTED: i32 = 0;
const OP_RECORD_CLOSED: i32 = 1;
const OP_UPSERT_SEARCH_ATTRIBUTES: i32 = 2;
const FIELD_TYPE_STRING: i32 = 0;
const FIELD_TYPE_INT: i32 = 1;
const FIELD_TYPE_BOOL: i32 = 2;
const FIELD_TYPE_BINARY: i32 = 3;

fn field_string(v: &str) -> Value {
    Value::Struct(Struct { fields: vec![(10, Value::I32(FIELD_TYPE_STRING)), (20, Value::Bytes(v.as_bytes().to_vec()))] })
}
fn field_int(v: i64) -> Value {
    Value::Struct(Struct { fields: vec![(10, Value::I32(FIELD_TYPE_INT)), (30, Value::I64(v))] })
}
fn field_bool(v: bool) -> Value {
    Value::Struct(Struct { fields: vec![(10, Value::I32(FIELD_TYPE_BOOL)), (40, Value::Bool(v))] })
}
fn field_binary(v: &[u8]) -> Value {
    Value::Struct(Struct { fields: vec![(10, Value::I32(FIELD_TYPE_BINARY)), (50, Value::Bytes(v.to_vec()))] })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum VisibilityOperation {
    RecordStarted,
    RecordClosed,
    UpsertSearchAttributes,
}

/// Go `indexer.Message`: (workflowID, versioned ThriftRW bytes).
fn encode_message(
    message_type: i32,
    row: &VisibilityWriteRow,
    version: Option<i64>,
    fields: Option<Vec<(Value, Value)>>,
    operation: Option<i32>,
) -> (String, Vec<u8>) {
    let mut s = vec![
        (10, Value::I32(message_type)),
        (20, Value::Bytes(row.domain_id.as_bytes().to_vec())),
        (30, Value::Bytes(row.workflow_id.as_bytes().to_vec())),
        (40, Value::Bytes(row.run_id.as_bytes().to_vec())),
    ];
    if let Some(v) = version {
        s.push((50, Value::I64(v)));
    }
    if let Some(f) = fields {
        s.push((60, Value::Map(f)));
    }
    if let Some(op) = operation {
        s.push((70, Value::I32(op)));
    }
    let mut w = Writer::new();
    write_struct(&mut w, &Struct { fields: s });
    let mut out = vec![0x59];
    out.extend(w.into_bytes());
    (row.workflow_id.clone(), out)
}

fn kv(k: &str, v: Value) -> (Value, Value) {
    (Value::Bytes(k.as_bytes().to_vec()), v)
}

/// Go sends UnixNano; the Rust record may carry milliseconds, so scale small values up.
fn nanos(ts: i64) -> i64 {
    if ts != 0 && ts.abs() < 100_000_000_000_000 {
        ts.saturating_mul(1_000_000)
    } else {
        ts
    }
}

/// Go `createVisibilityMessage`.
fn create_visibility_message(row: &VisibilityWriteRow, op: VisibilityOperation) -> (String, Vec<u8>) {
    let mut fields: BTreeMap<String, Value> = BTreeMap::new();
    fields.insert(WORKFLOW_TYPE.into(), field_string(&row.workflow_type_name));
    fields.insert(START_TIME.into(), field_int(nanos(row.start_time)));
    fields.insert(EXECUTION_TIME.into(), field_int(nanos(row.execution_time)));
    fields.insert(TASK_LIST.into(), field_string(&row.task_list));
    fields.insert(IS_CRON.into(), field_bool(row.is_cron));
    fields.insert(NUM_CLUSTERS.into(), field_int(i64::from(row.num_clusters)));
    fields.insert(CLUSTER_ATTRIBUTE_SCOPE.into(), field_string(&row.cluster_attribute_scope));
    fields.insert(CLUSTER_ATTRIBUTE_NAME.into(), field_string(&row.cluster_attribute_name));
    fields.insert(UPDATE_TIME.into(), field_int(nanos(row.update_time)));
    fields.insert(SHARD_ID.into(), field_int(row.shard_id));
    fields.insert(CRON_SCHEDULE.into(), field_string(&row.cron_schedule));
    fields.insert(EXECUTION_STATUS.into(), field_int(i64::from(row.request_execution_status)));
    fields.insert(SCHEDULED_EXECUTION_TIME.into(), field_int(nanos(row.scheduled_execution_time)));
    if !row.memo.is_empty() {
        fields.insert(MEMO.into(), field_binary(&row.memo));
        fields.insert(ENCODING.into(), field_string(&row.encoding));
    }
    for (k, v) in &row.search_attributes {
        fields.insert(k.clone(), field_binary(v));
    }
    if op == VisibilityOperation::RecordClosed {
        let (close_time, close_status, history_length) = row.closed.unwrap_or((0, 0, 0));
        fields.insert(CLOSE_TIME.into(), field_int(nanos(close_time)));
        fields.insert(CLOSE_STATUS.into(), field_int(i64::from(close_status)));
        fields.insert(HISTORY_LENGTH.into(), field_int(history_length));
    }
    let op = match op {
        VisibilityOperation::RecordStarted => OP_RECORD_STARTED,
        VisibilityOperation::RecordClosed => OP_RECORD_CLOSED,
        VisibilityOperation::UpsertSearchAttributes => OP_UPSERT_SEARCH_ATTRIBUTES,
    };
    let fields = fields.into_iter().map(|(k, v)| kv(&k, v)).collect();
    encode_message(MESSAGE_TYPE_INDEX, row, Some(row.task_id), Some(fields), Some(op))
}

/// Go `getVisibilityMessageForUninitializedWorkflow`.
fn uninitialized_message(row: &VisibilityWriteRow) -> (String, Vec<u8>) {
    let fields = vec![
        kv(WORKFLOW_TYPE, field_string(&row.workflow_type_name)),
        kv(UPDATE_TIME, field_int(nanos(row.update_time))),
        kv(SHARD_ID, field_int(row.shard_id)),
    ];
    encode_message(MESSAGE_TYPE_CREATE, row, None, Some(fields), None)
}

/// Go `getVisibilityMessageForDeletion`.
fn deletion_message(domain_id: &str, workflow_id: &str, run_id: &str, task_id: i64) -> (String, Vec<u8>) {
    let row = VisibilityWriteRow {
        domain_id: domain_id.into(),
        workflow_id: workflow_id.into(),
        run_id: run_id.into(),
        ..Default::default()
    };
    encode_message(MESSAGE_TYPE_DELETE, &row, Some(task_id), None, None)
}

/// Sarama default hash partitioner: FNV-1a 32, `int32(hash) % n`, negated when negative.
pub fn sarama_partition(key: &[u8], partitions: i32) -> i32 {
    let mut hash: u32 = 0x811c_9dc5;
    for b in key {
        hash ^= u32::from(*b);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    if partitions <= 0 {
        return 0;
    }
    let p = (hash as i32).wrapping_rem(partitions);
    if p < 0 {
        -p
    } else {
        p
    }
}

/// Go `messaging.Producer` for a single topic.
#[async_trait::async_trait]
pub trait MessageProducer: Send + Sync {
    async fn publish(&self, key: &str, value: Vec<u8>) -> Result<(), PersistenceError>;
}

struct Connected {
    client: Client,
    partitions: i32,
}

/// Go `kafka.producerImpl` (sync producer; the publish resolves after the broker acknowledged).
/// The broker connection is established lazily on first publish.
pub struct KafkaProducer {
    brokers: Vec<String>,
    topic: String,
    connected: OnceCell<Connected>,
    partition_clients: Mutex<HashMap<i32, Arc<PartitionClient>>>,
}

impl KafkaProducer {
    pub fn new(brokers: Vec<String>, topic: String) -> Self {
        Self { brokers, topic, connected: OnceCell::new(), partition_clients: Mutex::new(HashMap::new()) }
    }

    async fn connect(&self) -> anyhow::Result<Connected> {
        let (client, topics) = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let client = ClientBuilder::new(self.brokers.clone()).build().await?;
            let topics = client.list_topics().await?;
            anyhow::Ok((client, topics))
        })
        .await
        .map_err(|_| anyhow::anyhow!("timed out connecting to kafka brokers"))??;
        let topic = topics
            .iter()
            .find(|t| t.name == self.topic)
            .ok_or_else(|| anyhow::anyhow!("kafka topic {} not found", self.topic))?;
        Ok(Connected { client, partitions: i32::try_from(topic.partitions.len())? })
    }

    async fn partition_client(&self, c: &Connected, partition: i32) -> anyhow::Result<Arc<PartitionClient>> {
        if let Some(p) = self.partition_clients.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.get(&partition) {
            return Ok(p.clone());
        }
        let created = Arc::new(c.client.partition_client(self.topic.clone(), partition, UnknownTopicHandling::Error).await?);
        self.partition_clients.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.insert(partition, created.clone());
        Ok(created)
    }

    async fn send(&self, key: &str, value: Vec<u8>) -> anyhow::Result<()> {
        let connected = self.connected.get_or_try_init(|| self.connect()).await?;
        let partition = sarama_partition(key.as_bytes(), connected.partitions);
        let client = self.partition_client(connected, partition).await?;
        let record = Record { key: Some(key.as_bytes().to_vec()), value: Some(value), headers: BTreeMap::new(), timestamp: chrono::Utc::now() };
        if let Err(e) = client.produce(vec![record], Compression::NoCompression).await {
            if let Ok(mut m) = self.partition_clients.lock() {
                m.remove(&partition);
            }
            anyhow::bail!("failed to publish message to kafka topic {}: {e}", self.topic);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl MessageProducer for KafkaProducer {
    async fn publish(&self, key: &str, value: Vec<u8>) -> Result<(), PersistenceError> {
        self.send(key, value).await.map_err(|e| PersistenceError::Internal(e.to_string()))
    }
}

/// Go `esVisibilityStore` write half.
pub struct EsVisibilityWriter {
    producer: Arc<dyn MessageProducer>,
}

impl EsVisibilityWriter {
    pub fn new(producer: Arc<dyn MessageProducer>) -> Self {
        Self { producer }
    }
}

#[async_trait::async_trait]
impl VisibilityWriter for EsVisibilityWriter {
    async fn record_started_async(&self, row: VisibilityWriteRow) -> Result<(), PersistenceError> {
        let (k, v) = create_visibility_message(&row, VisibilityOperation::RecordStarted);
        self.producer.publish(&k, v).await
    }
    async fn record_closed_async(&self, row: VisibilityWriteRow) -> Result<(), PersistenceError> {
        let (k, v) = create_visibility_message(&row, VisibilityOperation::RecordClosed);
        self.producer.publish(&k, v).await
    }
    fn record_uninitialized(&self) -> Result<(), PersistenceError> {
        Ok(())
    }
    fn upsert(&self) -> Result<(), PersistenceError> {
        Ok(())
    }
    async fn delete_async(&self, _domain_id: String, _run_id: String) -> Result<(), PersistenceError> {
        Ok(())
    }
    async fn record_uninitialized_row(&self, row: VisibilityWriteRow) -> Result<(), PersistenceError> {
        let (k, v) = uninitialized_message(&row);
        self.producer.publish(&k, v).await
    }
    async fn upsert_row(&self, row: VisibilityWriteRow) -> Result<(), PersistenceError> {
        let (k, v) = create_visibility_message(&row, VisibilityOperation::UpsertSearchAttributes);
        self.producer.publish(&k, v).await
    }
    async fn delete_row(&self, domain_id: String, workflow_id: String, run_id: String, task_id: i64) -> Result<(), PersistenceError> {
        let (k, v) = deletion_message(&domain_id, &workflow_id, &run_id, task_id);
        self.producer.publish(&k, v).await
    }
}

/// Go `visibilityHybridManager` write path (`chooseVisibilityManagerForWrite`).
pub struct HybridVisibilityWriter {
    mgrs: HashMap<String, Arc<dyn VisibilityWriter>>,
    write_mode: Arc<dyn Fn() -> String + Send + Sync>,
}

impl HybridVisibilityWriter {
    pub fn new(mgrs: HashMap<String, Arc<dyn VisibilityWriter>>, write_mode: Arc<dyn Fn() -> String + Send + Sync>) -> Self {
        Self { mgrs, write_mode }
    }

    /// Ordered list of store names a write is sent to (the Go loop), `Err` entries are errors.
    fn plan(&self) -> (Vec<String>, Vec<String>) {
        let write_mode = (self.write_mode)();
        let mut targets = Vec::new();
        let mut errors = Vec::new();
        for raw in write_mode.split(',') {
            let mut mode = raw.trim().to_lowercase();
            if mode == "off" {
                mode = "db".to_string();
            }
            if self.mgrs.contains_key(&mode) {
                targets.push(mode);
            } else if mode != "db" && !write_mode.contains("db") {
                // Go: requested mode unavailable, fall back to db.
                if self.mgrs.contains_key("db") {
                    targets.push("db".to_string());
                } else {
                    errors.push("Visibility store manager with name db not found".to_string());
                }
            } else {
                errors.push(format!("DB visibility mode is not available: {mode}"));
            }
        }
        (targets, errors)
    }

    fn finish(errors: Vec<String>) -> Result<(), PersistenceError> {
        if errors.is_empty() {
            Ok(())
        } else {
            Err(PersistenceError::Internal(format!("Error writing to visibility: {}", errors.join("; "))))
        }
    }
}

#[async_trait::async_trait]
impl VisibilityWriter for HybridVisibilityWriter {
    async fn record_started_async(&self, row: VisibilityWriteRow) -> Result<(), PersistenceError> {
        let (targets, mut errors) = self.plan();
        for t in targets {
            if let Err(e) = self.mgrs[&t].record_started_async(row.clone()).await {
                errors.push(e.to_string());
            }
        }
        Self::finish(errors)
    }
    async fn record_closed_async(&self, row: VisibilityWriteRow) -> Result<(), PersistenceError> {
        let (targets, mut errors) = self.plan();
        for t in targets {
            if let Err(e) = self.mgrs[&t].record_closed_async(row.clone()).await {
                errors.push(e.to_string());
            }
        }
        Self::finish(errors)
    }
    fn record_uninitialized(&self) -> Result<(), PersistenceError> {
        Ok(())
    }
    fn upsert(&self) -> Result<(), PersistenceError> {
        Ok(())
    }
    async fn delete_async(&self, domain_id: String, run_id: String) -> Result<(), PersistenceError> {
        self.delete_row(domain_id, String::new(), run_id, 0).await
    }
    async fn record_uninitialized_row(&self, row: VisibilityWriteRow) -> Result<(), PersistenceError> {
        let (targets, mut errors) = self.plan();
        for t in targets {
            if let Err(e) = self.mgrs[&t].record_uninitialized_row(row.clone()).await {
                errors.push(e.to_string());
            }
        }
        Self::finish(errors)
    }
    async fn upsert_row(&self, row: VisibilityWriteRow) -> Result<(), PersistenceError> {
        let (targets, mut errors) = self.plan();
        for t in targets {
            if let Err(e) = self.mgrs[&t].upsert_row(row.clone()).await {
                errors.push(e.to_string());
            }
        }
        Self::finish(errors)
    }
    async fn delete_row(&self, domain_id: String, workflow_id: String, run_id: String, task_id: i64) -> Result<(), PersistenceError> {
        let (targets, mut errors) = self.plan();
        for t in targets {
            if let Err(e) = self.mgrs[&t].delete_row(domain_id.clone(), workflow_id.clone(), run_id.clone(), task_id).await {
                errors.push(e.to_string());
            }
        }
        Self::finish(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partitioner_matches_sarama() {
        assert_eq!(sarama_partition(b"", 3), (0x811c_9dc5u32 as i32).wrapping_rem(3).abs());
        assert_eq!(sarama_partition(b"a", 0), 0);
    }

    #[test]
    fn message_has_version_prefix_and_key() {
        let row = VisibilityWriteRow { workflow_id: "wf".into(), run_id: "r".into(), domain_id: "d".into(), ..Default::default() };
        let (k, v) = create_visibility_message(&row, VisibilityOperation::RecordStarted);
        assert_eq!(k, "wf");
        assert_eq!(v[0], 0x59);
        let (_, d) = deletion_message("d", "wf", "r", 7);
        assert_eq!(d[0], 0x59);
        let (_, u) = uninitialized_message(&row);
        assert_eq!(u[0], 0x59);
    }

    struct Rec(Mutex<Vec<String>>);
    #[async_trait::async_trait]
    impl MessageProducer for Rec {
        async fn publish(&self, key: &str, _v: Vec<u8>) -> Result<(), PersistenceError> {
            self.0.lock().unwrap().push(key.to_string());
            Ok(())
        }
    }

    #[tokio::test]
    async fn hybrid_falls_back_to_db_when_mode_unavailable() {
        let db = Arc::new(Rec(Mutex::new(vec![])));
        let es = Arc::new(Rec(Mutex::new(vec![])));
        let mut m: HashMap<String, Arc<dyn VisibilityWriter>> = HashMap::new();
        m.insert("db".into(), Arc::new(EsVisibilityWriter::new(db.clone())));
        let h = HybridVisibilityWriter::new(m.clone(), Arc::new(|| "es".to_string()));
        h.record_started_async(VisibilityWriteRow { workflow_id: "w".into(), ..Default::default() }).await.unwrap();
        assert_eq!(db.0.lock().unwrap().len(), 1);
        m.insert("es".into(), Arc::new(EsVisibilityWriter::new(es.clone())));
        let h = HybridVisibilityWriter::new(m, Arc::new(|| "es, db".to_string()));
        h.record_started_async(VisibilityWriteRow { workflow_id: "w".into(), ..Default::default() }).await.unwrap();
        assert_eq!(es.0.lock().unwrap().len(), 1);
        assert_eq!(db.0.lock().unwrap().len(), 2);
    }
}
