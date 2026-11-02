//! Kafka transport for the async workflow publisher (Go
//! `common/asyncworkflow/queue/kafka` + `common/messaging/kafka/producer_impl.go`).
//!
//! Go publishes with a synchronous Sarama producer (`Return.Successes`): the
//! RPC succeeds only after the broker acknowledged. The Kafka record is
//! key = partition key, value = versioned (0x59) ThriftRW
//! `sqlblobs.AsyncRequestMessage{partitionKey,type,header,encoding,payload}`;
//! the original RPC headers travel inside that struct, not as Kafka headers.
//! Partition selection reproduces Sarama's default hash partitioner
//! (FNV-1a 32, sign fix, modulo partition count).
//!
//! Differences from Go (documented, not hidden): the broker connection is
//! established lazily on first publish (Go connects at producer creation; a
//! failure surfaces as the publish error either way), TLS `serverName`
//! override is not supported by the client, and no topic-tagged metrics exist
//! yet because the Frontend has no metrics seam.

use crate::async_publisher::{
    AsyncMessageProducer, AsyncProducerFactory, ProducerFuture, ProducerMessage,
};
use crate::async_queue::{AsyncWorkflowQueue, AsyncWorkflowQueueProviderFactory};
use cadence_thrift::value::{write_struct, Struct, Value};
use cadence_thrift::Writer;
use chrono::Utc;
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder, Credentials, SaslConfig};
use rskafka::record::Record;
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::OnceCell;

pub const KAFKA_QUEUE_TYPE: &str = "kafka";

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct TlsConfig {
    pub enabled: bool,
    pub cert_file: String,
    pub key_file: String,
    pub ca_file: String,
    pub ca_files: Vec<String>,
    pub enable_host_verification: bool,
    pub require_client_auth: bool,
    pub server_name: String,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct SaslSettings {
    pub enabled: bool,
    pub user: String,
    pub password: String,
    /// plain, sha512 or sha256
    pub algorithm: String,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ConnectionConfig {
    pub brokers: Vec<String>,
    pub tls: TlsConfig,
    pub sasl: SaslSettings,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct KafkaQueueConfig {
    pub connection: ConnectionConfig,
    pub topic: String,
}

impl KafkaQueueConfig {
    /// Go: brokers sorted at queue construction.
    pub fn from_yaml(value: Option<&serde_yaml::Value>) -> anyhow::Result<Self> {
        let value = value.ok_or_else(|| anyhow::anyhow!("bad config: missing kafka config"))?;
        let mut cfg: Self = serde_yaml::from_value(value.clone())
            .map_err(|e| anyhow::anyhow!("bad config: {e}"))?;
        cfg.connection.brokers.sort();
        Ok(cfg)
    }

    /// Go `queueConfig.ID()`.
    pub fn id(&self) -> String {
        format!(
            "kafka::{}/{}",
            self.topic,
            self.connection.brokers.join(",")
        )
    }
}

struct KafkaQueue {
    id: String,
}

impl AsyncWorkflowQueue for KafkaQueue {
    fn id(&self) -> &str {
        &self.id
    }
}

/// Go `newSaramaConfigWithAuth` SASL mapping.
pub fn sasl_config(sasl: &SaslSettings) -> anyhow::Result<Option<SaslConfig>> {
    if !sasl.enabled {
        return Ok(None);
    }
    let creds = Credentials::new(sasl.user.clone(), sasl.password.clone());
    Ok(Some(match sasl.algorithm.as_str() {
        "sha512" => SaslConfig::ScramSha512(creds),
        "sha256" => SaslConfig::ScramSha256(creds),
        "plain" => SaslConfig::Plain(creds),
        other => anyhow::bail!("unknown SASL algorithm {other}"),
    }))
}

/// Sarama default hash partitioner: FNV-1a 32 of the key, as int32, negated
/// when negative, modulo the partition count.
pub fn sarama_partition(key: &[u8], partitions: i32) -> i32 {
    let mut hash: u32 = 0x811c_9dc5;
    for b in key {
        hash ^= u32::from(*b);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    let mut signed = hash as i32;
    if signed < 0 {
        signed = signed.wrapping_neg();
    }
    if partitions <= 0 {
        return 0;
    }
    signed.wrapping_rem(partitions).abs()
}

/// Versioned ThriftRW `sqlblobs.AsyncRequestMessage` (Go codec prefix 0x59).
pub fn encode_async_request_message(message: &ProducerMessage) -> Vec<u8> {
    let mut header_fields: Vec<(Value, Value)> = message
        .headers
        .iter()
        .map(|(k, v)| (Value::Bytes(k.as_bytes().to_vec()), Value::Bytes(v.clone())))
        .collect();
    header_fields.sort_by(|a, b| match (&a.0, &b.0) {
        (Value::Bytes(x), Value::Bytes(y)) => x.cmp(y),
        _ => std::cmp::Ordering::Equal,
    });
    let header = Struct {
        fields: vec![(10, Value::Map(header_fields))],
    };
    let body = Struct {
        fields: vec![
            (10, Value::Bytes(message.partition_key.as_bytes().to_vec())),
            (12, Value::I32(message.request_type)),
            (14, Value::Struct(header)),
            (16, Value::Bytes(message.encoding.as_bytes().to_vec())),
            (18, Value::Bytes(message.payload.clone())),
        ],
    };
    let mut writer = Writer::new();
    write_struct(&mut writer, &body);
    let mut out = vec![0x59];
    out.extend(writer.into_bytes());
    out
}

/// Kafka record for a queue message (Go `getProducerMessage`).
pub fn build_record(message: &ProducerMessage) -> Record {
    Record {
        key: Some(message.partition_key.as_bytes().to_vec()),
        value: Some(encode_async_request_message(message)),
        headers: BTreeMap::new(),
        timestamp: Utc::now(),
    }
}

struct Connected {
    client: Client,
    partitions: i32,
}

pub struct KafkaProducer {
    config: Arc<KafkaQueueConfig>,
    connected: OnceCell<Connected>,
    partition_clients: Mutex<HashMap<i32, Arc<PartitionClient>>>,
    closed: AtomicBool,
}

impl KafkaProducer {
    pub fn new(config: Arc<KafkaQueueConfig>) -> Self {
        Self {
            config,
            connected: OnceCell::new(),
            partition_clients: Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
        }
    }

    async fn connect(&self) -> anyhow::Result<Connected> {
        let conn = &self.config.connection;
        let mut builder = ClientBuilder::new(conn.brokers.clone());
        if let Some(tls) = crate::async_kafka_tls::client_config(&conn.tls)? {
            builder = builder.tls_config(tls);
        }
        if let Some(sasl) = sasl_config(&conn.sasl)? {
            builder = builder.sasl_config(sasl);
        }
        let client = builder.build().await?;
        let topics = client.list_topics().await?;
        let topic = topics
            .iter()
            .find(|t| t.name == self.config.topic)
            .ok_or_else(|| anyhow::anyhow!("kafka topic {} not found", self.config.topic))?;
        Ok(Connected {
            client,
            partitions: i32::try_from(topic.partitions.len())?,
        })
    }

    async fn partition_client(
        &self,
        connected: &Connected,
        partition: i32,
    ) -> anyhow::Result<Arc<PartitionClient>> {
        if let Some(c) = self
            .partition_clients
            .lock()
            .map_err(|_| anyhow::anyhow!("kafka partition cache poisoned"))?
            .get(&partition)
        {
            return Ok(c.clone());
        }
        let created = Arc::new(
            connected
                .client
                .partition_client(
                    self.config.topic.clone(),
                    partition,
                    UnknownTopicHandling::Error,
                )
                .await?,
        );
        self.partition_clients
            .lock()
            .map_err(|_| anyhow::anyhow!("kafka partition cache poisoned"))?
            .insert(partition, created.clone());
        Ok(created)
    }

    async fn send(&self, message: ProducerMessage) -> anyhow::Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            anyhow::bail!("kafka producer is closed");
        }
        let connected = self.connected.get_or_try_init(|| self.connect()).await?;
        let partition = sarama_partition(message.partition_key.as_bytes(), connected.partitions);
        let client = self.partition_client(connected, partition).await?;
        // produce resolves after the broker acknowledged (acks=all default).
        if let Err(error) = client
            .produce(vec![build_record(&message)], Compression::NoCompression)
            .await
        {
            // Drop the cached leader connection so the next publish re-resolves it.
            if let Ok(mut clients) = self.partition_clients.lock() {
                clients.remove(&partition);
            }
            return Err(anyhow::anyhow!(
                "failed to publish message to kafka topic {}: {error}",
                self.config.topic
            ));
        }
        Ok(())
    }
}

impl AsyncMessageProducer for KafkaProducer {
    fn publish<'a>(&'a self, message: ProducerMessage) -> ProducerFuture<'a> {
        Box::pin(self.send(message))
    }

    fn close(&self) -> anyhow::Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        if let Ok(mut clients) = self.partition_clients.lock() {
            clients.clear();
        }
        Ok(())
    }
}

/// Queue + producer factory for queue type `kafka`.
#[derive(Default)]
pub struct KafkaTransport {
    configs: Mutex<HashMap<String, Arc<KafkaQueueConfig>>>,
}

impl AsyncWorkflowQueueProviderFactory for KafkaTransport {
    fn create(
        &self,
        queue_type: &str,
        config: Option<&serde_yaml::Value>,
    ) -> anyhow::Result<Arc<dyn AsyncWorkflowQueue>> {
        if queue_type != KAFKA_QUEUE_TYPE {
            anyhow::bail!("unknown async workflow queue type {queue_type:?}");
        }
        let cfg = KafkaQueueConfig::from_yaml(config)?;
        let id = cfg.id();
        self.configs
            .lock()
            .map_err(|_| anyhow::anyhow!("kafka config registry poisoned"))?
            .insert(id.clone(), Arc::new(cfg));
        Ok(Arc::new(KafkaQueue { id }))
    }
}

impl AsyncProducerFactory for KafkaTransport {
    fn create(
        &self,
        queue: &Arc<dyn AsyncWorkflowQueue>,
    ) -> anyhow::Result<Arc<dyn AsyncMessageProducer>> {
        let cfg = self
            .configs
            .lock()
            .map_err(|_| anyhow::anyhow!("kafka config registry poisoned"))?
            .get(queue.id())
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown kafka queue {}", queue.id()))?;
        // Go newSaramaConfigWithAuth validates TLS/SASL at producer creation.
        crate::async_kafka_tls::client_config(&cfg.connection.tls)?;
        sasl_config(&cfg.connection.sasl)?;
        Ok(Arc::new(KafkaProducer::new(cfg)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn yaml(s: &str) -> serde_yaml::Value {
        serde_yaml::from_str(s).unwrap()
    }

    #[test]
    fn config_decodes_sorts_brokers_and_builds_go_id() {
        let cfg = KafkaQueueConfig::from_yaml(Some(&yaml(
            "topic: async\nconnection:\n  brokers: [b:9092, a:9092]\n  sasl: {enabled: true, user: u, password: p, algorithm: sha256}\n  tls: {enabled: false}\n",
        )))
        .unwrap();
        assert_eq!(cfg.id(), "kafka::async/a:9092,b:9092");
        assert!(matches!(
            sasl_config(&cfg.connection.sasl).unwrap(),
            Some(SaslConfig::ScramSha256(_))
        ));
        assert!(KafkaQueueConfig::from_yaml(None).is_err());
    }

    #[test]
    fn sasl_algorithms_match_go() {
        let mk = |a: &str| SaslSettings {
            enabled: true,
            algorithm: a.into(),
            ..Default::default()
        };
        assert!(matches!(
            sasl_config(&mk("plain")).unwrap(),
            Some(SaslConfig::Plain(_))
        ));
        assert!(matches!(
            sasl_config(&mk("sha512")).unwrap(),
            Some(SaslConfig::ScramSha512(_))
        ));
        assert!(sasl_config(&mk("md5")).is_err());
        assert!(sasl_config(&SaslSettings::default()).unwrap().is_none());
    }

    #[test]
    fn partitioner_matches_sarama_hash() {
        // FNV-1a 32 of "a" is 0xe40c292c (positive as int32 -> 3826002220 is
        // negative as int32, so it is negated before the modulo).
        let h = 0xe40c_292cu32 as i32;
        assert_eq!(sarama_partition(b"a", 7), h.wrapping_neg() % 7);
        assert_eq!(
            sarama_partition(b"", 5),
            (0x811c_9dc5u32 as i32).wrapping_neg() % 5
        );
        assert_eq!(sarama_partition(b"x", 1), 0);
        for k in ["order-1", "order-2", "wf"] {
            let p = sarama_partition(k.as_bytes(), 10);
            assert!((0..10).contains(&p));
            assert_eq!(p, sarama_partition(k.as_bytes(), 10));
        }
    }

    #[test]
    fn record_has_key_and_versioned_thrift_value() {
        let msg = ProducerMessage {
            partition_key: "wf-1".into(),
            request_type: 1,
            headers: HashMap::from([("h".to_owned(), b"v".to_vec())]),
            encoding: "thriftrw".into(),
            payload: vec![0x59, 0x00],
        };
        let rec = build_record(&msg);
        assert_eq!(rec.key.as_deref(), Some(&b"wf-1"[..]));
        assert!(rec.headers.is_empty());
        let value = rec.value.unwrap();
        assert_eq!(value[0], 0x59);
        let mut reader = cadence_thrift::Reader::new(&value[1..]);
        let parsed = cadence_thrift::value::read_struct(&mut reader).unwrap();
        let get = |id: i16| parsed.fields.iter().find(|(i, _)| *i == id).map(|(_, v)| v);
        assert_eq!(get(10), Some(&Value::Bytes(b"wf-1".to_vec())));
        assert_eq!(get(12), Some(&Value::I32(1)));
        assert_eq!(get(16), Some(&Value::Bytes(b"thriftrw".to_vec())));
        assert_eq!(get(18), Some(&Value::Bytes(vec![0x59, 0x00])));
        match get(14) {
            Some(Value::Struct(h)) => match &h.fields[0].1 {
                Value::Map(items) => assert_eq!(items.len(), 1),
                other => panic!("header fields not a map: {other:?}"),
            },
            other => panic!("header missing: {other:?}"),
        }
    }

    #[test]
    fn transport_rejects_unknown_type_and_creates_producer_for_kafka() {
        let t = KafkaTransport::default();
        assert!(AsyncWorkflowQueueProviderFactory::create(&t, "sqs", None).is_err());
        let v = yaml("topic: t\nconnection: {brokers: ['127.0.0.1:1']}\n");
        let q = AsyncWorkflowQueueProviderFactory::create(&t, "kafka", Some(&v)).unwrap();
        assert_eq!(q.id(), "kafka::t/127.0.0.1:1");
        let p = AsyncProducerFactory::create(&t, &q).unwrap();
        p.close().unwrap();
    }

    #[tokio::test]
    async fn publish_to_unreachable_broker_is_an_error_not_success() {
        let t = KafkaTransport::default();
        let v = yaml("topic: t\nconnection: {brokers: ['127.0.0.1:1']}\n");
        let q = AsyncWorkflowQueueProviderFactory::create(&t, "kafka", Some(&v)).unwrap();
        let p = AsyncProducerFactory::create(&t, &q).unwrap();
        let res = p
            .publish(ProducerMessage {
                partition_key: "k".into(),
                request_type: 0,
                headers: HashMap::new(),
                encoding: "thriftrw".into(),
                payload: vec![],
            })
            .await;
        assert!(res.is_err());
        p.close().unwrap();
        assert!(p
            .publish(ProducerMessage {
                partition_key: "k".into(),
                request_type: 0,
                headers: HashMap::new(),
                encoding: "thriftrw".into(),
                payload: vec![],
            })
            .await
            .is_err());
    }
}
