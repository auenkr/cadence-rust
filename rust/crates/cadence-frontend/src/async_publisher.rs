//! Production `AsyncWorkflowPublisher` (Go `api/producer_manager.go` plus the
//! async branches of `handler.go`). Queue transport is abstracted behind
//! `AsyncMessageProducer`; the Kafka client is a later slice.
#![allow(clippy::result_large_err)]

use crate::async_queue::{
    resolve_domain_queue, AsyncWorkflowQueue, AsyncWorkflowQueueProvider,
    AsyncWorkflowQueueProviderFactory, PRODUCER_CACHE_MAX_COUNT, PRODUCER_CACHE_TTL_SECS,
};
use crate::handler_workflow::{AsyncWorkflowPublisher, WorkflowFuture};
use crate::handler_workflow_async::AsyncWorkflowMessage;
use cadence_api::errors::{CadenceError, InternalServiceError};
use cadence_api::types::{AsyncWorkflowConfiguration, DataBlob};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Queue message: partition key, type (0 start / 1 signal-with-start), the
/// original RPC headers and the versioned ThriftRW payload. Domain is not a
/// message field in Go.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProducerMessage {
    pub partition_key: String,
    pub request_type: i32,
    pub headers: HashMap<String, Vec<u8>>,
    pub encoding: String,
    pub payload: Vec<u8>,
}

pub type ProducerFuture<'a> = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>>;

/// Synchronous-ack producer: resolves only after the broker acknowledged.
pub trait AsyncMessageProducer: Send + Sync {
    fn publish<'a>(&'a self, message: ProducerMessage) -> ProducerFuture<'a>;
    fn close(&self) -> anyhow::Result<()>;
}

pub trait AsyncProducerFactory: Send + Sync {
    fn create(
        &self,
        queue: &Arc<dyn AsyncWorkflowQueue>,
    ) -> anyhow::Result<Arc<dyn AsyncMessageProducer>>;
}

/// Domain-level lookup of the persisted async workflow configuration.
pub trait AsyncDomainConfigLookup: Send + Sync {
    fn async_config(&self, domain: &str) -> Result<AsyncWorkflowConfiguration, CadenceError>;
}

pub struct PersistenceAsyncDomainConfig {
    manager: Arc<dyn cadence_persistence::FrontendDomainManager>,
}

impl PersistenceAsyncDomainConfig {
    pub fn new(manager: Arc<dyn cadence_persistence::FrontendDomainManager>) -> Self {
        Self { manager }
    }
}

impl AsyncDomainConfigLookup for PersistenceAsyncDomainConfig {
    fn async_config(&self, domain: &str) -> Result<AsyncWorkflowConfiguration, CadenceError> {
        let described =
            self.manager
                .describe(&cadence_api::service_types::DescribeDomainRequest {
                    name: Some(domain.to_owned()),
                    ..Default::default()
                })?;
        Ok(described
            .configuration
            .and_then(|c| c.async_workflow_config)
            .unwrap_or_default())
    }
}

struct Entry {
    created: Instant,
    producer: Arc<dyn AsyncMessageProducer>,
}

/// Bounded producer cache keyed by queue ID (Go: TTL 5 min, max 100).
pub struct ProducerCache {
    ttl: Duration,
    max: usize,
    entries: Mutex<HashMap<String, Entry>>,
}

impl ProducerCache {
    pub fn new(ttl: Duration, max: usize) -> Self {
        Self {
            ttl,
            max: max.max(1),
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub fn go_defaults() -> Self {
        Self::new(
            Duration::from_secs(PRODUCER_CACHE_TTL_SECS),
            PRODUCER_CACHE_MAX_COUNT,
        )
    }

    pub fn len(&self) -> usize {
        self.entries.lock().map_or(0, |e| e.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn get_or_create(
        &self,
        id: &str,
        create: impl FnOnce() -> anyhow::Result<Arc<dyn AsyncMessageProducer>>,
    ) -> anyhow::Result<Arc<dyn AsyncMessageProducer>> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| anyhow::anyhow!("producer cache poisoned"))?;
        let now = Instant::now();
        let expired: Vec<String> = entries
            .iter()
            .filter(|(_, e)| now.duration_since(e.created) >= self.ttl)
            .map(|(k, _)| k.clone())
            .collect();
        for key in expired {
            if let Some(e) = entries.remove(&key) {
                let _ = e.producer.close();
            }
        }
        if let Some(e) = entries.get(id) {
            return Ok(e.producer.clone());
        }
        let producer = create()?;
        if entries.len() >= self.max {
            let oldest = entries
                .iter()
                .min_by_key(|(_, e)| e.created)
                .map(|(k, _)| k.clone());
            if let Some(e) = oldest.and_then(|k| entries.remove(&k)) {
                let _ = e.producer.close();
            }
        }
        entries.insert(
            id.to_owned(),
            Entry {
                created: now,
                producer: producer.clone(),
            },
        );
        Ok(producer)
    }

    pub fn close(&self) {
        if let Ok(mut entries) = self.entries.lock() {
            for (_, e) in entries.drain() {
                let _ = e.producer.close();
            }
        }
    }
}

pub struct QueueAsyncPublisher {
    domains: Arc<dyn AsyncDomainConfigLookup>,
    provider: Arc<dyn AsyncWorkflowQueueProvider>,
    producers: Arc<dyn AsyncProducerFactory>,
    cache: ProducerCache,
}

impl QueueAsyncPublisher {
    pub fn new(
        domains: Arc<dyn AsyncDomainConfigLookup>,
        provider: Arc<dyn AsyncWorkflowQueueProvider>,
        producers: Arc<dyn AsyncProducerFactory>,
        cache: ProducerCache,
    ) -> Self {
        Self {
            domains,
            provider,
            producers,
            cache,
        }
    }

    pub fn close(&self) {
        self.cache.close();
        let _ = self.provider.close();
    }

    fn internal(error: anyhow::Error) -> CadenceError {
        CadenceError::Internal(InternalServiceError::new(error.to_string()))
    }
}

impl AsyncWorkflowPublisher for QueueAsyncPublisher {
    fn publish<'a>(&'a self, message: AsyncWorkflowMessage) -> WorkflowFuture<'a> {
        Box::pin(async move {
            let config = self.domains.async_config(&message.domain)?;
            let queue = resolve_domain_queue(self.provider.as_ref(), &message.domain, &config)?;
            let producer = self
                .cache
                .get_or_create(queue.id(), || self.producers.create(&queue))
                .map_err(Self::internal)?;
            producer
                .publish(ProducerMessage {
                    partition_key: message.partition_key,
                    request_type: message.request_type,
                    headers: message.headers,
                    encoding: message.encoding.to_owned(),
                    payload: message.payload,
                })
                .await
                .map_err(Self::internal)?;
            Ok(serde_json::json!({}))
        })
    }
}

/// Provider over the server's `asyncWorkflowQueues` map. Concrete queue
/// construction is delegated to `factory` (Kafka arrives in a later slice).
pub struct ConfiguredQueueProvider {
    providers: HashMap<String, cadence_service_core::config::AsyncWorkflowQueueProviderConfig>,
    factory: Arc<dyn AsyncWorkflowQueueProviderFactory>,
}

impl ConfiguredQueueProvider {
    pub fn new(
        providers: HashMap<String, cadence_service_core::config::AsyncWorkflowQueueProviderConfig>,
        factory: Arc<dyn AsyncWorkflowQueueProviderFactory>,
    ) -> Self {
        Self { providers, factory }
    }
}

impl AsyncWorkflowQueueProvider for ConfiguredQueueProvider {
    fn get_predefined_queue(&self, name: &str) -> anyhow::Result<Arc<dyn AsyncWorkflowQueue>> {
        let cfg = self
            .providers
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("no predefined async queue named {name}"))?;
        self.factory.create(&cfg.queue_type, cfg.config.as_ref())
    }

    fn get_queue(
        &self,
        queue_type: &str,
        queue_config: Option<&DataBlob>,
    ) -> anyhow::Result<Arc<dyn AsyncWorkflowQueue>> {
        let value = match queue_config {
            Some(blob) if !blob.data.is_empty() => Some(serde_yaml::from_slice(&blob.data)?),
            _ => None,
        };
        self.factory.create(queue_type, value.as_ref())
    }

    fn close(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Until the Kafka client slice lands, queue types have no transport.
pub struct UnsupportedTransport;

impl AsyncWorkflowQueueProviderFactory for UnsupportedTransport {
    fn create(
        &self,
        queue_type: &str,
        _config: Option<&serde_yaml::Value>,
    ) -> anyhow::Result<Arc<dyn AsyncWorkflowQueue>> {
        anyhow::bail!("async workflow queue type {queue_type:?} has no Rust transport yet")
    }
}

impl AsyncProducerFactory for UnsupportedTransport {
    fn create(
        &self,
        queue: &Arc<dyn AsyncWorkflowQueue>,
    ) -> anyhow::Result<Arc<dyn AsyncMessageProducer>> {
        anyhow::bail!("no Rust producer transport for async queue {}", queue.id())
    }
}

/// Builds the production publisher from the server config.
pub fn production_publisher(
    domains: Arc<dyn AsyncDomainConfigLookup>,
    queues: HashMap<String, cadence_service_core::config::AsyncWorkflowQueueProviderConfig>,
) -> Arc<dyn AsyncWorkflowPublisher> {
    Arc::new(QueueAsyncPublisher::new(
        domains,
        Arc::new(ConfiguredQueueProvider::new(
            queues,
            Arc::new(UnsupportedTransport),
        )),
        Arc::new(UnsupportedTransport),
        ProducerCache::go_defaults(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Domains(AsyncWorkflowConfiguration);
    impl AsyncDomainConfigLookup for Domains {
        fn async_config(&self, _: &str) -> Result<AsyncWorkflowConfiguration, CadenceError> {
            Ok(self.0.clone())
        }
    }

    struct Q(String);
    impl AsyncWorkflowQueue for Q {
        fn id(&self) -> &str {
            &self.0
        }
    }
    struct Provider;
    impl AsyncWorkflowQueueProvider for Provider {
        fn get_predefined_queue(&self, n: &str) -> anyhow::Result<Arc<dyn AsyncWorkflowQueue>> {
            Ok(Arc::new(Q(format!("p:{n}"))))
        }
        fn get_queue(
            &self,
            t: &str,
            _: Option<&DataBlob>,
        ) -> anyhow::Result<Arc<dyn AsyncWorkflowQueue>> {
            Ok(Arc::new(Q(format!("i:{t}"))))
        }
        fn close(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct Producer {
        sent: Mutex<Vec<ProducerMessage>>,
        fail: bool,
        closed: AtomicUsize,
    }
    impl AsyncMessageProducer for Producer {
        fn publish<'a>(&'a self, m: ProducerMessage) -> ProducerFuture<'a> {
            Box::pin(async move {
                if self.fail {
                    anyhow::bail!("broker down");
                }
                self.sent.lock().unwrap().push(m);
                Ok(())
            })
        }
        fn close(&self) -> anyhow::Result<()> {
            self.closed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct Factory {
        producer: Arc<Producer>,
        created: Mutex<Vec<String>>,
    }
    impl AsyncProducerFactory for Factory {
        fn create(
            &self,
            q: &Arc<dyn AsyncWorkflowQueue>,
        ) -> anyhow::Result<Arc<dyn AsyncMessageProducer>> {
            self.created.lock().unwrap().push(q.id().to_owned());
            Ok(self.producer.clone())
        }
    }

    fn enabled(queue: &str) -> AsyncWorkflowConfiguration {
        AsyncWorkflowConfiguration {
            enabled: true,
            predefined_queue_name: queue.into(),
            ..Default::default()
        }
    }

    fn msg(kind: i32) -> AsyncWorkflowMessage {
        AsyncWorkflowMessage {
            domain: "demo".into(),
            partition_key: "wf-1".into(),
            request_type: kind,
            headers: HashMap::from([("k".to_owned(), b"v".to_vec())]),
            encoding: "thriftrw",
            payload: vec![0x59, 0x00],
        }
    }

    fn publisher(
        cfg: AsyncWorkflowConfiguration,
        producer: Arc<Producer>,
    ) -> (QueueAsyncPublisher, Arc<Factory>) {
        let factory = Arc::new(Factory {
            producer,
            created: Mutex::default(),
        });
        (
            QueueAsyncPublisher::new(
                Arc::new(Domains(cfg)),
                Arc::new(Provider),
                factory.clone(),
                ProducerCache::go_defaults(),
            ),
            factory,
        )
    }

    #[tokio::test]
    async fn publishes_exact_envelope_for_both_types() {
        let producer = Arc::new(Producer::default());
        let (p, factory) = publisher(enabled("fast"), producer.clone());
        for kind in [0, 1] {
            assert_eq!(p.publish(msg(kind)).await.unwrap(), serde_json::json!({}));
        }
        let sent = producer.sent.lock().unwrap();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0].request_type, 0);
        assert_eq!(sent[1].request_type, 1);
        assert_eq!(sent[1].partition_key, "wf-1");
        assert_eq!(sent[1].headers["k"], b"v");
        assert_eq!(sent[1].encoding, "thriftrw");
        assert_eq!(sent[1].payload, [0x59, 0x00]);
        // Producer cached by queue ID: created once.
        assert_eq!(*factory.created.lock().unwrap(), ["p:fast"]);
    }

    #[tokio::test]
    async fn disabled_domain_is_bad_request_without_producer() {
        let producer = Arc::new(Producer::default());
        let (p, factory) = publisher(AsyncWorkflowConfiguration::default(), producer.clone());
        let err = p.publish(msg(0)).await.unwrap_err();
        assert!(matches!(err, CadenceError::BadRequest(ref e)
                if e.to_string() == "async workflow is not enabled for domain demo"));
        assert!(factory.created.lock().unwrap().is_empty());
        assert!(producer.sent.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn publish_failure_passes_through() {
        let producer = Arc::new(Producer {
            fail: true,
            ..Default::default()
        });
        let (p, _) = publisher(enabled("fast"), producer.clone());
        let err = p.publish(msg(0)).await.unwrap_err();
        assert!(matches!(err, CadenceError::Internal(ref e) if e.to_string() == "broker down"));
        assert!(producer.sent.lock().unwrap().is_empty());
    }

    #[test]
    fn cache_evicts_oldest_at_max_and_closes_it() {
        let cache = ProducerCache::new(Duration::from_secs(60), 2);
        let made: Vec<Arc<Producer>> = (0..3).map(|_| Arc::new(Producer::default())).collect();
        for (i, p) in made.iter().enumerate() {
            let p = p.clone();
            cache
                .get_or_create(&format!("q{i}"), move || Ok(p))
                .unwrap();
            std::thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(cache.len(), 2);
        assert_eq!(made[0].closed.load(Ordering::SeqCst), 1);
        assert_eq!(made[2].closed.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn cache_entry_expires_after_ttl() {
        let cache = ProducerCache::new(Duration::from_millis(1), 10);
        let first = Arc::new(Producer::default());
        let f = first.clone();
        cache.get_or_create("q", move || Ok(f)).unwrap();
        std::thread::sleep(Duration::from_millis(5));
        let second = Arc::new(Producer::default());
        let s = second.clone();
        cache.get_or_create("q", move || Ok(s)).unwrap();
        assert_eq!(first.closed.load(Ordering::SeqCst), 1);
        assert_eq!(cache.len(), 1);
    }

    #[tokio::test]
    async fn production_publisher_reports_missing_transport() {
        let p = production_publisher(Arc::new(Domains(enabled("fast"))), HashMap::new());
        let err = p.publish(msg(0)).await.unwrap_err();
        assert!(matches!(err, CadenceError::Internal(_)));
    }
}
