//! Go `common/persistence/client/factory.go` `NewVisibilityManager` (write side): wraps the DB
//! visibility writer and, when `persistence.advancedVisibilityStore` is set, the Kafka-backed
//! ES/OpenSearch writer into a `visibilityHybridManager`.
use std::collections::HashMap;
use std::sync::Arc;

use cadence_service_core::config::KafkaConfig;

use crate::wiring::persistence::es_visibility::{EsVisibilityWriter, HybridVisibilityWriter, KafkaProducer};
use crate::wiring::persistence::visibility::VisibilityWriter;

/// Go `constants.VisibilityAppName`.
const VISIBILITY_APP_NAME: &str = "visibility";

pub async fn build_visibility_writer(
    db: Arc<dyn VisibilityWriter>,
    advanced_visibility_store: &str,
    kafka: &KafkaConfig,
    write_mode: Arc<dyn Fn() -> String + Send + Sync>,
) -> anyhow::Result<Arc<dyn VisibilityWriter>> {
    let mut mgrs: HashMap<String, Arc<dyn VisibilityWriter>> = HashMap::new();
    mgrs.insert("db".to_string(), db);
    let mode = match advanced_visibility_store {
        "" => None,
        "es-visibility" => Some("es"),
        "os-visibility" => Some("os"),
        other => anyhow::bail!("unsupported advanced visibility store {other:?} for Rust History"),
    };
    if let Some(mode) = mode {
        let (topic, brokers) = kafka.producer_target(VISIBILITY_APP_NAME).map_err(|e| anyhow::anyhow!(e))?;
        let tls = crate::wiring::persistence::kafka_auth::client_config(&kafka.tls)?;
        let sasl = crate::wiring::persistence::kafka_auth::sasl_config(&kafka.sasl)?;
        let producer = Arc::new(KafkaProducer::with_auth(brokers, topic, tls, sasl));
        // Go `NewSyncProducer` fails service start when the brokers/topic are unreachable.
        producer.connect_eager().await?;
        mgrs.insert(mode.to_string(), Arc::new(EsVisibilityWriter::new(producer)));
    }
    Ok(Arc::new(HybridVisibilityWriter::new(mgrs, write_mode)))
}
