//! Async workflow queue selection and provider seams.
//!
//! Go resolves a domain's persisted async config on each request, preferring
//! `predefinedQueueName` over the inline `(queueType, queueConfig)` pair. The
//! transport implementation is deliberately supplied by a provider.

use cadence_api::errors::{BadRequestError, CadenceError, InternalServiceError};
use cadence_api::types::{AsyncWorkflowConfiguration, DataBlob};
use std::sync::Arc;

pub const PRODUCER_CACHE_TTL_SECS: u64 = 5 * 60;
pub const PRODUCER_CACHE_MAX_COUNT: usize = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueSelection {
    Predefined { name: String },
    Inline { queue_type: String, queue_config: Option<DataBlob> },
}

/// Select a queue only after the caller's ordinary request validation, as Go
/// does in ProducerManager after its workflow handler validation.
pub fn select_domain_queue(
    domain: &str,
    config: &AsyncWorkflowConfiguration,
) -> Result<QueueSelection, CadenceError> {
    if !config.enabled {
        return Err(CadenceError::BadRequest(BadRequestError::new(format!(
            "async workflow is not enabled for domain {domain}"
        ))));
    }
    if !config.predefined_queue_name.is_empty() {
        return Ok(QueueSelection::Predefined {
            name: config.predefined_queue_name.clone(),
        });
    }
    Ok(QueueSelection::Inline {
        queue_type: config.queue_type.clone(),
        queue_config: config.queue_config.clone(),
    })
}

pub trait AsyncWorkflowQueue: Send + Sync {
    /// Go queue.ID(), used as the producer cache key.
    fn id(&self) -> &str;
}

/// Implementations own provider-specific queue construction and resources.
/// `close` is called during Frontend shutdown by the eventual publisher owner.
pub trait AsyncWorkflowQueueProvider: Send + Sync {
    fn get_predefined_queue(&self, name: &str) -> anyhow::Result<Arc<dyn AsyncWorkflowQueue>>;
    fn get_queue(
        &self,
        queue_type: &str,
        queue_config: Option<&DataBlob>,
    ) -> anyhow::Result<Arc<dyn AsyncWorkflowQueue>>;
    fn close(&self) -> anyhow::Result<()>;
}

pub trait AsyncWorkflowQueueProviderFactory: Send + Sync {
    fn create(
        &self,
        queue_type: &str,
        config: Option<&serde_yaml::Value>,
    ) -> anyhow::Result<Arc<dyn AsyncWorkflowQueue>>;
}

/// Resolve a selected domain queue through a provider implementation.
pub fn resolve_domain_queue(
    provider: &dyn AsyncWorkflowQueueProvider,
    domain: &str,
    config: &AsyncWorkflowConfiguration,
) -> Result<Arc<dyn AsyncWorkflowQueue>, CadenceError> {
    let selection = select_domain_queue(domain, config)?;
    let queue = match selection {
        QueueSelection::Predefined { name } => provider.get_predefined_queue(&name),
        QueueSelection::Inline {
            queue_type,
            queue_config,
        } => provider.get_queue(&queue_type, queue_config.as_ref()),
    };
    queue.map_err(|error| CadenceError::Internal(InternalServiceError::new(error.to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct QueueProvider {
        selected: Mutex<Vec<String>>,
    }

    struct Queue(String);
    impl AsyncWorkflowQueue for Queue {
        fn id(&self) -> &str { &self.0 }
    }
    impl AsyncWorkflowQueueProvider for QueueProvider {
        fn get_predefined_queue(&self, name: &str) -> anyhow::Result<Arc<dyn AsyncWorkflowQueue>> {
            self.selected.lock().unwrap().push(format!("predefined:{name}"));
            Ok(Arc::new(Queue(format!("predefined:{name}"))))
        }
        fn get_queue(&self, kind: &str, _config: Option<&DataBlob>) -> anyhow::Result<Arc<dyn AsyncWorkflowQueue>> {
            self.selected.lock().unwrap().push(format!("inline:{kind}"));
            Ok(Arc::new(Queue(format!("inline:{kind}"))))
        }
        fn close(&self) -> anyhow::Result<()> { Ok(()) }
    }

    #[test]
    fn disabled_domain_fails_before_queue_selection() {
        let provider = QueueProvider::default();
        let error = match resolve_domain_queue(&provider, "demo", &AsyncWorkflowConfiguration::default()) {
            Ok(_) => panic!("disabled domain unexpectedly resolved a queue"),
            Err(error) => error,
        };
        assert!(matches!(error, CadenceError::BadRequest(_)));
        assert!(provider.selected.lock().unwrap().is_empty());
    }

    #[test]
    fn predefined_queue_takes_precedence_over_inline_fields() {
        let provider = QueueProvider::default();
        let cfg = AsyncWorkflowConfiguration { enabled: true, predefined_queue_name: "fast".into(), queue_type: "ignored".into(), ..Default::default() };
        assert_eq!(resolve_domain_queue(&provider, "demo", &cfg).unwrap().id(), "predefined:fast");
        assert_eq!(*provider.selected.lock().unwrap(), ["predefined:fast"]);
    }

    #[test]
    fn enabled_domain_without_predefined_queue_uses_type_config() {
        let provider = QueueProvider::default();
        let cfg = AsyncWorkflowConfiguration { enabled: true, queue_type: "kafka".into(), ..Default::default() };
        assert_eq!(resolve_domain_queue(&provider, "demo", &cfg).unwrap().id(), "inline:kafka");
        assert_eq!(*provider.selected.lock().unwrap(), ["inline:kafka"]);
    }

    #[test]
    fn predefined_lookup_error_is_propagated() {
        struct Missing;
        impl AsyncWorkflowQueueProvider for Missing {
            fn get_predefined_queue(&self, _: &str) -> anyhow::Result<Arc<dyn AsyncWorkflowQueue>> { anyhow::bail!("queue absent") }
            fn get_queue(&self, _: &str, _: Option<&DataBlob>) -> anyhow::Result<Arc<dyn AsyncWorkflowQueue>> { unreachable!() }
            fn close(&self) -> anyhow::Result<()> { Ok(()) }
        }
        let cfg = AsyncWorkflowConfiguration { enabled: true, predefined_queue_name: "missing".into(), ..Default::default() };
        let error = match resolve_domain_queue(&Missing, "demo", &cfg) {
            Ok(_) => panic!("missing predefined queue unexpectedly resolved"),
            Err(error) => error,
        };
        assert!(matches!(error, CadenceError::Internal(message) if message.to_string() == "queue absent"));
    }
}
