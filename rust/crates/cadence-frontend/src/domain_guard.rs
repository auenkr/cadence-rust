//! Go `WorkflowHandler` request validation and graceful-failover guard for the
//! domain RPCs and CreateSemaphore that the transport layers serve directly.
//!
//! Go order: `requestValidator.Validate*` first, then (graceful failover only)
//! `checkOngoingFailover`, then the domain handler.

use crate::request_validator::RequestValidator;
use cadence_api::service_types::{
    DeleteDomainRequest, DeprecateDomainRequest, DescribeDomainRequest, DescribeDomainResponse,
    UpdateDomainRequest,
};
use cadence_api::types::{CreateSemaphoreRequest, FailoverDomainRequest, RegisterDomainRequest};
use cadence_service_core::matching_metrics::{MetricEvent, MetricKind, MetricSink, MetricValue};
use cadence_service_core::workflow_api::{DomainGuardError, DomainValidator, GuardFuture};
use futures_core_shim::BoxFuture;
use std::collections::BTreeMap;
use std::sync::Arc;

mod futures_core_shim {
    use std::future::Future;
    use std::pin::Pin;
    pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;
}

/// Source of per-cluster `DescribeDomain` failover versions (testable seam).
pub trait ClusterDescribe: Send + Sync {
    fn enabled_clusters(&self) -> Vec<String>;
    /// `Err` is a client lookup failure (Go `GetRemoteFrontendClient` error).
    /// The future yields `None` when the DescribeDomain call failed.
    fn describe(&self, cluster: &str, domain: &str) -> Result<BoxFuture<Option<i64>>, String>;
}

/// Go `checkOngoingFailover`.
pub async fn check_ongoing_failover(
    source: &dyn ClusterDescribe,
    domain: &str,
) -> Result<(), DomainGuardError> {
    let mut handles = Vec::new();
    for cluster in source.enabled_clusters() {
        let future = source
            .describe(&cluster, domain)
            .map_err(DomainGuardError::BadRequest)?;
        handles.push(tokio::spawn(future));
    }
    let mut versions = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(Some(version)) => versions.push(version),
            _ => {
                return Err(DomainGuardError::Internal(
                    "Failed to verify failover version from all clusters".into(),
                ))
            }
        }
    }
    if versions.windows(2).any(|w| w[0] != w[1]) {
        return Err(DomainGuardError::BadRequest(
            "Concurrent failover is not allowed".into(),
        ));
    }
    Ok(())
}

/// Live source: local cluster via the domain manager, others via remote
/// frontend clients.
pub struct LiveClusters {
    pub current: String,
    pub enabled: Vec<String>,
    pub manager: Arc<dyn cadence_persistence::FrontendDomainManager>,
    pub remotes: crate::wrappers::redirection_runtime::RemoteFrontends,
}

impl ClusterDescribe for LiveClusters {
    fn enabled_clusters(&self) -> Vec<String> {
        self.enabled.clone()
    }

    fn describe(&self, cluster: &str, domain: &str) -> Result<BoxFuture<Option<i64>>, String> {
        let request = DescribeDomainRequest {
            name: Some(domain.to_owned()),
            uuid: None,
        };
        if cluster == self.current {
            let manager = self.manager.clone();
            return Ok(Box::pin(async move {
                manager.describe(&request).ok().map(|r| r.failover_version)
            }));
        }
        let client = self
            .remotes
            .get(cluster)
            .map_err(|e| format!("{e}"))?
            .clone();
        Ok(Box::pin(async move {
            let value = serde_json::to_value(&request).ok()?;
            let response = client
                .call(
                    "DescribeDomain",
                    value,
                    cadence_service_core::frontend_remote::DEFAULT_REMOTE_TIMEOUT,
                )
                .await
                .ok()?;
            serde_json::from_value::<DescribeDomainResponse>(response)
                .ok()
                .map(|r| r.failover_version)
        }))
    }
}

pub struct DomainGuard {
    pub validator: RequestValidator,
    pub clusters: Arc<dyn ClusterDescribe>,
    pub metrics: Arc<dyn MetricSink>,
}

fn decode<T: serde::de::DeserializeOwned>(
    request: &serde_json::Value,
) -> Result<T, DomainGuardError> {
    serde_json::from_value(request.clone())
        .map_err(|e| DomainGuardError::BadRequest(format!("invalid request: {e}")))
}

fn bad(e: cadence_api::errors::BadRequestError) -> DomainGuardError {
    DomainGuardError::BadRequest(e.message)
}

impl DomainValidator for DomainGuard {
    fn validate(
        &self,
        operation: &str,
        request: &serde_json::Value,
    ) -> Result<(), DomainGuardError> {
        let v = &self.validator;
        match operation {
            "RegisterDomain" => v
                .validate_register_domain_request(Some(&decode::<RegisterDomainRequest>(request)?))
                .map_err(bad),
            "UpdateDomain" => v
                .validate_update_domain_request(Some(&decode::<UpdateDomainRequest>(request)?))
                .map_err(bad),
            "DeleteDomain" => v
                .validate_delete_domain_request(Some(&decode::<DeleteDomainRequest>(request)?))
                .map_err(bad),
            "DeprecateDomain" => v
                .validate_deprecate_domain_request(Some(&decode::<DeprecateDomainRequest>(
                    request,
                )?))
                .map_err(bad),
            "FailoverDomain" => v
                .validate_failover_domain_request(Some(&decode::<FailoverDomainRequest>(request)?))
                .map_err(bad),
            "CreateSemaphore" => v
                .validate_create_semaphore_request(&decode::<CreateSemaphoreRequest>(request)?)
                .map_err(bad),
            _ => Ok(()),
        }
    }

    fn check_ongoing_failover<'a>(&'a self, domain: &'a str) -> GuardFuture<'a> {
        Box::pin(check_ongoing_failover(self.clusters.as_ref(), domain))
    }

    fn graceful_failover_metric(&self, operation: &str, domain: &str, reason: Option<&str>) {
        let mut tags = BTreeMap::new();
        tags.insert("operation".to_owned(), operation.to_owned());
        tags.insert("domain".to_owned(), domain.to_owned());
        let name = if let Some(reason) = reason {
            tags.insert("reason".to_owned(), reason.to_owned());
            "graceful_failover_initiation_failures"
        } else {
            "graceful_failover_initiation_success"
        };
        self.metrics.record(MetricEvent {
            name,
            kind: MetricKind::Counter,
            value: MetricValue::Count(1),
            tags,
            histogram: None,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct Fake {
        results: Vec<Result<Option<i64>, String>>,
    }
    impl ClusterDescribe for Fake {
        fn enabled_clusters(&self) -> Vec<String> {
            (0..self.results.len()).map(|i| format!("c{i}")).collect()
        }
        fn describe(&self, cluster: &str, _: &str) -> Result<BoxFuture<Option<i64>>, String> {
            let i: usize = cluster[1..].parse().unwrap();
            match self.results[i].clone() {
                Ok(v) => Ok(Box::pin(async move { v })),
                Err(e) => Err(e),
            }
        }
    }

    async fn run(results: Vec<Result<Option<i64>, String>>) -> Result<(), DomainGuardError> {
        check_ongoing_failover(&Fake { results }, "d").await
    }

    #[tokio::test]
    async fn ongoing_failover_matches_go() {
        assert!(run(vec![Ok(Some(1)), Ok(Some(1))]).await.is_ok());
        assert!(matches!(
            run(vec![Ok(Some(1)), Ok(Some(2))]).await,
            Err(DomainGuardError::BadRequest(m)) if m == "Concurrent failover is not allowed"
        ));
        assert!(matches!(
            run(vec![Ok(Some(1)), Ok(None)]).await,
            Err(DomainGuardError::Internal(m)) if m == "Failed to verify failover version from all clusters"
        ));
        assert!(matches!(
            run(vec![Ok(Some(1)), Err("no client".into())]).await,
            Err(DomainGuardError::BadRequest(m)) if m == "no client"
        ));
    }

    #[derive(Default)]
    struct Sink(Mutex<Vec<MetricEvent>>);
    impl MetricSink for Sink {
        fn record(&self, event: MetricEvent) {
            self.0.lock().unwrap().push(event);
        }
    }

    fn guard(sink: Arc<Sink>) -> DomainGuard {
        DomainGuard {
            validator: RequestValidator::default(),
            clusters: Arc::new(Fake { results: vec![] }),
            metrics: sink,
        }
    }

    #[test]
    fn validate_uses_go_validator_per_operation() {
        let g = guard(Arc::new(Sink::default()));
        let register = serde_json::to_value(RegisterDomainRequest {
            name: "d".into(),
            workflow_execution_retention_period_in_days: 100_000,
            ..Default::default()
        })
        .unwrap();
        assert!(g.validate("RegisterDomain", &register).is_err());
        let delete = serde_json::to_value(DeleteDomainRequest::default()).unwrap();
        assert!(matches!(
            g.validate("DeleteDomain", &delete),
            Err(DomainGuardError::BadRequest(_))
        ));
        let sem = serde_json::to_value(CreateSemaphoreRequest {
            domain: "d".into(),
            semaphore_name: "s".into(),
            capacity: 0,
            bucket_capacity: 0,
        })
        .unwrap();
        assert!(matches!(
            g.validate("CreateSemaphore", &sem),
            Err(DomainGuardError::BadRequest(m)) if m == "Capacity must be positive, got 0."
        ));
    }

    #[test]
    fn graceful_metrics_are_tagged() {
        let sink = Arc::new(Sink::default());
        let g = guard(sink.clone());
        g.graceful_failover_metric("UpdateDomain", "d", Some("validation"));
        g.graceful_failover_metric("UpdateDomain", "d", None);
        let events = sink.0.lock().unwrap();
        assert_eq!(events[0].name, "graceful_failover_initiation_failures");
        assert_eq!(events[0].tags["reason"], "validation");
        assert_eq!(events[1].name, "graceful_failover_initiation_success");
    }
}
