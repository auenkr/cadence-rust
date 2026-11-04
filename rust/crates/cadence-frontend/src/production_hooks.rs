//! Production implementations of the optional `Handler` hooks that Go's
//! `WorkflowHandler` always has: bad-binary admission, isolation-group drain
//! checks / poll wait, partition-config context and zombie-poller cancel.
#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cadence_api::errors::{BadRequestError, CadenceError};
use cadence_api::types::IsolationGroupConfiguration;

use crate::handler_workflow::{
    IsolationGroupState, OutstandingPollCanceller, PollAdmissionPolicy, PollIsolationGroupWaiter,
    WorkflowFuture,
};
use crate::wrappers::metered::{Counter, Metrics};

/// Go `isolationgroup.GroupKey`.
pub const GROUP_KEY: &str = "isolation-group";
/// Go `common.AutoforwardingClusterHeaderName`.
pub const FORWARDING_CLUSTER_HEADER: &str = "cadence-forwarding-cluster";
/// Go `common.PartitionConfigHeaderName`.
pub const PARTITION_CONFIG_HEADER: &str = "cadence-workflow-partition-config";
/// Go `common.IsolationGroupHeaderName`.
pub const WORKER_ISOLATION_GROUP_HEADER: &str = "cadence-worker-isolation-group";
/// Go `common.ClientIsolationGroupHeaderName`.
pub const CLIENT_ISOLATION_GROUP_HEADER: &str = "cadence-client-isolation-group";

/// Go `ClientPartitionConfigMiddleware` followed by
/// `ForwardPartitionConfigMiddleware`: returns `(isolation_group,
/// partition_config)` derived from inbound (lower-cased) headers.
pub fn context_from_headers(
    headers: &HashMap<String, String>,
) -> Result<(String, Option<HashMap<String, String>>), CadenceError> {
    let get = |name: &str| headers.get(name).map(String::as_str).unwrap_or_default();
    let mut group = String::new();
    let mut config = None;
    let zone = get(CLIENT_ISOLATION_GROUP_HEADER);
    if !zone.is_empty() {
        group = zone.to_owned();
        config = Some(HashMap::from([(GROUP_KEY.to_owned(), zone.to_owned())]));
    }
    if headers.contains_key(FORWARDING_CLUSTER_HEADER) {
        // The forwarded request overwrites whatever the client header set.
        let blob = get(PARTITION_CONFIG_HEADER);
        config = if blob.is_empty() {
            None
        } else {
            Some(
                serde_json::from_str::<Option<HashMap<String, String>>>(blob)
                    .map_err(|e| CadenceError::BadRequest(BadRequestError::new(e.to_string())))?
                    .unwrap_or_default(),
            )
        };
        group = get(WORKER_ISOLATION_GROUP_HEADER).to_owned();
    }
    Ok((group, config))
}

type DomainConfigLookup = Arc<dyn Fn(&str) -> Result<Option<IsolationGroupConfiguration>, String> + Send + Sync>;
type GlobalConfigLookup = Arc<dyn Fn() -> Option<IsolationGroupConfiguration> + Send + Sync>;
type EnabledLookup = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// Go `defaultisolationgroupstate`: a group is drained when either the global
/// or the domain configuration marks it drained. The enable flag is
/// `system.enableTasklistIsolation` (domain filtered).
#[derive(Clone)]
pub struct DomainIsolationState {
    enabled: EnabledLookup,
    domain: DomainConfigLookup,
    global: GlobalConfigLookup,
    tick: Duration,
}

impl DomainIsolationState {
    pub fn new(enabled: EnabledLookup, domain: DomainConfigLookup, global: GlobalConfigLookup) -> Self {
        Self {
            enabled,
            domain,
            global,
            tick: Duration::from_secs(30),
        }
    }

    pub fn with_tick(mut self, tick: Duration) -> Self {
        self.tick = tick;
        self
    }

    fn drained(config: &Option<IsolationGroupConfiguration>, group: &str) -> bool {
        config.iter().flat_map(|c| &c.isolation_groups).any(|p| {
            p.name == group && p.state == Some(cadence_api::enums::IsolationGroupState::Drained)
        })
    }
}

impl IsolationGroupState for DomainIsolationState {
    fn is_drained(&self, domain: &str, isolation_group: &str) -> Result<bool, String> {
        if !(self.enabled)(domain) {
            return Ok(false);
        }
        let domain_cfg = (self.domain)(domain)?;
        Ok(Self::drained(&(self.global)(), isolation_group)
            || Self::drained(&domain_cfg, isolation_group))
    }
}

impl PollIsolationGroupWaiter for DomainIsolationState {
    fn wait_until_healthy<'a>(
        &'a self,
        domain: &'a str,
        isolation_group: &'a str,
        deadline: Option<Instant>,
    ) -> Pin<Box<dyn Future<Output = Result<bool, CadenceError>> + Send + 'a>> {
        Box::pin(async move {
            loop {
                // Lookup errors are surfaced; the handler fails open like Go.
                let drained = self
                    .is_drained(domain, isolation_group)
                    .map_err(|e| CadenceError::Unknown(e))?;
                if !drained {
                    return Ok(true);
                }
                let now = Instant::now();
                let next = match deadline {
                    Some(d) if d <= now => return Ok(false),
                    Some(d) => (now + self.tick).min(d),
                    None => now + self.tick,
                };
                tokio::time::sleep_until(tokio::time::Instant::from_std(next)).await;
                if deadline.is_some_and(|d| Instant::now() >= d) {
                    return Ok(false);
                }
            }
        })
    }
}

/// Go `checkBadBinary`: reject decision polls from a binary checksum listed in
/// the domain's BadBinaries.
pub struct BadBinaryAdmission {
    bad_binaries: Arc<dyn Fn(&str) -> Result<Option<Vec<String>>, CadenceError> + Send + Sync>,
    metrics: Arc<dyn Metrics + Send + Sync>,
}

impl BadBinaryAdmission {
    pub fn new(
        bad_binaries: Arc<dyn Fn(&str) -> Result<Option<Vec<String>>, CadenceError> + Send + Sync>,
        metrics: Arc<dyn Metrics + Send + Sync>,
    ) -> Self {
        Self { bad_binaries, metrics }
    }
}

impl PollAdmissionPolicy for BadBinaryAdmission {
    fn admit<'a>(
        &'a self,
        domain: &'a str,
        _isolation_group: &'a str,
        binary_checksum: &'a str,
        decision_poll: bool,
    ) -> Pin<Box<dyn Future<Output = Result<(), CadenceError>> + Send + 'a>> {
        Box::pin(async move {
            // Go only checks bad binaries in PollForDecisionTask.
            if !decision_poll {
                return Ok(());
            }
            if let Some(bad) = (self.bad_binaries)(domain)? {
                if bad.iter().any(|b| b == binary_checksum) {
                    self.metrics.increment(Counter::BadBinary);
                    return Err(CadenceError::BadRequest(BadRequestError::new(format!(
                        "binary {binary_checksum} already marked as bad deployment"
                    ))));
                }
            }
            Ok(())
        })
    }
}

/// Go `cancelOutstandingPoll` over the production Thrift Matching transport.
pub struct ProductionPollCanceller {
    pub backend: crate::workflow_backend::ProductionBackend,
}

impl OutstandingPollCanceller for ProductionPollCanceller {
    fn cancel_poll<'a>(
        &'a self,
        request: serde_json::Value,
        _target_cluster: Option<String>,
    ) -> WorkflowFuture<'a> {
        Box::pin(async move {
            self.backend.cancel_outstanding_poll(&request).await?;
            Ok(serde_json::Value::Null)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use cadence_api::enums::IsolationGroupState as S;
    use cadence_api::types::IsolationGroupPartition;

    fn cfg(name: &str, state: S) -> Option<IsolationGroupConfiguration> {
        Some(IsolationGroupConfiguration {
            isolation_groups: vec![IsolationGroupPartition {
                name: name.into(),
                state: Some(state),
            }],
        })
    }

    fn state(enabled: bool, domain: Option<IsolationGroupConfiguration>, global: Option<IsolationGroupConfiguration>) -> DomainIsolationState {
        DomainIsolationState::new(
            Arc::new(move |_| enabled),
            Arc::new(move |_| Ok(domain.clone())),
            Arc::new(move || global.clone()),
        )
        .with_tick(Duration::from_millis(10))
    }

    #[test]
    fn headers_client_group() {
        let h = HashMap::from([(CLIENT_ISOLATION_GROUP_HEADER.to_owned(), "blue".to_owned())]);
        let (g, c) = context_from_headers(&h).unwrap();
        assert_eq!(g, "blue");
        assert_eq!(c.unwrap()[GROUP_KEY], "blue");
        let (g, c) = context_from_headers(&HashMap::new()).unwrap();
        assert!(g.is_empty() && c.is_none());
    }

    #[test]
    fn headers_forwarded_override_client() {
        let h = HashMap::from([
            (CLIENT_ISOLATION_GROUP_HEADER.to_owned(), "blue".to_owned()),
            (FORWARDING_CLUSTER_HEADER.to_owned(), "c2".to_owned()),
            (WORKER_ISOLATION_GROUP_HEADER.to_owned(), "red".to_owned()),
            (PARTITION_CONFIG_HEADER.to_owned(), r#"{"isolation-group":"red","wf-id":"x"}"#.to_owned()),
        ]);
        let (g, c) = context_from_headers(&h).unwrap();
        assert_eq!(g, "red");
        assert_eq!(c.unwrap()["wf-id"], "x");
        let bad = HashMap::from([
            (FORWARDING_CLUSTER_HEADER.to_owned(), "c2".to_owned()),
            (PARTITION_CONFIG_HEADER.to_owned(), "{".to_owned()),
        ]);
        assert!(context_from_headers(&bad).is_err());
    }

    #[test]
    fn drained_by_global_or_domain_and_flag() {
        assert!(state(true, None, cfg("a", S::Drained)).is_drained("d", "a").unwrap());
        assert!(state(true, cfg("a", S::Drained), None).is_drained("d", "a").unwrap());
        assert!(!state(true, cfg("a", S::Healthy), None).is_drained("d", "a").unwrap());
        assert!(!state(true, cfg("a", S::Drained), None).is_drained("d", "b").unwrap());
        assert!(!state(false, cfg("a", S::Drained), None).is_drained("d", "a").unwrap());
    }

    #[tokio::test]
    async fn waiter_times_out_when_drained_and_passes_when_healthy() {
        let s = state(true, cfg("a", S::Drained), None);
        let deadline = Instant::now() + Duration::from_millis(60);
        assert!(!s.wait_until_healthy("d", "a", Some(deadline)).await.unwrap());
        let ok = state(true, None, None);
        assert!(ok.wait_until_healthy("d", "a", None).await.unwrap());
    }

    #[derive(Default)]
    struct Count(Mutex<Vec<Counter>>);
    impl Metrics for Count {
        fn increment(&self, c: Counter) {
            self.0.lock().unwrap().push(c);
        }
    }

    #[tokio::test]
    async fn bad_binary_rejects_decision_polls_only() {
        let metrics = Arc::new(Count::default());
        let a = BadBinaryAdmission::new(
            Arc::new(|_| Ok(Some(vec!["bad".to_owned()]))),
            metrics.clone(),
        );
        let err = a.admit("d", "", "bad", true).await.unwrap_err();
        assert!(err.to_string().contains("binary bad already marked as bad deployment"));
        assert_eq!(metrics.0.lock().unwrap().as_slice(), &[Counter::BadBinary]);
        a.admit("d", "", "good", true).await.unwrap();
        a.admit("d", "", "bad", false).await.unwrap();
    }
}
