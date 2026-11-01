//! Port of Go `domaindeprecation/activities.go` over a frontend `Backend`.

use std::sync::Arc;

use cadence_proto::api as pb;
use tonic::transport::Channel;

use super::entities::DomainDeprecationParams;
use super::helpers::{ERR_ACTIVE_POLLERS_NON_RETRYABLE, ERR_DOMAIN_DOES_NOT_EXIST_NON_RETRYABLE};
use super::{
    CHECK_ACTIVE_POLLERS_ACTIVITY, CHECK_OPEN_WORKFLOWS_ACTIVITY, DEPRECATE_DOMAIN_ACTIVITY,
    DISABLE_ARCHIVAL_ACTIVITY,
};
use crate::client::CadenceClient;
use crate::sdk::activity::{ActivityFailure, ActivityRegistry};
use crate::sdk::converter;

/// Go `scheduler.TaskListName`, whose pollers are ignored.
const SCHEDULER_TASK_LIST: &str = crate::scheduler::TASK_LIST;

pub enum DescribeError {
    NotFound,
    Other(String),
}

#[derive(Clone, Debug, Default)]
pub struct DomainInfo {
    /// Replication cluster names (empty when no replication configuration).
    pub clusters: Vec<String>,
    pub history_archival_disabled: bool,
    pub visibility_archival_disabled: bool,
}

/// Task lists of a domain in one cluster with their poller counts.
#[derive(Clone, Debug, Default)]
pub struct TaskLists {
    pub decision: Vec<(String, usize)>,
    pub activity: Vec<(String, usize)>,
}

pub trait Backend: Send + Sync + 'static {
    fn describe_domain(
        &self,
        name: &str,
    ) -> impl std::future::Future<Output = Result<DomainInfo, DescribeError>> + Send;
    fn task_lists_by_domain(
        &self,
        cluster: &str,
        domain: &str,
    ) -> impl std::future::Future<Output = Result<TaskLists, String>> + Send;
    /// Disables history and visibility archival; returns the resulting
    /// (history_disabled, visibility_disabled).
    fn disable_archival(
        &self,
        domain: &str,
        security_token: &str,
    ) -> impl std::future::Future<Output = Result<(bool, bool), String>> + Send;
    fn deprecate_domain(
        &self,
        domain: &str,
        security_token: &str,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send;
    fn count_open(&self, domain: &str)
        -> impl std::future::Future<Output = Result<i64, String>> + Send;
}

fn custom(reason: &str, message: Option<String>) -> ActivityFailure {
    ActivityFailure::Failed {
        reason: reason.to_owned(),
        details: message
            .map(|m| converter::encode(&m).unwrap_or_default())
            .unwrap_or_default(),
    }
}

async fn describe<B: Backend>(b: &B, name: &str) -> Result<DomainInfo, ActivityFailure> {
    b.describe_domain(name).await.map_err(|e| match e {
        DescribeError::NotFound => custom(ERR_DOMAIN_DOES_NOT_EXIST_NON_RETRYABLE, None),
        DescribeError::Other(m) => ActivityFailure::generic(format!("failed to describe domain: {m}")),
    })
}

pub async fn check_active_pollers<B: Backend>(
    b: &B,
    params: &DomainDeprecationParams,
) -> Result<(), ActivityFailure> {
    let info = describe(b, &params.domain_name).await?;
    if info.clusters.is_empty() {
        return Ok(());
    }
    let mut active = Vec::new();
    for cluster in &info.clusters {
        let lists = b.task_lists_by_domain(cluster, &params.domain_name).await.map_err(|e| {
            ActivityFailure::generic(format!(
                "failed to get task lists for domain {} in cluster {cluster}: {e}",
                params.domain_name
            ))
        })?;
        for (name, pollers) in lists.decision.iter().chain(lists.activity.iter()) {
            if name != SCHEDULER_TASK_LIST && *pollers > 0 {
                active.push(format!("{name} (cluster: {cluster})"));
            }
        }
    }
    if active.is_empty() {
        return Ok(());
    }
    Err(custom(
        ERR_ACTIVE_POLLERS_NON_RETRYABLE,
        Some(format!(
            "domain {} has active pollers on task lists: [{}], use Force to override",
            params.domain_name,
            active.join(" ")
        )),
    ))
}

pub async fn disable_archival<B: Backend>(
    b: &B,
    params: &DomainDeprecationParams,
) -> Result<(), ActivityFailure> {
    let info = describe(b, &params.domain_name).await?;
    if info.history_archival_disabled && info.visibility_archival_disabled {
        return Ok(());
    }
    let (history, visibility) = b
        .disable_archival(&params.domain_name, &params.security_token)
        .await
        .map_err(|e| ActivityFailure::generic(format!("failed to update domain: {e}")))?;
    if !history || !visibility {
        return Err(ActivityFailure::generic(format!(
            "failed to disable archival for domain {}",
            params.domain_name
        )));
    }
    Ok(())
}

pub async fn deprecate_domain<B: Backend>(
    b: &B,
    params: &DomainDeprecationParams,
) -> Result<(), ActivityFailure> {
    b.deprecate_domain(&params.domain_name, &params.security_token)
        .await
        .map_err(|e| ActivityFailure::generic(format!("failed to deprecate domain: {e}")))
}

pub async fn check_open_workflows<B: Backend>(
    b: &B,
    params: &DomainDeprecationParams,
) -> Result<bool, ActivityFailure> {
    b.count_open(&params.domain_name)
        .await
        .map(|c| c > 0)
        .map_err(|e| ActivityFailure::generic(format!("failed to count open workflows: {e}")))
}

fn decode_params(input: &[u8]) -> Result<DomainDeprecationParams, ActivityFailure> {
    converter::decode(input).map_err(|e| ActivityFailure::generic(e.to_string()))
}

pub fn register_activities<B: Backend>(registry: &mut ActivityRegistry, backend: Arc<B>) {
    macro_rules! unit {
        ($name:expr, $f:ident) => {{
            let b = backend.clone();
            registry.register($name, move |_ctx, input| {
                let b = b.clone();
                async move {
                    $f(&*b, &decode_params(&input)?).await?;
                    Ok(None)
                }
            });
        }};
    }
    unit!(CHECK_ACTIVE_POLLERS_ACTIVITY, check_active_pollers);
    unit!(DISABLE_ARCHIVAL_ACTIVITY, disable_archival);
    unit!(DEPRECATE_DOMAIN_ACTIVITY, deprecate_domain);
    let b = backend;
    registry.register(CHECK_OPEN_WORKFLOWS_ACTIVITY, move |_ctx, input| {
        let b = b.clone();
        async move {
            let open = check_open_workflows(&*b, &decode_params(&input)?).await?;
            converter::encode(&open)
                .map(Some)
                .map_err(|e| ActivityFailure::generic(e.to_string()))
        }
    });
}

/// Frontend gRPC backend. `remote` resolves another cluster's frontend channel.
pub struct GrpcBackend {
    local: Channel,
    current_cluster: String,
    remote: crate::batcher::grpc::AdminResolver,
    _client: CadenceClient,
}

impl GrpcBackend {
    pub fn new(
        local: Channel,
        current_cluster: String,
        remote: crate::batcher::grpc::AdminResolver,
        client: CadenceClient,
    ) -> Self {
        Self { local, current_cluster, remote, _client: client }
    }
}

impl Backend for GrpcBackend {
    async fn describe_domain(&self, name: &str) -> Result<DomainInfo, DescribeError> {
        use pb::domain_api_client::DomainApiClient;
        let resp = DomainApiClient::new(self.local.clone())
            .describe_domain(tonic::Request::new(pb::DescribeDomainRequest {
                describe_by: Some(pb::describe_domain_request::DescribeBy::Name(name.to_owned())),
            }))
            .await
            .map_err(|s| {
                if s.code() == tonic::Code::NotFound
                    && crate::parentclosepolicy_processor::detail_name(&s)
                        == "uber.cadence.api.v1.EntityNotExistsError"
                {
                    DescribeError::NotFound
                } else {
                    DescribeError::Other(s.to_string())
                }
            })?;
        let d = resp.into_inner().domain.unwrap_or_default();
        Ok(DomainInfo {
            clusters: d.clusters.into_iter().map(|c| c.cluster_name).collect(),
            history_archival_disabled: d.history_archival_status
                == pb::ArchivalStatus::Disabled as i32,
            visibility_archival_disabled: d.visibility_archival_status
                == pb::ArchivalStatus::Disabled as i32,
        })
    }

    async fn task_lists_by_domain(&self, cluster: &str, domain: &str) -> Result<TaskLists, String> {
        use pb::workflow_api_client::WorkflowApiClient;
        let channel = if cluster == self.current_cluster {
            self.local.clone()
        } else {
            (self.remote)(cluster)
                .ok_or_else(|| format!("failed to get frontend client for cluster {cluster}"))?
        };
        let resp = WorkflowApiClient::new(channel)
            .get_task_lists_by_domain(tonic::Request::new(pb::GetTaskListsByDomainRequest {
                domain: domain.to_owned(),
            }))
            .await
            .map_err(|s| s.to_string())?
            .into_inner();
        let conv = |m: std::collections::HashMap<String, pb::DescribeTaskListResponse>| {
            let mut v: Vec<_> = m.into_iter().map(|(k, t)| (k, t.pollers.len())).collect();
            v.sort();
            v
        };
        Ok(TaskLists {
            decision: conv(resp.decision_task_list_map),
            activity: conv(resp.activity_task_list_map),
        })
    }

    async fn disable_archival(&self, domain: &str, token: &str) -> Result<(bool, bool), String> {
        use pb::domain_api_client::DomainApiClient;
        let disabled = pb::ArchivalStatus::Disabled as i32;
        let resp = DomainApiClient::new(self.local.clone())
            .update_domain(tonic::Request::new(pb::UpdateDomainRequest {
                security_token: token.to_owned(),
                name: domain.to_owned(),
                history_archival_status: disabled,
                visibility_archival_status: disabled,
                update_mask: Some(prost_types::FieldMask {
                    paths: vec![
                        "history_archival_status".to_owned(),
                        "visibility_archival_status".to_owned(),
                    ],
                }),
                ..Default::default()
            }))
            .await
            .map_err(|s| s.to_string())?
            .into_inner();
        let d = resp.domain.unwrap_or_default();
        Ok((d.history_archival_status == disabled, d.visibility_archival_status == disabled))
    }

    async fn deprecate_domain(&self, domain: &str, token: &str) -> Result<(), String> {
        use pb::domain_api_client::DomainApiClient;
        DomainApiClient::new(self.local.clone())
            .deprecate_domain(tonic::Request::new(pb::DeprecateDomainRequest {
                security_token: token.to_owned(),
                name: domain.to_owned(),
            }))
            .await
            .map(|_| ())
            .map_err(|s| s.to_string())
    }

    async fn count_open(&self, domain: &str) -> Result<i64, String> {
        use pb::visibility_api_client::VisibilityApiClient;
        VisibilityApiClient::new(self.local.clone())
            .count_workflow_executions(tonic::Request::new(pb::CountWorkflowExecutionsRequest {
                domain: domain.to_owned(),
                query: "CloseTime = missing".to_owned(),
            }))
            .await
            .map(|r| r.into_inner().count)
            .map_err(|s| s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Fake {
        missing: bool,
        info: DomainInfo,
        lists: Vec<(String, TaskLists)>,
        update_result: (bool, bool),
        open: i64,
        updates: Mutex<u32>,
    }

    impl Backend for Fake {
        async fn describe_domain(&self, _: &str) -> Result<DomainInfo, DescribeError> {
            if self.missing { Err(DescribeError::NotFound) } else { Ok(self.info.clone()) }
        }
        async fn task_lists_by_domain(&self, c: &str, _: &str) -> Result<TaskLists, String> {
            Ok(self.lists.iter().find(|(n, _)| n == c).map(|(_, l)| l.clone()).unwrap_or_default())
        }
        async fn disable_archival(&self, _: &str, _: &str) -> Result<(bool, bool), String> {
            *self.updates.lock().unwrap() += 1;
            Ok(self.update_result)
        }
        async fn deprecate_domain(&self, _: &str, _: &str) -> Result<(), String> { Ok(()) }
        async fn count_open(&self, _: &str) -> Result<i64, String> { Ok(self.open) }
    }

    fn params() -> DomainDeprecationParams {
        DomainDeprecationParams { domain_name: "d".into(), ..Default::default() }
    }

    fn block<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(f)
    }

    #[test]
    fn missing_domain_is_non_retryable() {
        let f = Fake { missing: true, ..Default::default() };
        let err = block(check_active_pollers(&f, &params())).unwrap_err();
        assert_eq!(err, custom("domain does not exist", None));
    }

    #[test]
    fn active_pollers_ignore_scheduler_list_and_report_others() {
        let mut f = Fake::default();
        f.info.clusters = vec!["c1".into()];
        f.lists = vec![("c1".into(), TaskLists {
            decision: vec![("cadence-scheduler".into(), 2), ("tl".into(), 1)],
            activity: vec![("idle".into(), 0)],
        })];
        let err = block(check_active_pollers(&f, &params())).unwrap_err();
        let ActivityFailure::Failed { reason, details } = err else { panic!() };
        assert_eq!(reason, "domain has active pollers");
        let msg: String = converter::decode(&details).unwrap();
        assert_eq!(msg, "domain d has active pollers on task lists: [tl (cluster: c1)], use Force to override");
        f.lists[0].1.decision.remove(1);
        assert!(block(check_active_pollers(&f, &params())).is_ok());
    }

    #[test]
    fn no_replication_config_skips_poller_check() {
        assert!(block(check_active_pollers(&Fake::default(), &params())).is_ok());
    }

    #[test]
    fn disable_archival_skips_when_already_disabled_and_validates_result() {
        let mut f = Fake::default();
        f.info.history_archival_disabled = true;
        f.info.visibility_archival_disabled = true;
        block(disable_archival(&f, &params())).unwrap();
        assert_eq!(*f.updates.lock().unwrap(), 0);
        f.info.visibility_archival_disabled = false;
        f.update_result = (true, false);
        assert!(block(disable_archival(&f, &params())).is_err());
        f.update_result = (true, true);
        block(disable_archival(&f, &params())).unwrap();
    }

    #[test]
    fn open_workflow_count_maps_to_bool() {
        let mut f = Fake::default();
        assert!(!block(check_open_workflows(&f, &params())).unwrap());
        f.open = 3;
        assert!(block(check_open_workflows(&f, &params())).unwrap());
    }
}
