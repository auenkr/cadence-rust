//! Worker process lifecycle boundary.

use crate::runtime::{Lifecycle, WorkflowRegistry};

#[derive(Debug)]
pub struct WorkerService {
    lifecycle: Lifecycle,
    registry: WorkflowRegistry,
}

impl Default for WorkerService {
    fn default() -> Self {
        let mut registry = WorkflowRegistry::default();
        let _ = registry.register(crate::parentclosepolicy::registration());
        let _ = registry.register(crate::archiver::registrations());
        let _ = registry.register(crate::domaindeprecation::registration());
        for registration in crate::failovermanager::registrations() {
            let _ = registry.register(registration);
        }
        let _ = registry.register(crate::scheduler::registration());
        for registration in crate::batcher::registrations() {
            let _ = registry.register(registration);
        }
        for registration in crate::scanner::registrations() {
            let _ = registry.register(registration);
        }
        Self {
            lifecycle: Lifecycle::default(),
            registry,
        }
    }
}

impl WorkerService {
    pub fn registrations(&self) -> impl Iterator<Item = &crate::runtime::WorkflowRegistration> {
        self.registry.iter()
    }

    /// Start the enabled pollers in Go `service.go` order and block until the
    /// process receives SIGINT/SIGTERM or `stop()` is called. Modules whose
    /// Go workflows are not yet ported to the SDK runtime stay disabled and
    /// are logged.
    pub fn start(&self, svc: &cadence_service_core::config::Config) -> anyhow::Result<()> {
        self.start_with_config(svc, &crate::config::Config::default())
    }

    pub fn start_with_config(
        &self,
        svc: &cadence_service_core::config::Config,
        config: &crate::config::Config,
    ) -> anyhow::Result<()> {
        if !self.lifecycle.start() {
            anyhow::bail!("worker service is already started or stopped")
        }
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(self.run(svc, config))
    }

    async fn run(
        &self,
        svc: &cadence_service_core::config::Config,
        config: &crate::config::Config,
    ) -> anyhow::Result<()> {
        eprintln!("worker starting");
        let endpoint = frontend_endpoint(svc)?;
        let client = crate::client::CadenceClient::connect_lazy(&endpoint)?;
        let identity = format!("{}@cadence-worker", std::process::id());
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let mut tasks = Vec::new();

        ensure_domain_exists(&client, crate::workercommon::SYSTEM_DOMAIN).await;
        for module in startup_plan(config) {
            match module.state {
                ModuleState::Disabled => eprintln!("worker module {} disabled by config", module.name),
                ModuleState::NotPorted => {
                    eprintln!("worker module {} not started: not ported to the Rust runtime", module.name)
                }
                ModuleState::Run => {
                    if module.name == "parentclosepolicy" {
                        tasks.extend(self.start_parent_close_policy(
                            svc, &endpoint, client.clone(), config, &identity, &stop_rx,
                        )?);
                        eprintln!("worker module parentclosepolicy started");
                    }
                }
            }
        }
        eprintln!("worker started");
        wait_for_shutdown(&self.lifecycle).await;
        let _ = stop_tx.send(true);
        for t in tasks {
            let _ = t.await;
        }
        self.lifecycle.stop();
        eprintln!("worker stopped");
        Ok(())
    }

    fn start_parent_close_policy(
        &self,
        svc: &cadence_service_core::config::Config,
        frontend: &str,
        client: crate::client::CadenceClient,
        config: &crate::config::Config,
        identity: &str,
        stop: &tokio::sync::watch::Receiver<bool>,
    ) -> anyhow::Result<Vec<tokio::task::JoinHandle<()>>> {
        use crate::parentclosepolicy as pcp;
        let history_channel = tonic::transport::Endpoint::from_shared(history_endpoint(svc))?
            .connect_lazy();
        let remotes = remote_endpoints(svc);
        let backend = std::sync::Arc::new(crate::parentclosepolicy_processor::GrpcBackend::new(
            history_channel,
            client.clone(),
            std::sync::Arc::new(move |cluster: &str| {
                remotes
                    .get(cluster)
                    .and_then(|e| crate::client::CadenceClient::connect_lazy(e).ok())
            }),
        ));
        let _ = frontend;
        let mut activities = crate::sdk::activity::ActivityRegistry::default();
        crate::parentclosepolicy_processor::register_activity(
            &mut activities,
            backend,
            config.parent_close_policy_system_workflows.max(1) as usize,
        );
        let mut workflows = crate::sdk::registry::Registry::default();
        pcp::register_sdk(&mut workflows);
        let decision = crate::sdk::decision_poller::DecisionWorker::new(
            client.clone(),
            std::sync::Arc::new(workflows),
            crate::sdk::decision_poller::PollerOptions {
                domain: crate::workercommon::SYSTEM_DOMAIN.to_owned(),
                task_list: pcp::TASK_LIST.to_owned(),
                identity: identity.to_owned(),
                binary_checksum: String::new(),
            },
        );
        let activity = crate::sdk::activity::ActivityWorker::new(
            client,
            std::sync::Arc::new(activities),
            crate::sdk::activity::ActivityPollerOptions {
                domain: crate::workercommon::SYSTEM_DOMAIN.to_owned(),
                task_list: pcp::TASK_LIST.to_owned(),
                identity: identity.to_owned(),
            },
        );
        Ok(vec![
            tokio::spawn(decision.run(stop.clone())),
            tokio::spawn(activity.run(stop.clone())),
        ])
    }

    pub fn stop(&self) -> bool {
        self.lifecycle.stop()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ModuleState {
    Run,
    Disabled,
    NotPorted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Module {
    pub name: &'static str,
    pub state: ModuleState,
}

/// Go `service.go` startup order with its config gates. Only modules whose
/// workflows run on the SDK runtime are `Run`.
pub fn startup_plan(config: &crate::config::Config) -> Vec<Module> {
    use ModuleState::*;
    let gated = |enabled: bool| if enabled { NotPorted } else { Disabled };
    vec![
        Module { name: "scanner", state: NotPorted },
        Module { name: "fixer", state: NotPorted },
        Module { name: "indexer", state: NotPorted },
        Module { name: "replicator", state: NotPorted },
        Module { name: "diagnostics", state: NotPorted },
        Module { name: "domaindeprecation", state: NotPorted },
        Module { name: "archiver", state: NotPorted },
        Module { name: "batcher", state: gated(config.enable_batcher) },
        Module { name: "scheduler", state: gated(config.enable_scheduler) },
        Module {
            name: "parentclosepolicy",
            state: if config.enable_parent_close_policy_worker { Run } else { Disabled },
        },
        Module { name: "esanalyzer", state: gated(config.enable_es_analyzer) },
        Module { name: "failovermanager", state: gated(config.enable_failover_manager) },
        Module { name: "asyncworkflow", state: gated(config.enable_async_workflow_consumption) },
    ]
}

/// Go builds the public client from the current cluster's `rpcAddress`.
pub fn frontend_endpoint(svc: &cadence_service_core::config::Config) -> anyhow::Result<String> {
    let meta = svc
        .cluster_metadata
        .as_ref()
        .or(svc.legacy_cluster_metadata.as_ref())
        .ok_or_else(|| anyhow::anyhow!("ClusterGroupMetadata cannot be empty"))?;
    let info = meta
        .cluster_group
        .get(&meta.current_cluster_name)
        .or_else(|| meta.cluster_information.get(&meta.current_cluster_name))
        .ok_or_else(|| anyhow::anyhow!("current cluster {} not in cluster group", meta.current_cluster_name))?;
    Ok(endpoint_url(&info.rpc_address))
}

fn endpoint_url(address: &str) -> String {
    if address.contains("://") {
        address.to_owned()
    } else {
        format!("http://{address}")
    }
}

/// Remote clusters' frontend addresses keyed by cluster name.
pub fn remote_endpoints(
    svc: &cadence_service_core::config::Config,
) -> std::collections::HashMap<String, String> {
    let mut out = std::collections::HashMap::new();
    if let Some(meta) = svc.cluster_metadata.as_ref().or(svc.legacy_cluster_metadata.as_ref()) {
        for (name, info) in meta.cluster_group.iter().chain(meta.cluster_information.iter()) {
            if *name != meta.current_cluster_name && !info.rpc_address.is_empty() {
                out.entry(name.clone()).or_insert_with(|| endpoint_url(&info.rpc_address));
            }
        }
    }
    out
}

/// History address: first-host routing only; shard-owner routing through
/// membership is not ported (single history host deployments).
pub fn history_endpoint(svc: &cadence_service_core::config::Config) -> String {
    let rpc = svc.services.get("history").map(|s| s.rpc.clone()).unwrap_or_default();
    let host = if rpc.bind_on_ip.is_empty() || rpc.bind_on_ip == "0.0.0.0" {
        "127.0.0.1".to_owned()
    } else {
        rpc.bind_on_ip
    };
    let port = if rpc.grpc_port == 0 { 7934 } else { rpc.grpc_port };
    format!("http://{host}:{port}")
}

async fn ensure_domain_exists(client: &crate::client::CadenceClient, domain: &str) {
    use cadence_proto::api::describe_domain_request::DescribeBy;
    let mut api = client.domain_api();
    let result = api
        .describe_domain(tonic::Request::new(cadence_proto::api::DescribeDomainRequest {
            describe_by: Some(DescribeBy::Name(domain.to_owned())),
        }))
        .await;
    match result {
        Ok(_) => {}
        Err(s) if s.code() == tonic::Code::NotFound => eprintln!(
            "system domain {domain} does not exist; persistence-level registration with the fixed system domain ID is not ported, pollers will retry until it exists"
        ),
        Err(s) => eprintln!("failed to verify if system domain {domain} exists: {s}"),
    }
}

async fn wait_for_shutdown(lifecycle: &Lifecycle) {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).ok();
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(500));
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => return,
            _ = async { match term.as_mut() { Some(t) => { t.recv().await; } None => std::future::pending().await } } => return,
            _ = tick.tick() => {
                if lifecycle.state() != crate::runtime::State::Running { return; }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_follows_go_order_and_gates() {
        let mut c = crate::config::Config::default();
        c.enable_batcher = false;
        let plan = startup_plan(&c);
        let names: Vec<_> = plan.iter().map(|m| m.name).collect();
        assert_eq!(names[0], "scanner");
        assert_eq!(names[9], "parentclosepolicy");
        assert_eq!(plan[7].state, ModuleState::Disabled);
        assert_eq!(plan[9].state, ModuleState::Run);
        c.enable_parent_close_policy_worker = false;
        assert_eq!(startup_plan(&c)[9].state, ModuleState::Disabled);
    }

    #[test]
    fn endpoint_gets_http_scheme() {
        assert_eq!(endpoint_url("localhost:7833"), "http://localhost:7833");
        assert_eq!(endpoint_url("https://x:1"), "https://x:1");
    }
}
