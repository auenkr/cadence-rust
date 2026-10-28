//! Production configuration and routing for the byte-preserving adaptive scaler.
use crate::{
    admin::DynamicConfigOverlay,
    handlers::AppState,
    matching_domain_cache::MatchingDomainCache,
    matching_membership::Membership,
    matching_registry_native::NativeQueueRegistry,
    matching_scaler::ScalerConfig,
    matching_scaler_native::{self, ConfigProvider, DescribePartition, DescribedPartition, NativeScalerConfig, ScalerMetrics},
    matching_shard_routing::MatchingShardRouter,
};
use cadence_common::{go_string::GoString, task_list::TaskListNameBytes};
use cadence_persistence::{dynamic_config as dc, matching_native_task_store::TaskListKeyBytes};
use cadence_proto::{native::{api, matching}, native_service::matching_api_client::MatchingApiClient};
use std::{sync::{Arc, Mutex}, time::{Duration, SystemTime, UNIX_EPOCH}};
use tonic::{Request, Status};

pub type DomainNameProvider = Arc<dyn Fn(&GoString) -> Option<String> + Send + Sync>;

/// Install a real metrics callback; callers must explicitly supply the sink.
pub fn attach_native_scaler(
    registry: NativeQueueRegistry,
    state: &AppState,
    cache: Arc<MatchingDomainCache>,
    membership: Membership,
    router: Option<Arc<MatchingShardRouter>>,
    metrics: ScalerMetrics,
) -> NativeQueueRegistry {
    let names = Arc::new(move |id: &GoString| {
        let uuid = id.as_utf8().ok().and_then(|s| uuid::Uuid::parse_str(s).ok())?;
        cache.get_domain_by_id(&uuid.to_string()).ok().map(|d| d.info.name)
    });
    registry.with_scaler_starter(matching_scaler_native::starter_with_metrics(
        config_provider(state.admin.dynamic_config.clone(), names),
        describe_provider(membership, router), metrics,
    ))
}

/// Sample every property from a single dynamic-config snapshot on each call.
pub fn config_provider(
    overlay: Arc<Mutex<DynamicConfigOverlay>>,
    domain_name: DomainNameProvider,
) -> ConfigProvider {
    Arc::new(move |domain, list, ty| {
        let name = domain_name(domain);
        let config = overlay.lock().expect("admin lock");
        let integer = |key, default| dc::task_list_integer_bytes(config.property_values(key), name.as_deref(), list.as_bytes(), ty, default);
        let duration = |key, default| dc::task_list_duration_bytes(config.property_values(key), name.as_deref(), list.as_bytes(), ty, default);
        let boolean = |key, default| dc::task_list_boolean_bytes(config.property_values(key), name.as_deref(), list.as_bytes(), ty, default);
        let groups: Vec<GoString> = dc::string_list(config.property_values("system.allIsolationGroups")).into_iter().map(GoString::from).collect();
        NativeScalerConfig {
            settings: ScalerConfig {
                enabled: boolean("matching.enableAdaptiveScaler", false),
                cache_enabled: boolean("matching.enableGetNumberOfPartitionsFromCache", false),
                update_interval_nanos: duration("matching.adaptiveScalerUpdateInterval", 15_000_000_000),
                upscale_rps: integer("matching.partitionUpscaleRPS", 200) as f64,
                downscale_factor: dc::task_list_float_bytes(config.property_values("matching.partitionDownscaleFactor"), name.as_deref(), list.as_bytes(), ty, 0.75),
                upscale_sustain_nanos: duration("matching.partitionUpscaleSustainedDuration", 60_000_000_000),
                downscale_sustain_nanos: duration("matching.partitionDownscaleSustainedDuration", 120_000_000_000),
                min_write_partitions: integer("matching.taskListMinimumWritePartitions", 1),
                isolation_assignment: boolean("matching.enablePartitionIsolationGroupAssignment", false),
                empty_check: boolean("matching.enablePartitionEmptyCheck", false),
                group_upscale_sustain_nanos: duration("matching.isolationGroupUpscaleSustainedDuration", 60_000_000_000),
                group_downscale_sustain_nanos: duration("matching.isolationGroupDownscaleSustainedDuration", 120_000_000_000),
                group_has_pollers_sustain_nanos: duration("matching.isolationGroupHasPollersSustainedDuration", 60_000_000_000),
                group_no_pollers_sustain_nanos: duration("matching.isolationGroupNoPollersSustainedDuration", 60_000_000_000),
                groups_per_partition: integer("matching.isolationGroupsPerPartition", 2),
                poller_window_nanos: duration("matching.taskIsolationPollerWindow", 2_000_000_000),
                // The native scaler uses the byte-valued list below.
                isolation_groups: Vec::new(),
            },
            isolation_groups: groups,
        }
    })
}

/// Construct the exact native request used for a non-root partition.
pub fn partition_request(root: &TaskListKeyBytes, id: i32) -> Result<matching::DescribeTaskListRequest, Status> {
    let name = TaskListNameBytes::parse(root.name.as_bytes()).map_err(|error| crate::matching_native_status::raw_status(tonic::Code::InvalidArgument, &error, tonic::codegen::Bytes::new()))?;
    Ok(matching::DescribeTaskListRequest {
        domain_id: GoString::from(root.domain_id.to_string()),
        request: Some(api::DescribeTaskListRequest {
            task_list: Some(api::TaskList { name: name.partition_name(id as i64), kind: 1, ..Default::default() }),
            task_list_type: root.task_type + 1,
            include_task_list_status: true,
            ..Default::default()
        }),
    })
}

/// Translate the owner's live queue status and partition config without decoding group keys.
pub fn partition_response(id: i32, response: matching::DescribeTaskListResponse, empty_check: bool) -> DescribedPartition {
    let status = response.task_list_status.unwrap_or_default();
    DescribedPartition {
        qps: status.new_tasks_per_second,
        empty: if empty_check { status.empty } else { status.backlog_count_hint == 0 },
        has_write: response.partition_config.as_ref().is_none_or(|config| config.write_partitions.contains_key(&id)),
        groups: status.isolation_group_metrics.into_iter().map(|(key, value)| (key, (value.new_tasks_per_second, value.poller_count > 0))).collect(),
    }
}

pub fn describe_provider(membership: Membership, router: Option<Arc<MatchingShardRouter>>) -> DescribePartition {
    Arc::new(move |registry, root, id, config| {
        let membership = membership.clone();
        let router = registry.shard_router().or_else(|| router.clone());
        Box::pin(async move {
            let message = partition_request(&root, id)?;
            let partition = &message.request.as_ref().expect("partition request").task_list.as_ref().expect("partition task list").name;
            let channel = if let Some(router) = router {
                router.remote_channel_bytes(partition.as_bytes()).await.map(|(channel, _)| channel).map_err(|error| Status::unavailable(error.to_string()))?
            } else { membership.remote_channel_bytes(partition.as_bytes())? };
            let Some(channel) = channel else {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos().min(i64::MAX as u128) as i64;
                return matching_scaler_native::describe_local(&registry, &root, id, &config, now).await;
            };
            let mut request = Request::new(message);
            request.set_timeout(Duration::from_secs(60)); // Go MatchingDefaultTimeout
            crate::matching_dispatch::matching_yarpc_grpc_metadata(&mut request);
            let reply = tokio::time::timeout(Duration::from_secs(60), MatchingApiClient::new(channel).describe_task_list(request)).await
                .map_err(|_| Status::deadline_exceeded("DescribeTaskList deadline exceeded"))??.into_inner();
            Ok(partition_response(id, reply, config.settings.empty_check))
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::{admin::{DynamicConfigFilter, DynamicConfigValue}, enums::EncodingType, types::DataBlob};
    use prost::Message;

    #[test]
    fn config_is_sampled_with_raw_task_list_filters() {
        let overlay = Arc::new(Mutex::new(DynamicConfigOverlay::default()));
        let provider = config_provider(overlay.clone(), Arc::new(|_| Some("domain".into())));
        let domain = GoString::from("id");
        let task = GoString::from(&b"queue\xff"[..]);
        let initial = provider(&domain, &task, 0);
        assert!(!initial.settings.enabled);
        assert_eq!(initial.settings.update_interval_nanos, 15_000_000_000);
        assert_eq!(initial.settings.upscale_rps, 200.0);
        assert_eq!(initial.settings.downscale_factor, 0.75);
        assert_eq!(initial.settings.poller_window_nanos, 2_000_000_000);
        overlay.lock().unwrap().update(false, "matching.partitionUpscaleRPS", vec![DynamicConfigValue {
            value: Some(DataBlob::new(EncodingType::JSON, b"17".to_vec())),
            filters: vec![DynamicConfigFilter { name: "taskListName".into(), value: Some(DataBlob::new(EncodingType::JSON, br#""queue\ufffd""#.to_vec())) }],
        }]);
        assert_eq!(provider(&domain, &task, 0).settings.upscale_rps, 200.0);
        assert_eq!(provider(&domain, &GoString::from("queue\u{fffd}"), 0).settings.upscale_rps, 17.0);
    }

    #[test]
    fn raw_partition_request_and_reply_preserve_bytes_and_scaler_fields() {
        let root = TaskListKeyBytes { domain_id: uuid::Uuid::nil(), name: GoString::from(&b"root\xff"[..]), task_type: 1 };
        let request = partition_request(&root, 2).unwrap();
        let wire = request.encode_to_vec();
        let decoded = matching::DescribeTaskListRequest::decode(wire.as_slice()).unwrap();
        let payload = decoded.request.unwrap();
        assert_eq!(payload.task_list.unwrap().name.as_bytes(), b"/__cadence_sys/root\xff/2");
        assert_eq!(payload.task_list_type, 2);
        assert!(payload.include_task_list_status);
        let group = GoString::from(&b"group\xff"[..]);
        let mut status = api::TaskListStatus { new_tasks_per_second: 4.5, backlog_count_hint: 0, empty: false, ..Default::default() };
        status.isolation_group_metrics.insert(group.clone(), api::IsolationGroupMetrics { new_tasks_per_second: 3.0, poller_count: 2 });
        let response = matching::DescribeTaskListResponse { task_list_status: Some(status), partition_config: Some(api::TaskListPartitionConfig::default()), ..Default::default() };
        let wire = response.encode_to_vec();
        let decoded = matching::DescribeTaskListResponse::decode(wire.as_slice()).unwrap();
        let metrics = partition_response(2, decoded, false);
        assert_eq!(metrics.qps, 4.5);
        assert!(metrics.empty);
        assert!(!metrics.has_write);
        assert_eq!(metrics.groups.get(&group), Some(&(3.0, true)));
    }
}
