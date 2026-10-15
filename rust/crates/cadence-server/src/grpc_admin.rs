//! Native internal AdminAPI adapters over the shared Rust state.
//! Each adapter reuses the JSON admin handlers in-process, mirroring how the
//! public gRPC adapters reuse the HTTP handlers.

use crate::grpc::Frontend;
use cadence_proto::admin as a;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct Admin {
    pub frontend: Frontend,
}

impl Admin {
    async fn call<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        input: &Req,
    ) -> Result<Resp, Status> {
        self.call_with(axum::http::Method::POST, path, input).await
    }

    async fn call_with<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        method: axum::http::Method,
        path: &str,
        input: &Req,
    ) -> Result<Resp, Status> {
        self.frontend.call(method, path, input).await
    }
}

fn data_blob_out(v: cadence_api::types::DataBlob) -> cadence_proto::api::DataBlob {
    // Admin DataBlobs carry JSON bytes, mirroring Go's json.Marshal wrapper.
    cadence_proto::api::DataBlob {
        encoding_type: 2, // JSON
        data: v.data,
    }
}

fn data_blob_in(v: Option<cadence_proto::api::DataBlob>) -> Option<cadence_api::types::DataBlob> {
    v.map(|b| cadence_api::types::DataBlob {
        encoding_type: None,
        data: b.data,
    })
}

fn filters_in(
    v: Vec<a::DynamicConfigFilter>,
) -> Vec<cadence_api::admin::DynamicConfigFilter> {
    v.into_iter()
        .map(|f| cadence_api::admin::DynamicConfigFilter {
            name: f.name,
            value: data_blob_in(f.value),
        })
        .collect()
}

fn filters_out(
    v: Vec<cadence_api::admin::DynamicConfigFilter>,
) -> Vec<a::DynamicConfigFilter> {
    v.into_iter()
        .map(|f| a::DynamicConfigFilter {
            name: f.name,
            value: f.value.map(data_blob_out),
        })
        .collect()
}

fn values_in(
    v: Vec<a::DynamicConfigValue>,
) -> Vec<cadence_api::admin::DynamicConfigValue> {
    v.into_iter()
        .map(|c| cadence_api::admin::DynamicConfigValue {
            value: data_blob_in(c.value),
            filters: filters_in(c.filters),
        })
        .collect()
}

fn values_out(
    v: Vec<cadence_api::admin::DynamicConfigValue>,
) -> Vec<a::DynamicConfigValue> {
    v.into_iter()
        .map(|c| a::DynamicConfigValue {
            value: c.value.map(data_blob_out),
            filters: filters_out(c.filters),
        })
        .collect()
}

fn async_config_in(
    v: Option<cadence_proto::api::AsyncWorkflowConfiguration>,
) -> Option<cadence_api::types::AsyncWorkflowConfiguration> {
    v.map(|c| cadence_api::types::AsyncWorkflowConfiguration {
        enabled: c.enabled,
        predefined_queue_name: c.predefined_queue_name,
        queue_type: c.queue_type,
        queue_config: data_blob_in(c.queue_config),
    })
}

fn async_config_out(
    v: Option<cadence_api::types::AsyncWorkflowConfiguration>,
) -> Option<cadence_proto::api::AsyncWorkflowConfiguration> {
    v.map(|c| cadence_proto::api::AsyncWorkflowConfiguration {
        enabled: c.enabled,
        predefined_queue_name: c.predefined_queue_name,
        queue_type: c.queue_type,
        queue_config: c.queue_config.map(data_blob_out),
    })
}

#[tonic::async_trait]
impl a::admin_api_server::AdminApi for Admin {
    async fn describe_cluster(
        &self,
        _request: Request<a::DescribeClusterRequest>,
    ) -> Result<Response<a::DescribeClusterResponse>, Status> {
        let hostname =
            std::env::var("HOSTNAME").unwrap_or_else(|_| "cadence-rust".to_string());
        let v = crate::admin::describe_cluster_response(&hostname);
        Ok(Response::new(a::DescribeClusterResponse {
            supported_client_versions: v.supported_client_versions.map(|s| {
                cadence_proto::api::SupportedClientVersions {
                    go_sdk: s.go_sdk,
                    java_sdk: s.java_sdk,
                }
            }),
            membership_info: v.membership_info.map(|m| a::MembershipInfo {
                current_host: m.current_host.map(|h| a::HostInfo {
                    identity: h.identity,
                }),
                reachable_members: m.reachable_members,
                rings: m
                    .rings
                    .into_iter()
                    .map(|r| a::RingInfo {
                        role: r.role,
                        member_count: r.member_count,
                        members: r
                            .members
                            .into_iter()
                            .map(|h| a::HostInfo {
                                identity: h.identity,
                            })
                            .collect(),
                    })
                    .collect(),
            }),
            persistence_info: v
                .persistence_info
                .unwrap_or_default()
                .into_iter()
                .map(|(k, p)| {
                    (
                        k,
                        a::PersistenceInfo {
                            backend: p.backend,
                            settings: vec![],
                            features: p
                                .features
                                .into_iter()
                                .map(|f| a::PersistenceFeature {
                                    key: f.key,
                                    enabled: f.enabled,
                                })
                                .collect(),
                        },
                    )
                })
                .collect(),
        }))
    }

    async fn refresh_workflow_tasks(
        &self,
        request: Request<a::RefreshWorkflowTasksRequest>,
    ) -> Result<Response<a::RefreshWorkflowTasksResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::admin::RefreshWorkflowTasksRequest {
            domain: r.domain,
            domain_id: String::new(),
            workflow_execution: r.workflow_execution.map(|e| {
                cadence_api::types::WorkflowExecution {
                    workflow_id: e.workflow_id,
                    run_id: e.run_id,
                }
            }),
        };
        let _: cadence_api::admin::RefreshWorkflowTasksResponse =
            self.call("/api/v1/admin/workflows/refresh-tasks", &input).await?;
        Ok(Response::new(a::RefreshWorkflowTasksResponse {}))
    }

    async fn delete_workflow(
        &self,
        request: Request<a::DeleteWorkflowRequest>,
    ) -> Result<Response<a::DeleteWorkflowResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::admin::DeleteWorkflowRequest {
            domain: r.domain,
            workflow_execution: r.workflow_execution.map(|e| {
                cadence_api::types::WorkflowExecution {
                    workflow_id: e.workflow_id,
                    run_id: e.run_id,
                }
            }),
        };
        let v: cadence_api::admin::DeleteWorkflowResponse = self
            .call("/api/v1/admin/workflows/delete", &input)
            .await?;
        Ok(Response::new(a::DeleteWorkflowResponse {
            history_deleted: v.history_deleted,
            executions_deleted: v.executions_deleted,
            visibility_deleted: v.visibility_deleted,
        }))
    }

    async fn add_search_attribute(
        &self,
        request: Request<a::AddSearchAttributeRequest>,
    ) -> Result<Response<a::AddSearchAttributeResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::admin::AddSearchAttributeRequest {
            search_attribute: Some(
                r.search_attribute
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            k,
                            cadence_api::enums::IndexedValueType::from_i32(v)
                                .unwrap_or(cadence_api::enums::IndexedValueType::String),
                        )
                    })
                    .collect(),
            ),
            security_token: r.security_token,
        };
        let _: cadence_api::admin::AddSearchAttributeResponse = self
            .call("/api/v1/admin/search-attributes/add", &input)
            .await?;
        Ok(Response::new(a::AddSearchAttributeResponse {}))
    }

    async fn get_dynamic_config(
        &self,
        request: Request<a::GetDynamicConfigRequest>,
    ) -> Result<Response<a::GetDynamicConfigResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::admin::GetDynamicConfigRequest {
            config_name: r.config_name,
            filters: filters_in(r.filters),
        };
        let v: cadence_api::admin::GetDynamicConfigResponse = self
            .call("/api/v1/admin/dynamic-config/get", &input)
            .await?;
        Ok(Response::new(a::GetDynamicConfigResponse {
            value: v.value.map(data_blob_out),
        }))
    }

    async fn update_dynamic_config(
        &self,
        request: Request<a::UpdateDynamicConfigRequest>,
    ) -> Result<Response<a::UpdateDynamicConfigResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::admin::UpdateDynamicConfigRequest {
            config_name: r.config_name,
            config_values: values_in(r.config_values),
        };
        let _: cadence_api::admin::UpdateDynamicConfigResponse = self
            .call("/api/v1/admin/dynamic-config/update", &input)
            .await?;
        Ok(Response::new(a::UpdateDynamicConfigResponse {}))
    }

    async fn restore_dynamic_config(
        &self,
        request: Request<a::RestoreDynamicConfigRequest>,
    ) -> Result<Response<a::RestoreDynamicConfigResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::admin::RestoreDynamicConfigRequest {
            config_name: r.config_name,
            filters: filters_in(r.filters),
        };
        let _: cadence_api::admin::RestoreDynamicConfigResponse = self
            .call("/api/v1/admin/dynamic-config/restore", &input)
            .await?;
        Ok(Response::new(a::RestoreDynamicConfigResponse {}))
    }

    async fn list_dynamic_config(
        &self,
        request: Request<a::ListDynamicConfigRequest>,
    ) -> Result<Response<a::ListDynamicConfigResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::admin::ListDynamicConfigRequest {
            config_name: r.config_name,
        };
        let v: cadence_api::admin::ListDynamicConfigResponse = self
            .call("/api/v1/admin/dynamic-config/list", &input)
            .await?;
        Ok(Response::new(a::ListDynamicConfigResponse {
            entries: v
                .entries
                .into_iter()
                .map(|e| a::DynamicConfigEntry {
                    name: e.name,
                    values: values_out(e.values),
                })
                .collect(),
        }))
    }

    async fn get_operational_dynamic_config(
        &self,
        request: Request<a::GetOperationalDynamicConfigRequest>,
    ) -> Result<Response<a::GetOperationalDynamicConfigResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::admin::GetDynamicConfigRequest {
            config_name: r.config_name,
            filters: filters_in(r.filters),
        };
        let v: cadence_api::admin::GetDynamicConfigResponse = self
            .call("/api/v1/admin/dynamic-config-operational/get", &input)
            .await?;
        Ok(Response::new(a::GetOperationalDynamicConfigResponse {
            value: v.value.map(data_blob_out),
        }))
    }

    async fn update_operational_dynamic_config(
        &self,
        request: Request<a::UpdateOperationalDynamicConfigRequest>,
    ) -> Result<Response<a::UpdateOperationalDynamicConfigResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::admin::UpdateDynamicConfigRequest {
            config_name: r.config_name,
            config_values: values_in(r.config_values),
        };
        let _: cadence_api::admin::UpdateDynamicConfigResponse = self
            .call("/api/v1/admin/dynamic-config-operational/update", &input)
            .await?;
        Ok(Response::new(a::UpdateOperationalDynamicConfigResponse {}))
    }

    async fn restore_operational_dynamic_config(
        &self,
        request: Request<a::RestoreOperationalDynamicConfigRequest>,
    ) -> Result<Response<a::RestoreOperationalDynamicConfigResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::admin::RestoreDynamicConfigRequest {
            config_name: r.config_name,
            filters: filters_in(r.filters),
        };
        let _: cadence_api::admin::RestoreDynamicConfigResponse = self
            .call("/api/v1/admin/dynamic-config-operational/restore", &input)
            .await?;
        Ok(Response::new(a::RestoreOperationalDynamicConfigResponse {}))
    }

    async fn list_operational_dynamic_config(
        &self,
        request: Request<a::ListOperationalDynamicConfigRequest>,
    ) -> Result<Response<a::ListOperationalDynamicConfigResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::admin::ListDynamicConfigRequest {
            config_name: r.config_name,
        };
        let v: cadence_api::admin::ListDynamicConfigResponse = self
            .call("/api/v1/admin/dynamic-config-operational/list", &input)
            .await?;
        Ok(Response::new(a::ListOperationalDynamicConfigResponse {
            entries: v
                .entries
                .into_iter()
                .map(|e| a::DynamicConfigEntry {
                    name: e.name,
                    values: values_out(e.values),
                })
                .collect(),
        }))
    }

    async fn get_global_isolation_groups(
        &self,
        _request: Request<a::GetGlobalIsolationGroupsRequest>,
    ) -> Result<Response<a::GetGlobalIsolationGroupsResponse>, Status> {
        let v: cadence_api::admin::GetGlobalIsolationGroupsResponse = self
            .call(
                "/api/v1/admin/isolation-groups/global/get",
                &serde_json::json!({}),
            )
            .await?;
        Ok(Response::new(a::GetGlobalIsolationGroupsResponse {
            isolation_groups: v
                .isolation_groups
                .map(|c| cadence_proto::api::IsolationGroupConfiguration {
                    isolation_groups: c
                        .isolation_groups
                        .into_iter()
                        .map(|g| cadence_proto::api::IsolationGroupPartition {
                            name: g.name,
                            state: g.state.map_or(0, |s| s as i32 + 1),
                        })
                        .collect(),
                }),
        }))
    }

    async fn update_global_isolation_groups(
        &self,
        request: Request<a::UpdateGlobalIsolationGroupsRequest>,
    ) -> Result<Response<a::UpdateGlobalIsolationGroupsResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::admin::UpdateGlobalIsolationGroupsRequest {
            isolation_groups: r.isolation_groups.map(|c| {
                cadence_api::types::IsolationGroupConfiguration {
                    isolation_groups: c
                        .isolation_groups
                        .into_iter()
                        .map(|g| cadence_api::types::IsolationGroupPartition {
                            name: g.name,
                            state: cadence_api::enums::IsolationGroupState::from_i32(
                                g.state.checked_sub(1).unwrap_or(-1),
                            ),
                        })
                        .collect(),
                }
            }),
        };
        let _: cadence_api::admin::UpdateGlobalIsolationGroupsResponse = self
            .call("/api/v1/admin/isolation-groups/global/update", &input)
            .await?;
        Ok(Response::new(a::UpdateGlobalIsolationGroupsResponse {}))
    }

    async fn get_domain_isolation_groups(
        &self,
        request: Request<a::GetDomainIsolationGroupsRequest>,
    ) -> Result<Response<a::GetDomainIsolationGroupsResponse>, Status> {
        let r = request.into_inner();
        if r.domain.is_empty() {
            return Err(Status::invalid_argument("Domain is not set."));
        }
        let input = cadence_api::admin::GetDomainIsolationGroupsRequest { domain: r.domain };
        let v: cadence_api::admin::GetDomainIsolationGroupsResponse = self
            .call("/api/v1/admin/isolation-groups/domain/get", &input)
            .await?;
        Ok(Response::new(a::GetDomainIsolationGroupsResponse {
            isolation_groups: v.isolation_groups.map(|c| {
                cadence_proto::api::IsolationGroupConfiguration {
                    isolation_groups: c
                        .isolation_groups
                        .into_iter()
                        .map(|g| cadence_proto::api::IsolationGroupPartition {
                            name: g.name,
                            state: g.state.map_or(0, |s| s as i32 + 1),
                        })
                        .collect(),
                }
            }),
        }))
    }

    async fn update_domain_isolation_groups(
        &self,
        request: Request<a::UpdateDomainIsolationGroupsRequest>,
    ) -> Result<Response<a::UpdateDomainIsolationGroupsResponse>, Status> {
        let r = request.into_inner();
        if r.domain.is_empty() {
            return Err(Status::invalid_argument("Domain is not set."));
        }
        let input = cadence_api::admin::UpdateDomainIsolationGroupsRequest {
            domain: r.domain,
            isolation_groups: r.isolation_groups.map(|c| {
                cadence_api::types::IsolationGroupConfiguration {
                    isolation_groups: c
                        .isolation_groups
                        .into_iter()
                        .map(|g| cadence_api::types::IsolationGroupPartition {
                            name: g.name,
                            state: cadence_api::enums::IsolationGroupState::from_i32(
                                g.state.checked_sub(1).unwrap_or(-1),
                            ),
                        })
                        .collect(),
                }
            }),
        };
        let _: cadence_api::admin::UpdateDomainIsolationGroupsResponse = self
            .call("/api/v1/admin/isolation-groups/domain/update", &input)
            .await?;
        Ok(Response::new(a::UpdateDomainIsolationGroupsResponse {}))
    }

    async fn get_domain_async_workflow_configuraton(
        &self,
        request: Request<a::GetDomainAsyncWorkflowConfiguratonRequest>,
    ) -> Result<Response<a::GetDomainAsyncWorkflowConfiguratonResponse>, Status> {
        let r = request.into_inner();
        if r.domain.is_empty() {
            return Err(Status::invalid_argument("Domain is not set."));
        }
        let input = cadence_api::admin::GetDomainAsyncWorkflowConfiguratonRequest {
            domain: r.domain,
        };
        let v: cadence_api::admin::GetDomainAsyncWorkflowConfiguratonResponse = self
            .call("/api/v1/admin/async-workflow-config/get", &input)
            .await?;
        Ok(Response::new(a::GetDomainAsyncWorkflowConfiguratonResponse {
            configuration: async_config_out(v.configuration),
        }))
    }

    async fn update_domain_async_workflow_configuraton(
        &self,
        request: Request<a::UpdateDomainAsyncWorkflowConfiguratonRequest>,
    ) -> Result<Response<a::UpdateDomainAsyncWorkflowConfiguratonResponse>, Status> {
        let r = request.into_inner();
        if r.domain.is_empty() {
            return Err(Status::invalid_argument("Domain is not set."));
        }
        let input = cadence_api::admin::UpdateDomainAsyncWorkflowConfiguratonRequest {
            domain: r.domain,
            configuration: async_config_in(r.configuration),
        };
        let _: cadence_api::admin::UpdateDomainAsyncWorkflowConfiguratonResponse = self
            .call("/api/v1/admin/async-workflow-config/update", &input)
            .await?;
        Ok(Response::new(a::UpdateDomainAsyncWorkflowConfiguratonResponse {}))
    }

    async fn update_task_list_partition_config(
        &self,
        request: Request<a::UpdateTaskListPartitionConfigRequest>,
    ) -> Result<Response<a::UpdateTaskListPartitionConfigResponse>, Status> {
        let r = request.into_inner();
        if r.domain.is_empty() {
            return Err(Status::invalid_argument("Domain is not set."));
        }
        let matching = crate::grpc_matching::Matching::new(self.frontend.state.clone());
        let domain_id = self
            .frontend
            .state
            .domains
            .domain_id(&r.domain)
            .map_err(|e| Status::not_found(e.message))?;
        let task_list = r.task_list.unwrap_or_default();
        let config = r.partition_config.unwrap_or_default();
        let resp = cadence_proto::matching::matching_api_server::MatchingApi::update_task_list_partition_config(
            &matching,
            Request::new(cadence_proto::matching::UpdateTaskListPartitionConfigRequest {
                domain_id,
                task_list: Some(task_list),
                task_list_type: r.task_list_type,
                partition_config: Some(config),
            }),
        )
        .await?;
        let _ = resp;
        Ok(Response::new(a::UpdateTaskListPartitionConfigResponse {}))
    }

    async fn describe_history_host(
        &self,
        _request: Request<a::DescribeHistoryHostRequest>,
    ) -> Result<Response<a::DescribeHistoryHostResponse>, Status> {
        // Single host owns every shard and execution, so all oneof selectors
        // (host address, shard ID, workflow execution) resolve here.
        let v: cadence_api::admin::DescribeHistoryHostResponse = self
            .call(
                "/api/v1/admin/history-host/describe",
                &serde_json::json!({}),
            )
            .await?;
        Ok(Response::new(a::DescribeHistoryHostResponse {
            number_of_shards: v.number_of_shards,
            shard_ids: v.shard_ids,
            domain_cache: v.domain_cache.map(|d| a::DomainCacheInfo {
                num_of_items_in_cache_by_id: d.num_of_items_in_cache_by_id,
                num_of_items_in_cache_by_name: d.num_of_items_in_cache_by_name,
            }),
            shard_controller_status: v.shard_controller_status,
            address: v.address,
        }))
    }

    async fn describe_shard_distribution(
        &self,
        _request: Request<a::DescribeShardDistributionRequest>,
    ) -> Result<Response<a::DescribeShardDistributionResponse>, Status> {
        let v: cadence_api::admin::DescribeShardDistributionResponse = self
            .call_with(
                axum::http::Method::GET,
                "/api/v1/admin/shards/describe-distribution",
                &serde_json::json!({}),
            )
            .await?;
        Ok(Response::new(a::DescribeShardDistributionResponse {
            number_of_shards: v.number_of_shards,
            shards: v.shards.unwrap_or_default(),
        }))
    }

    async fn close_shard(
        &self,
        request: Request<a::CloseShardRequest>,
    ) -> Result<Response<a::CloseShardResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::admin::CloseShardRequest {
            shard_id: r.shard_id,
        };
        let _: cadence_api::admin::CloseShardResponse = self
            .call("/api/v1/admin/shards/close", &input)
            .await?;
        Ok(Response::new(a::CloseShardResponse {}))
    }

    async fn remove_task(
        &self,
        request: Request<a::RemoveTaskRequest>,
    ) -> Result<Response<a::RemoveTaskResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::admin::RemoveTaskRequest {
            shard_id: r.shard_id,
            task_type: Some(r.task_type),
            task_id: r.task_id,
            visibility_timestamp: r.visibility_time.map(|t| {
                t.seconds.saturating_mul(1_000_000_000)
                    .saturating_add(i64::from(t.nanos))
            }),
            cluster_name: r.cluster_name,
        };
        let _: cadence_api::admin::RemoveTaskResponse = self
            .call("/api/v1/admin/tasks/remove", &input)
            .await?;
        Ok(Response::new(a::RemoveTaskResponse {}))
    }

    async fn reset_queue(
        &self,
        request: Request<a::ResetQueueRequest>,
    ) -> Result<Response<a::ResetQueueResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::admin::ResetQueueRequest {
            shard_id: r.shard_id,
            cluster_name: r.cluster_name,
            task_type: Some(r.task_type),
        };
        let _: cadence_api::admin::ResetQueueResponse = self
            .call("/api/v1/admin/queues/reset", &input)
            .await?;
        Ok(Response::new(a::ResetQueueResponse {}))
    }

    async fn describe_queue(
        &self,
        request: Request<a::DescribeQueueRequest>,
    ) -> Result<Response<a::DescribeQueueResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::admin::DescribeQueueRequest {
            shard_id: r.shard_id,
            cluster_name: r.cluster_name,
            task_type: Some(r.task_type),
        };
        let v: cadence_api::admin::DescribeQueueResponse = self
            .call("/api/v1/admin/queues/describe", &input)
            .await?;
        Ok(Response::new(a::DescribeQueueResponse {
            processing_queue_states: v.processing_queue_states,
        }))
    }

    async fn get_workflow_execution_raw_history_v2(
        &self,
        request: Request<a::GetWorkflowExecutionRawHistoryV2Request>,
    ) -> Result<Response<a::GetWorkflowExecutionRawHistoryV2Response>, Status> {
        let r = request.into_inner();
        let input = cadence_api::admin::GetWorkflowExecutionRawHistoryV2Request {
            domain: r.domain,
            execution: r.workflow_execution.map(|e| {
                cadence_api::types::WorkflowExecution {
                    workflow_id: e.workflow_id,
                    run_id: e.run_id,
                }
            }),
            start_event_id: None,
            start_event_version: None,
            end_event_id: None,
            end_event_version: None,
            maximum_page_size: r.page_size,
            next_page_token: r.next_page_token,
        };
        if input.execution.as_ref().is_none_or(|e| e.workflow_id.is_empty()) {
            return Err(Status::invalid_argument("Invalid WorkflowID."));
        }
        if input
            .execution
            .as_ref()
            .is_none_or(|e| e.run_id.is_empty())
        {
            return Err(Status::invalid_argument("Invalid RunID."));
        }
        if input.maximum_page_size <= 0 {
            return Err(Status::invalid_argument("Invalid PageSize."));
        }
        let v: cadence_api::admin::GetWorkflowExecutionRawHistoryV2Response = self
            .call("/api/v1/admin/workflows/raw-history", &input)
            .await?;
        Ok(Response::new(a::GetWorkflowExecutionRawHistoryV2Response {
            next_page_token: v.next_page_token,
            history_batches: v
                .history_batches
                .into_iter()
                .map(|b| cadence_proto::api::DataBlob {
                    encoding_type: 2,
                    data: b.data,
                })
                .collect(),
            version_history: None,
        }))
    }

    async fn describe_workflow_execution(
        &self,
        request: Request<a::DescribeWorkflowExecutionRequest>,
    ) -> Result<Response<a::DescribeWorkflowExecutionResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::admin::AdminDescribeWorkflowExecutionRequest {
            domain: r.domain,
            execution: r.workflow_execution.map(|e| {
                cadence_api::types::WorkflowExecution {
                    workflow_id: e.workflow_id,
                    run_id: e.run_id,
                }
            }),
        };
        if input.domain.is_empty() {
            return Err(Status::invalid_argument("Domain is not set."));
        }
        if input.execution.as_ref().is_none_or(|e| e.workflow_id.is_empty()) {
            return Err(Status::invalid_argument("Execution is not set on request."));
        }
        let v: cadence_api::admin::AdminDescribeWorkflowExecutionResponse = self
            .call("/api/v1/admin/workflows/describe", &input)
            .await?;
        Ok(Response::new(a::DescribeWorkflowExecutionResponse {
            shard_id: v.shard_id,
            history_addr: v.history_addr,
            mutable_state_in_cache: v.mutable_state_in_cache,
            mutable_state_in_database: v.mutable_state_in_database,
        }))
    }

    async fn maintain_corrupt_workflow(
        &self,
        request: Request<a::MaintainCorruptWorkflowRequest>,
    ) -> Result<Response<a::MaintainCorruptWorkflowResponse>, Status> {
        let r = request.into_inner();
        let input = cadence_api::admin::MaintainCorruptWorkflowRequest {
            domain: r.domain,
            execution: r.workflow_execution.map(|e| {
                cadence_api::types::WorkflowExecution {
                    workflow_id: e.workflow_id,
                    run_id: e.run_id,
                }
            }),
        };
        if input.domain.is_empty() {
            return Err(Status::invalid_argument("Domain is not set."));
        }
        if input.execution.as_ref().is_none_or(|e| e.workflow_id.is_empty()) {
            return Err(Status::invalid_argument("Execution is missing"));
        }
        let v: cadence_api::admin::MaintainCorruptWorkflowResponse = self
            .call("/api/v1/admin/workflows/maintain-corrupt", &input)
            .await?;
        Ok(Response::new(a::MaintainCorruptWorkflowResponse {
            history_deleted: v.history_deleted,
            executions_deleted: v.executions_deleted,
            visibility_deleted: v.visibility_deleted,
        }))
    }

    async fn reapply_events(
        &self,
        request: Request<a::ReapplyEventsRequest>,
    ) -> Result<Response<a::ReapplyEventsResponse>, Status> {
        let r = request.into_inner();
        if r.domain.is_empty() {
            return Err(Status::invalid_argument("Domain is not set."));
        }
        let execution = r.workflow_execution.unwrap_or_default();
        if execution.workflow_id.is_empty() {
            return Err(Status::invalid_argument("WorkflowID is not set on request."));
        }
        let events = r.events.ok_or_else(|| Status::invalid_argument("WorkflowID is not set on request."))?;
        let input = cadence_api::admin::ReapplyEventsRequest {
            domain: r.domain,
            workflow_execution: Some(cadence_api::types::WorkflowExecution {
                workflow_id: execution.workflow_id,
                run_id: execution.run_id,
            }),
            events: Some(cadence_api::types::DataBlob {
                encoding_type: None,
                data: events.data,
            }),
        };
        let _: cadence_api::admin::ReapplyEventsResponse = self
            .call("/api/v1/admin/workflows/reapply-events", &input)
            .await?;
        Ok(Response::new(a::ReapplyEventsResponse {}))
    }

    async fn get_replication_messages(
        &self,
        request: Request<a::GetReplicationMessagesRequest>,
    ) -> Result<Response<a::GetReplicationMessagesResponse>, Status> {
        let r = request.into_inner();
        if r.cluster_name.is_empty() {
            return Err(Status::invalid_argument("Cluster name is not set."));
        }
        // Single host produces no replication streams.
        Ok(Response::new(a::GetReplicationMessagesResponse {
            shard_messages: Default::default(),
        }))
    }

    async fn get_dlq_replication_messages(
        &self,
        request: Request<a::GetDlqReplicationMessagesRequest>,
    ) -> Result<Response<a::GetDlqReplicationMessagesResponse>, Status> {
        let _ = request;
        Ok(Response::new(a::GetDlqReplicationMessagesResponse {
            replication_tasks: vec![],
        }))
    }

    async fn get_domain_replication_messages(
        &self,
        _request: Request<a::GetDomainReplicationMessagesRequest>,
    ) -> Result<Response<a::GetDomainReplicationMessagesResponse>, Status> {
        Err(Status::internal(
            "domain replication queue not enabled for cluster",
        ))
    }

    async fn count_dlq_messages(
        &self,
        _request: Request<a::CountDlqMessagesRequest>,
    ) -> Result<Response<a::CountDlqMessagesResponse>, Status> {
        Ok(Response::new(a::CountDlqMessagesResponse {
            history: vec![],
            domain: 0,
        }))
    }

    async fn read_dlq_messages(
        &self,
        request: Request<a::ReadDlqMessagesRequest>,
    ) -> Result<Response<a::ReadDlqMessagesResponse>, Status> {
        let r = request.into_inner();
        if r.r#type == 0 {
            return Err(Status::invalid_argument("Queue type is not set."));
        }
        Ok(Response::new(a::ReadDlqMessagesResponse {
            r#type: r.r#type,
            replication_tasks: vec![],
            replication_tasks_info: vec![],
            next_page_token: vec![],
        }))
    }

    async fn purge_dlq_messages(
        &self,
        request: Request<a::PurgeDlqMessagesRequest>,
    ) -> Result<Response<a::PurgeDlqMessagesResponse>, Status> {
        let r = request.into_inner();
        if r.r#type == 0 {
            return Err(Status::invalid_argument("Queue type is not set."));
        }
        Ok(Response::new(a::PurgeDlqMessagesResponse {}))
    }

    async fn merge_dlq_messages(
        &self,
        request: Request<a::MergeDlqMessagesRequest>,
    ) -> Result<Response<a::MergeDlqMessagesResponse>, Status> {
        let r = request.into_inner();
        if r.r#type == 0 {
            return Err(Status::invalid_argument("Queue type is not set."));
        }
        Ok(Response::new(a::MergeDlqMessagesResponse {
            next_page_token: vec![],
        }))
    }

    async fn resend_replication_tasks(
        &self,
        request: Request<a::ResendReplicationTasksRequest>,
    ) -> Result<Response<a::ResendReplicationTasksResponse>, Status> {
        let r = request.into_inner();
        let execution = r.workflow_execution.unwrap_or_default();
        let input = cadence_api::admin::ResendReplicationTasksRequest {
            domain_id: r.domain_id,
            workflow_id: execution.workflow_id,
            run_id: execution.run_id,
            remote_cluster: r.remote_cluster,
            start_event_id: None,
            start_version: None,
            end_event_id: None,
            end_version: None,
        };
        let _: cadence_api::admin::ResendReplicationTasksResponse = self
            .call("/api/v1/admin/replication/resend", &input)
            .await?;
        Ok(Response::new(a::ResendReplicationTasksResponse {}))
    }

    async fn get_cross_cluster_tasks(
        &self,
        request: Request<a::GetCrossClusterTasksRequest>,
    ) -> Result<Response<a::GetCrossClusterTasksResponse>, Status> {
        let r = request.into_inner();
        if r.target_cluster.is_empty() {
            return Err(Status::invalid_argument("Target cluster is not set."));
        }
        Ok(Response::new(a::GetCrossClusterTasksResponse {
            tasks_by_shard: Default::default(),
            failed_cause_by_shard: Default::default(),
        }))
    }

    async fn respond_cross_cluster_tasks_completed(
        &self,
        request: Request<a::RespondCrossClusterTasksCompletedRequest>,
    ) -> Result<Response<a::RespondCrossClusterTasksCompletedResponse>, Status> {
        let r = request.into_inner();
        if r.target_cluster.is_empty() {
            return Err(Status::invalid_argument("Target cluster is not set."));
        }
        Ok(Response::new(
            a::RespondCrossClusterTasksCompletedResponse { tasks: None },
        ))
    }
}

pub async fn serve(
    addr: std::net::SocketAddr,
    state: crate::handlers::AppState,
) -> anyhow::Result<()> {
    let limit = state.grpc_max_message_size;
    let service = Admin {
        frontend: Frontend {
            state: state.clone(),
        },
    };
    let _ = &state;
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(cadence_proto::FILE_DESCRIPTOR_SET)
        .with_service_name("uber.cadence.admin.v1.AdminAPI")
        .build_v1()?;
    tonic::transport::Server::builder()
        .add_service(reflection)
        .add_service(
            a::admin_api_server::AdminApiServer::new(service).max_decoding_message_size(limit),
        )
        .serve(addr)
        .await?;
    Ok(())
}
