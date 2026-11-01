//! Frontend -> Matching gRPC client ported from `go/client/matching`.
//!
//! Every poll picks a task-list partition with the load balancer, resolves the
//! Matching host that owns the partition name and sends the internal envelope
//! (domain UUID, poller ID, isolation group). Partition counts come from
//! dynamic config closures (`matching.numTasklistReadPartitions`).
use crate::history_forward::{map_status, ForwardError};
use cadence_api::{tasks as tk, types as t};
use cadence_proto::{api as p, convert as cv, matching as m};
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::Channel;

/// `constants.ReservedTaskListPrefix`.
pub const RESERVED_TASK_LIST_PREFIX: &str = "/__cadence_sys/";
/// `api.v1.TaskListType` values.
pub const TASK_LIST_TYPE_DECISION: i32 = 1;
pub const TASK_LIST_TYPE_ACTIVITY: i32 = 2;

/// Resolve the Matching gRPC channel owning a task-list partition name.
pub trait MatchingPeers: Send + Sync + std::fmt::Debug {
    fn channel_for_task_list(&self, partition_name: &str) -> Result<Channel, ForwardError>;
    /// Every Matching host (GetTaskListsByDomain fan-out).
    fn all_channels(&self) -> Result<Vec<Channel>, ForwardError>;
}

/// One configured Matching endpoint owning every task list.
#[derive(Debug, Clone)]
pub struct StaticMatchingPeer(pub Channel);

impl MatchingPeers for StaticMatchingPeer {
    fn channel_for_task_list(&self, _partition_name: &str) -> Result<Channel, ForwardError> {
        Ok(self.0.clone())
    }
    fn all_channels(&self) -> Result<Vec<Channel>, ForwardError> {
        Ok(vec![self.0.clone()])
    }
}

/// Go `getPartitionTaskListName`.
pub fn partition_task_list_name(root: &str, partition: usize) -> String {
    if partition == 0 {
        root.to_owned()
    } else {
        format!("{RESERVED_TASK_LIST_PREFIX}{root}/{partition}")
    }
}

/// Go `defaultLoadBalancer.PickReadPartition`; `random` is a uniform value.
pub fn pick_read_partition(
    name: &str,
    sticky: bool,
    forwarded_from: &str,
    read_partitions: usize,
    random: usize,
) -> String {
    if !forwarded_from.is_empty() || sticky || name.starts_with(RESERVED_TASK_LIST_PREFIX) {
        return name.to_owned();
    }
    let n = read_partitions.max(1);
    partition_task_list_name(name, random % n)
}

/// Dynamic-config lookup of the number of read partitions for a task list.
pub type PartitionCount = Arc<dyn Fn(&str, &str, i32) -> usize + Send + Sync>;

#[derive(Clone)]
pub struct MatchingClient {
    peers: Arc<dyn MatchingPeers>,
    read_partitions: PartitionCount,
    max_message_size: usize,
}

impl std::fmt::Debug for MatchingClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MatchingClient").finish_non_exhaustive()
    }
}

/// Sends `CancelOutstandingPoll` when a long poll is dropped before it
/// finishes (Go does this when the Frontend request context is cancelled).
struct CancelOnDrop {
    client: MatchingClient,
    request: Option<m::CancelOutstandingPollRequest>,
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let (Some(request), Ok(handle)) =
            (self.request.take(), tokio::runtime::Handle::try_current())
        {
            let client = self.client.clone();
            handle.spawn(async move {
                let _ = client.cancel_outstanding_poll(request).await;
            });
        }
    }
}

fn matching_request<T>(payload: T, long_poll: bool) -> tonic::Request<T> {
    let mut request = tonic::Request::new(payload);
    let md = request.metadata_mut();
    md.insert("rpc-service", "cadence-matching".parse().expect("static"));
    md.insert("rpc-caller", "cadence-frontend".parse().expect("static"));
    md.insert("rpc-encoding", "proto".parse().expect("static"));
    request.set_timeout(std::time::Duration::from_secs(if long_poll { 120 } else { 60 }));
    request
}

impl MatchingClient {
    pub fn new(
        peers: Arc<dyn MatchingPeers>,
        read_partitions: PartitionCount,
        max_message_size: usize,
    ) -> Self {
        Self {
            peers,
            read_partitions,
            max_message_size,
        }
    }

    fn api(
        &self,
        channel: Channel,
    ) -> m::matching_api_client::MatchingApiClient<Channel> {
        m::matching_api_client::MatchingApiClient::new(channel)
            .max_decoding_message_size(self.max_message_size)
            .max_encoding_message_size(self.max_message_size)
    }

    fn pick(&self, domain: &str, task_list: &t::TaskList, task_type: i32) -> String {
        let sticky = task_list.kind == Some(cadence_api::enums::TaskListKind::Sticky);
        let n = (self.read_partitions)(domain, &task_list.name, task_type);
        pick_read_partition(&task_list.name, sticky, "", n, rand::random::<usize>())
    }

    pub async fn poll_for_decision_task(
        &self,
        domain_id: &str,
        poller_id: &str,
        isolation_group: &str,
        req: &tk::PollForDecisionTaskRequest,
    ) -> Result<m::PollForDecisionTaskResponse, ForwardError> {
        let task_list = req.task_list.clone().unwrap_or_default();
        let partition = self.pick(&req.domain, &task_list, TASK_LIST_TYPE_DECISION);
        let channel = self.peers.channel_for_task_list(&partition)?;
        let mut api_req = cv::poll_for_decision_task_request_out(req.clone());
        if let Some(tl) = api_req.task_list.as_mut() {
            tl.name = partition.clone();
        }
        let mut guard = CancelOnDrop {
            client: self.clone(),
            request: Some(m::CancelOutstandingPollRequest {
                domain_id: domain_id.to_owned(),
                poller_id: poller_id.to_owned(),
                task_list_type: TASK_LIST_TYPE_DECISION,
                task_list: Some(p::TaskList {
                    name: partition,
                    ..Default::default()
                }),
            }),
        };
        let result = self
            .api(channel)
            .poll_for_decision_task(matching_request(
                m::PollForDecisionTaskRequest {
                    request: Some(api_req),
                    domain_id: domain_id.to_owned(),
                    poller_id: poller_id.to_owned(),
                    forwarded_from: String::new(),
                    isolation_group: isolation_group.to_owned(),
                },
                true,
            ))
            .await;
        guard.request = None;
        result.map(|r| r.into_inner()).map_err(|s| map_status(&s))
    }

    pub async fn poll_for_activity_task(
        &self,
        domain_id: &str,
        poller_id: &str,
        isolation_group: &str,
        req: &tk::PollForActivityTaskRequest,
    ) -> Result<m::PollForActivityTaskResponse, ForwardError> {
        let task_list = req.task_list.clone().unwrap_or_default();
        let partition = self.pick(&req.domain, &task_list, TASK_LIST_TYPE_ACTIVITY);
        let channel = self.peers.channel_for_task_list(&partition)?;
        let mut api_req = cv::poll_for_activity_task_request_out(req.clone());
        if let Some(tl) = api_req.task_list.as_mut() {
            tl.name = partition.clone();
        }
        let mut guard = CancelOnDrop {
            client: self.clone(),
            request: Some(m::CancelOutstandingPollRequest {
                domain_id: domain_id.to_owned(),
                poller_id: poller_id.to_owned(),
                task_list_type: TASK_LIST_TYPE_ACTIVITY,
                task_list: Some(p::TaskList {
                    name: partition,
                    ..Default::default()
                }),
            }),
        };
        let result = self
            .api(channel)
            .poll_for_activity_task(matching_request(
                m::PollForActivityTaskRequest {
                    request: Some(api_req),
                    domain_id: domain_id.to_owned(),
                    poller_id: poller_id.to_owned(),
                    forwarded_from: String::new(),
                    isolation_group: isolation_group.to_owned(),
                },
                true,
            ))
            .await;
        guard.request = None;
        result.map(|r| r.into_inner()).map_err(|s| map_status(&s))
    }

    pub async fn cancel_outstanding_poll(
        &self,
        request: m::CancelOutstandingPollRequest,
    ) -> Result<(), ForwardError> {
        let name = request
            .task_list
            .as_ref()
            .map(|t| t.name.clone())
            .unwrap_or_default();
        let channel = self.peers.channel_for_task_list(&name)?;
        self.api(channel)
            .cancel_outstanding_poll(matching_request(request, false))
            .await
            .map_err(|s| map_status(&s))?;
        Ok(())
    }

    /// Go `DescribeTaskList`: routed to the root partition's owner.
    pub async fn describe_task_list(
        &self,
        domain_id: &str,
        req: &tk::DescribeTaskListRequest,
    ) -> Result<tk::DescribeTaskListResponse, ForwardError> {
        let name = req
            .task_list
            .as_ref()
            .map(|t| t.name.clone())
            .unwrap_or_default();
        let channel = self.peers.channel_for_task_list(&name)?;
        let response = self
            .api(channel)
            .describe_task_list(matching_request(
                m::DescribeTaskListRequest {
                    domain_id: domain_id.to_owned(),
                    request: Some(cv::describe_task_list_request_out(req.clone())),
                },
                false,
            ))
            .await
            .map_err(|s| map_status(&s))?
            .into_inner();
        cv::describe_task_list_in(p::DescribeTaskListResponse {
            pollers: response.pollers,
            task_list_status: response.task_list_status,
            ..Default::default()
        })
            .map_err(|s| ForwardError::Internal(s.message().to_owned()))
    }

    /// Go `ListTaskListPartitions`: routed by the task-list name.
    pub async fn list_task_list_partitions(
        &self,
        req: &tk::ListTaskListPartitionsRequest,
    ) -> Result<tk::ListTaskListPartitionsResponse, ForwardError> {
        let name = req
            .task_list
            .as_ref()
            .map(|t| t.name.clone())
            .unwrap_or_default();
        let channel = self.peers.channel_for_task_list(&name)?;
        let response = self
            .api(channel)
            .list_task_list_partitions(matching_request(
                cv::list_task_list_partitions_request_out(req.clone()),
                false,
            ))
            .await
            .map_err(|s| map_status(&s))?
            .into_inner();
        Ok(cv::list_task_list_partitions_in(response))
    }

    /// Go `GetTaskListsByDomain`: fan out to every Matching host and merge.
    pub async fn get_task_lists_by_domain(
        &self,
        req: &tk::GetTaskListsByDomainRequest,
    ) -> Result<tk::GetTaskListsByDomainResponse, ForwardError> {
        let mut decision: HashMap<String, tk::DescribeTaskListResponse> = HashMap::new();
        let mut activity: HashMap<String, tk::DescribeTaskListResponse> = HashMap::new();
        for channel in self.peers.all_channels()? {
            let response = self
                .api(channel)
                .get_task_lists_by_domain(matching_request(
                    cv::get_task_lists_by_domain_request_out(req.clone()),
                    false,
                ))
                .await
                .map_err(|s| map_status(&s))?
                .into_inner();
            let converted = cv::get_task_lists_by_domain_in(response)
                .map_err(|s| ForwardError::Internal(s.message().to_owned()))?;
            merge_describe(&mut decision, converted.decision_task_list_map);
            merge_describe(&mut activity, converted.activity_task_list_map);
        }
        Ok(tk::GetTaskListsByDomainResponse {
            decision_task_list_map: Some(decision),
            activity_task_list_map: Some(activity),
        })
    }
}

/// Go `mergeDescribeTaskListResponses`: pollers of the same list are appended.
fn merge_describe(
    into: &mut HashMap<String, tk::DescribeTaskListResponse>,
    from: Option<HashMap<String, tk::DescribeTaskListResponse>>,
) {
    for (name, response) in from.unwrap_or_default() {
        match into.get_mut(&name) {
            Some(existing) => existing.pollers.extend(response.pollers),
            None => {
                into.insert(name, response);
            }
        }
    }
}

fn pb_timestamp(v: Option<prost_types::Timestamp>) -> Option<i64> {
    cv::nanos(v)
}

fn seconds(v: Option<prost_types::Duration>) -> Option<i32> {
    cv::duration_seconds(v)
}

fn query_in(q: p::WorkflowQuery) -> tk::WorkflowQuery {
    tk::WorkflowQuery {
        query_type: q.query_type,
        query_args: cv::data(q.query_args),
    }
}

/// Matching activity response -> Thrift (Go `ToPollForActivityTaskResponse`).
pub fn activity_response_in(r: m::PollForActivityTaskResponse) -> tk::PollForActivityTaskResponse {
    tk::PollForActivityTaskResponse {
        task_token: r.task_token,
        workflow_execution: r.workflow_execution.map(cv::execution),
        activity_id: r.activity_id,
        activity_type: r.activity_type.map(|a| t::ActivityType { name: a.name }),
        input: cv::data(r.input),
        scheduled_timestamp: pb_timestamp(r.scheduled_time),
        schedule_to_close_timeout_seconds: seconds(r.schedule_to_close_timeout),
        started_timestamp: pb_timestamp(r.started_time),
        start_to_close_timeout_seconds: seconds(r.start_to_close_timeout),
        heartbeat_timeout_seconds: seconds(r.heartbeat_timeout),
        attempt: r.attempt,
        scheduled_timestamp_of_this_attempt: pb_timestamp(r.scheduled_time_of_this_attempt),
        heartbeat_details: cv::data(r.heartbeat_details),
        workflow_type: r.workflow_type.map(|w| t::WorkflowType { name: w.name }),
        workflow_domain: r.workflow_domain,
        header: r.header.map(header_in),
    }
}

fn header_in(h: p::Header) -> t::Header {
    t::Header {
        fields: Some(h.fields.into_iter().map(|(k, v)| (k, v.data)).collect()),
    }
}

/// Matching decision response -> Thrift without history (the Frontend adds
/// history in `createPollForDecisionTaskResponse`).
pub fn decision_response_in(r: &m::PollForDecisionTaskResponse) -> tk::PollForDecisionTaskResponse {
    tk::PollForDecisionTaskResponse {
        task_token: r.task_token.clone(),
        workflow_execution: r.workflow_execution.clone().map(cv::execution),
        workflow_type: r
            .workflow_type
            .clone()
            .map(|w| t::WorkflowType { name: w.name }),
        previous_started_event_id: r.previous_started_event_id,
        started_event_id: r.started_event_id,
        attempt: i64::from(r.attempt),
        backlog_count_hint: r.backlog_count_hint,
        next_event_id: r.next_event_id,
        scheduled_timestamp: pb_timestamp(r.scheduled_time),
        started_timestamp: pb_timestamp(r.started_time),
        history: None,
        next_page_token: Vec::new(),
        workflow_execution_task_list: r.workflow_execution_task_list.clone().map(|tl| t::TaskList {
            name: tl.name,
            base_name: tl.base_name,
            kind: cv::enum_in(tl.kind, cadence_api::enums::TaskListKind::from_i32)
                .ok()
                .flatten(),
        }),
        query: r.query.clone().map(query_in),
        query_id: None,
        queries: if r.queries.is_empty() {
            None
        } else {
            Some(
                r.queries
                    .iter()
                    .map(|(k, q)| (k.clone(), query_in(q.clone())))
                    .collect(),
            )
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_names_follow_go() {
        assert_eq!(partition_task_list_name("tl", 0), "tl");
        assert_eq!(partition_task_list_name("tl", 3), "/__cadence_sys/tl/3");
    }

    #[test]
    fn load_balancer_matches_go() {
        assert_eq!(pick_read_partition("tl", false, "", 4, 7), "/__cadence_sys/tl/3");
        assert_eq!(pick_read_partition("tl", false, "", 4, 8), "tl");
        assert_eq!(pick_read_partition("tl", true, "", 4, 7), "tl");
        assert_eq!(pick_read_partition("tl", false, "x", 4, 7), "tl");
        assert_eq!(
            pick_read_partition("/__cadence_sys/tl/2", false, "", 4, 7),
            "/__cadence_sys/tl/2"
        );
        assert_eq!(pick_read_partition("tl", false, "", 0, 5), "tl");
    }

    #[test]
    fn describe_merge_appends_pollers() {
        let mut into = HashMap::new();
        let mut a = tk::DescribeTaskListResponse::default();
        a.pollers.push(Default::default());
        into.insert("x".to_owned(), a.clone());
        merge_describe(&mut into, Some(HashMap::from([("x".to_owned(), a)])));
        assert_eq!(into["x"].pollers.len(), 2);
    }
}
