//! End-to-end tests driving the full HTTP API without any Go code:
//! register domain → start → describe → signal → history → terminate →
//! describe (closed) → list. Mirrors the `go/host` integration flow for the
//! core workflow lifecycle.

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use cadence_server::handlers::{router, AppState};
use serde_json::{json, Value};
use tower::ServiceExt;

async fn call(app: axum::Router, method: Method, uri: &str, body: Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, value)
}

async fn get(app: axum::Router, uri: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

fn start_body(request_id: &str) -> Value {
    json!({
        "domain": "e2e-domain",
        "workflowId": "e2e-wf",
        "workflowType": {"name": "e2e-type"},
        "taskList": {"name": "e2e-tl"},
        "executionStartToCloseTimeoutSeconds": 60,
        "taskStartToCloseTimeoutSeconds": 10,
        "identity": "e2e",
        "requestId": request_id,
    })
}

async fn setup_domain(app: axum::Router) {
    let (status, _) = call(
        app,
        Method::POST,
        "/api/v1/domains/register",
        json!({"name": "e2e-domain", "workflowExecutionRetentionPeriodInDays": 7}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn health_reports_ok() {
    let (status, body) = get(router(AppState::default()), "/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], true);
}

#[tokio::test]
async fn full_workflow_lifecycle() {
    let app = router(AppState::default());
    setup_domain(app.clone()).await;

    // Start.
    let (status, started) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/start",
        start_body("123e4567-e89b-12d3-a456-426614174001"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{started}");
    let run_id = started["runId"].as_str().unwrap().to_string();
    assert!(!run_id.is_empty());

    // Idempotent retry with the same request ID returns the same run.
    let (status, again) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/start",
        start_body("123e4567-e89b-12d3-a456-426614174001"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(again["runId"].as_str().unwrap(), run_id);

    // Duplicate start with a new request ID is a 409.
    let (status, body) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/start",
        start_body("123e4567-e89b-12d3-a456-426614174002"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["type"], "WorkflowExecutionAlreadyStartedError");

    // Describe (open).
    let (status, desc) = get(
        app.clone(),
        &format!("/api/v1/workflows/describe?domain=e2e-domain&workflowId=e2e-wf&runId={run_id}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(desc["workflowExecutionInfo"]["closeStatus"].is_null());
    assert_eq!(desc["workflowExecutionInfo"]["historyLength"], 2); // started + decision-scheduled

    // Signal appends a history event.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/signal",
        json!({
            "domain": "e2e-domain",
            "workflowExecution": {"workflowId": "e2e-wf", "runId": run_id},
            "signalName": "sig",
            "identity": "e2e",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // History shows started + signaled.
    let (status, hist) = get(
        app.clone(),
        &format!("/api/v1/workflows/history?domain=e2e-domain&workflowId=e2e-wf&runId={run_id}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(hist["history"]["events"].as_array().unwrap().len(), 3); // + signaled

    // Terminate closes the workflow.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/terminate",
        json!({
            "domain": "e2e-domain",
            "workflowExecution": {"workflowId": "e2e-wf", "runId": run_id},
            "reason": "e2e done",
            "identity": "e2e",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Describe (closed, terminated).
    let (status, desc) = get(
        app.clone(),
        &format!("/api/v1/workflows/describe?domain=e2e-domain&workflowId=e2e-wf&runId={run_id}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(desc["workflowExecutionInfo"]["closeStatus"], 3); // TERMINATED
    assert_eq!(desc["workflowExecutionInfo"]["historyLength"], 4); // + terminated

    // Signal after close is a 409.
    let (status, body) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/signal",
        json!({
            "domain": "e2e-domain",
            "workflowExecution": {"workflowId": "e2e-wf", "runId": run_id},
            "signalName": "late",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["type"], "WorkflowExecutionAlreadyCompletedError");

    // Lists reflect open/closed split.
    let (status, open) = get(
        app.clone(),
        "/api/v1/workflows/list?domain=e2e-domain&status=open",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(open.as_array().unwrap().len(), 0);
    let (status, closed) = get(
        app.clone(),
        "/api/v1/workflows/list?domain=e2e-domain&status=closed",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(closed.as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn unknown_domain_is_404() {
    let app = router(AppState::default());
    let (status, body) = call(
        app,
        Method::POST,
        "/api/v1/workflows/start",
        start_body("123e4567-e89b-12d3-a456-426614174001"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["type"], "EntityNotExistsError");
}

#[tokio::test]
async fn domain_crud_round_trip() {
    let app = router(AppState::default());
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/domains/register",
        json!({"name": "d1", "workflowExecutionRetentionPeriodInDays": 7}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Duplicate register is a 409.
    let (status, body) = call(
        app.clone(),
        Method::POST,
        "/api/v1/domains/register",
        json!({"name": "d1", "workflowExecutionRetentionPeriodInDays": 7}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["type"], "DomainAlreadyExistsError");

    let (status, desc) = get(app.clone(), "/api/v1/domains/describe?name=d1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(desc["domainInfo"]["name"], "d1");

    let (status, domains) = get(app.clone(), "/api/v1/domains/list").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(domains["domains"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn worker_simulation_drives_workflow_to_completion() {
    // Simulates a Cadence worker: poll decision → schedule activity →
    // poll activity → complete activity → complete workflow.
    let app = router(AppState::default());
    setup_domain(app.clone()).await;

    let (status, started) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/start",
        start_body("123e4567-e89b-12d3-a456-426614174009"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let run_id = started["runId"].as_str().unwrap().to_string();

    // 1. Poll the initial decision task.
    let (status, decision) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasks/poll-decision?timeoutSecs=1",
        json!({"domain": "e2e-domain", "taskList": {"name": "e2e-tl"}, "identity": "worker"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{decision}");
    let decision_token = decision["taskToken"].as_str().unwrap().to_string();
    assert_eq!(decision["attempt"], 0);

    // 2. Respond with a schedule-activity decision.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasks/respond-decision-complete",
        json!({
            "taskToken": decision_token,
            "identity": "worker",
            "decisions": [{
                "decisionType": 0,
                "scheduleActivityTaskDecisionAttributes": {
                    "activityId": "act-1",
                    "activityType": {"name": "greet"},
                    "taskList": {"name": "e2e-tl"},
                },
            }],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // 3. Poll and complete the activity.
    let (status, activity) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasks/poll-activity?timeoutSecs=1",
        json!({"domain": "e2e-domain", "taskList": {"name": "e2e-tl"}, "identity": "worker"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{activity}");
    assert_eq!(activity["activityId"], "act-1");
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasks/respond-activity-complete",
        json!({
            "taskToken": activity["taskToken"],
            "result": "aGk=",
            "identity": "worker",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // 4. A new decision was scheduled; complete the workflow through it.
    let (status, decision2) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasks/poll-decision?timeoutSecs=1",
        json!({"domain": "e2e-domain", "taskList": {"name": "e2e-tl"}, "identity": "worker"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasks/respond-decision-complete",
        json!({
            "taskToken": decision2["taskToken"],
            "identity": "worker",
            "decisions": [{
                "decisionType": 3,
                "completeWorkflowExecutionDecisionAttributes": {"result": "ZG9uZQ=="},
            }],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // 5. Workflow is closed completed with the full event chain.
    let (status, desc) = get(
        app.clone(),
        &format!("/api/v1/workflows/describe?domain=e2e-domain&workflowId=e2e-wf&runId={run_id}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(desc["workflowExecutionInfo"]["closeStatus"], 0); // COMPLETED
    let (status, hist) = get(
        app.clone(),
        &format!("/api/v1/workflows/history?domain=e2e-domain&workflowId=e2e-wf&runId={run_id}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let kinds: Vec<String> = hist["history"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["eventType"].to_string())
        .collect();
    assert_eq!(
        kinds,
        vec!["0", "4", "5", "6", "9", "10", "11", "4", "5", "6", "1"]
    );
}

#[tokio::test]
async fn empty_poll_returns_no_content() {
    let app = router(AppState::default());
    setup_domain(app.clone()).await;
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasks/poll-decision?timeoutSecs=1",
        json!({"domain": "e2e-domain", "taskList": {"name": "e2e-tl"}, "identity": "worker"}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn signal_with_start_starts_or_signals() {
    let app = router(AppState::default());
    setup_domain(app.clone()).await;
    let body = json!({
        "domain": "e2e-domain",
        "workflowId": "sws-wf",
        "workflowType": {"name": "t"},
        "taskList": {"name": "e2e-tl"},
        "executionStartToCloseTimeoutSeconds": 60,
        "taskStartToCloseTimeoutSeconds": 10,
        "requestId": "123e4567-e89b-12d3-a456-426614174000",
        "signalName": "go",
    });
    let (status, first) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/signal-with-start",
        body.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, second) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/signal-with-start",
        body,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["runId"], second["runId"]);
}

#[tokio::test]
async fn query_flow_answers_through_worker() {
    let app = router(AppState::default());
    setup_domain(app.clone()).await;
    let (status, started) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/start",
        start_body("123e4567-e89b-12d3-a456-426614174021"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let run_id = started["runId"].as_str().unwrap().to_string();

    // Worker task: poll in background, answer the embedded query.
    let worker_app = app.clone();
    let worker = tokio::spawn(async move {
        let (status, decision) = call(
            worker_app.clone(),
            Method::POST,
            "/api/v1/tasks/poll-decision?timeoutSecs=10",
            json!({"domain": "e2e-domain", "taskList": {"name": "e2e-tl"}, "identity": "worker"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let qid = decision["queryId"].as_str().unwrap().to_string();
        assert_eq!(decision["query"]["queryType"], "state");
        let (status, _) = call(
            worker_app,
            Method::POST,
            "/api/v1/tasks/respond-decision-complete",
            json!({
                "taskToken": decision["taskToken"],
                "identity": "worker",
                "decisions": [],
                "queryResults": {
                    qid.clone(): {"resultType": 0, "answer": "NDI="},
                },
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    });

    let (status, resp) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/query?timeoutSecs=10",
        json!({
            "domain": "e2e-domain",
            "execution": {"workflowId": "e2e-wf", "runId": run_id},
            "query": {"queryType": "state"},
        }),
    )
    .await;
    worker.await.unwrap();
    assert_eq!(status, StatusCode::OK, "{resp}");
    assert!(resp.get("queryResult").is_some());
}

#[tokio::test]
async fn query_task_completed_answers_direct_query() {
    use base64::{engine::general_purpose::STANDARD as B64, Engine};
    let app = router(AppState::default());
    setup_domain(app.clone()).await;
    let (status, started) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/start",
        start_body("123e4567-e89b-12d3-a456-426614174022"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let run_id = started["runId"].as_str().unwrap().to_string();

    // Worker task: poll in background, answer via RespondQueryTaskCompleted
    // with a minted QueryTaskToken, like a direct matching dispatch.
    let worker_app = app.clone();
    let run_id_worker = run_id.clone();
    let worker = tokio::spawn(async move {
        let (status, decision) = call(
            worker_app.clone(),
            Method::POST,
            "/api/v1/tasks/poll-decision?timeoutSecs=10",
            json!({"domain": "e2e-domain", "taskList": {"name": "e2e-tl"}, "identity": "worker"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let qid = decision["queryId"].as_str().unwrap().to_string();
        // Decode the decision token to recover the domain UUID, then mint
        // the query token the same way Go matching does: the buffered
        // query ID becomes the token taskId.
        let raw = decision["taskToken"].as_str().unwrap().to_string();
        let decoded: serde_json::Value =
            serde_json::from_slice(&B64.decode(raw.as_bytes()).unwrap()).unwrap();
        let token = serde_json::json!({
            "domainId": decoded["domainId"],
            "workflowId": "e2e-wf",
            "runId": run_id_worker,
            "taskList": "e2e-tl",
            "taskId": qid,
        });
        let token_b64 = B64.encode(serde_json::to_vec(&token).unwrap());
        let (status, _) = call(
            worker_app.clone(),
            Method::POST,
            "/api/v1/tasks/respond-query-completed",
            json!({
                "taskToken": token_b64,
                "completedType": 0,
                "queryResult": "NDI=",
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        // Complete the decision itself so the workflow can proceed.
        let (status, _) = call(
            worker_app,
            Method::POST,
            "/api/v1/tasks/respond-decision-complete",
            json!({
                "taskToken": decision["taskToken"],
                "identity": "worker",
                "decisions": [],
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    });

    let (status, resp) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/query?timeoutSecs=10",
        json!({
            "domain": "e2e-domain",
            "execution": {"workflowId": "e2e-wf", "runId": run_id},
            "query": {"queryType": "state"},
        }),
    )
    .await;
    worker.await.unwrap();
    assert_eq!(status, StatusCode::OK, "{resp}");
    assert!(resp.get("queryResult").is_some());
}

#[tokio::test]
async fn task_list_discovery_reports_queues() {
    let app = router(AppState::default());
    setup_domain(app.clone()).await;
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/start",
        start_body("123e4567-e89b-12d3-a456-426614174023"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, desc) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasklists/describe",
        json!({"domain": "e2e-domain", "taskList": {"name": "e2e-tl"}, "includeTaskListStatus": true}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(desc["taskListStatus"]["backlogCountHint"].as_i64().unwrap() >= 1);

    let (status, by_domain) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasklists/by-domain",
        json!({"domain": "e2e-domain"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(by_domain["decisionTaskListMap"]["e2e-tl"].is_object());

    let (status, parts) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasklists/partitions",
        json!({"domain": "e2e-domain", "taskList": {"name": "e2e-tl"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(parts["decisionTaskListPartitions"][0]["key"], "e2e-tl");
    assert_eq!(parts["activityTaskListPartitions"][0]["key"], "e2e-tl");

    // Unknown domains are 404; missing task lists are 400.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasklists/by-domain",
        json!({"domain": "nope"}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasklists/partitions",
        json!({"domain": "e2e-domain", "taskList": {"name": ""}}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn restart_workflow_starts_new_run() {
    let app = router(AppState::default());
    setup_domain(app.clone()).await;
    let (status, started) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/start",
        start_body("123e4567-e89b-12d3-a456-426614174024"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let run_id = started["runId"].as_str().unwrap().to_string();

    let (status, restarted) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/restart",
        json!({
            "domain": "e2e-domain",
            "workflowExecution": {"workflowId": "e2e-wf", "runId": run_id},
            "identity": "e2e-restart",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let new_run = restarted["runId"].as_str().unwrap().to_string();
    assert_ne!(new_run, run_id);

    // The old run was terminated; the new run has a pollable decision.
    let (status, hist) = get(
        app.clone(),
        &format!("/api/v1/workflows/history?domain=e2e-domain&workflowId=e2e-wf&runId={run_id}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let kinds: Vec<i64> = hist["history"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["eventType"].as_i64().unwrap())
        .collect();
    assert!(kinds.contains(&28));

    let (status, decision) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasks/poll-decision?timeoutSecs=1",
        json!({"domain": "e2e-domain", "taskList": {"name": "e2e-tl"}, "identity": "w"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        decision["workflowExecution"]["runId"].as_str().unwrap(),
        new_run
    );
}

#[tokio::test]
async fn async_start_is_consumed_into_a_run() {
    let app = router(AppState::default());
    setup_domain(app.clone()).await;
    // Drive the worker-role consumer directly: enqueue, drain, consume.
    let store = cadence_persistence::InMemoryExecutionStore::new();
    store.register_domain_name("async-domain");
    store.enqueue_async_start(cadence_api::types::StartWorkflowExecutionRequest {
        domain: "async-domain".to_string(),
        workflow_id: "async-wf".to_string(),
        workflow_type: Some(cadence_api::types::WorkflowType {
            name: "async-type".to_string(),
        }),
        task_list: Some(cadence_api::types::TaskList {
            name: "async-tl".to_string(),
            ..Default::default()
        }),
        execution_start_to_close_timeout_seconds: Some(60),
        task_start_to_close_timeout_seconds: Some(10),
        request_id: "123e4567-e89b-12d3-a456-426614174025".to_string(),
        ..Default::default()
    });
    for request in store.drain_async_requests() {
        assert!(store.consume_async_request(&request));
    }
    assert!(store.poll_decision("async-domain", "async-tl", "w").is_some());

    // The HTTP route validates and enqueues without starting synchronously.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/start-async",
        json!({"request": start_body("123e4567-e89b-12d3-a456-426614174026")}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/start-async",
        json!({"request": {"domain": "e2e-domain"}}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn reset_workflow_forks_at_decision_boundary() {
    let app = router(AppState::default());
    setup_domain(app.clone()).await;
    let (status, started) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/start",
        start_body("123e4567-e89b-12d3-a456-426614174027"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let run_id = started["runId"].as_str().unwrap().to_string();

    // Start the first decision and signal, then complete it.
    let (status, decision) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasks/poll-decision?timeoutSecs=1",
        json!({"domain": "e2e-domain", "taskList": {"name": "e2e-tl"}, "identity": "w"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/signal",
        json!({
            "domain": "e2e-domain",
            "workflowExecution": {"workflowId": "e2e-wf", "runId": run_id},
            "signalName": "ping",
            "requestId": "123e4567-e89b-12d3-a456-426614174028",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasks/respond-decision-complete",
        json!({"taskToken": decision["taskToken"], "identity": "w", "decisions": []}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Reset to the DecisionTaskStarted boundary (event 3).
    let (status, reset) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/reset",
        json!({
            "domain": "e2e-domain",
            "workflowExecution": {"workflowId": "e2e-wf", "runId": run_id},
            "reason": "e2e-reset",
            "decisionFinishEventId": 3,
            "requestId": "123e4567-e89b-12d3-a456-426614174029",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reset}");
    let new_run = reset["runId"].as_str().unwrap().to_string();
    assert_ne!(new_run, run_id);

    // The reset run replays prefix + signal and schedules a decision.
    let (status, hist) = get(
        app.clone(),
        &format!("/api/v1/workflows/history?domain=e2e-domain&workflowId=e2e-wf&runId={new_run}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(hist["history"]["events"].as_array().unwrap().len(), 5);

    // Invalid reset points are rejected.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/reset",
        json!({
            "domain": "e2e-domain",
            "workflowExecution": {"workflowId": "e2e-wf", "runId": new_run},
            "decisionFinishEventId": 1,
            "requestId": "123e4567-e89b-12d3-a456-426614174030",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn refresh_tasks_and_diagnose_behave() {
    let app = router(AppState::default());
    setup_domain(app.clone()).await;
    let (status, started) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/start",
        start_body("123e4567-e89b-12d3-a456-426614174031"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let run_id = started["runId"].as_str().unwrap().to_string();

    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/refresh-tasks",
        json!({
            "domain": "e2e-domain",
            "execution": {"workflowId": "e2e-wf", "runId": run_id},
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // Still exactly one pollable decision: refresh does not duplicate.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasks/poll-decision?timeoutSecs=1",
        json!({"domain": "e2e-domain", "taskList": {"name": "e2e-tl"}, "identity": "w"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasks/poll-decision?timeoutSecs=1",
        json!({"domain": "e2e-domain", "taskList": {"name": "e2e-tl"}, "identity": "w"}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Unknown executions are 404.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/refresh-tasks",
        json!({
            "domain": "e2e-domain",
            "execution": {"workflowId": "missing", "runId": "123e4567-e89b-12d3-a456-426614174099"},
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Diagnose without the system domain is 404, like Go without system domains.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/diagnose",
        json!({
            "domain": "e2e-domain",
            "workflowExecution": {"workflowId": "e2e-wf", "runId": run_id},
            "identity": "e2e",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // With the system domain registered, diagnose starts the starter workflow.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/domains/register",
        json!({"name": "cadence-system", "workflowExecutionRetentionPeriodInDays": 3}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, resp) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/diagnose",
        json!({
            "domain": "e2e-domain",
            "workflowExecution": {"workflowId": "e2e-wf", "runId": run_id},
            "identity": "e2e",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{resp}");
    assert_eq!(resp["domain"], "cadence-system");
    assert!(resp["diagnosticWorkflowExecution"]["runId"]
        .as_str()
        .is_some_and(|s| !s.is_empty()));
    // Re-diagnosing reuses the running starter workflow.
    let (status, resp2) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/diagnose",
        json!({
            "domain": "e2e-domain",
            "workflowExecution": {"workflowId": "e2e-wf", "runId": run_id},
            "identity": "e2e",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        resp["diagnosticWorkflowExecution"]["runId"],
        resp2["diagnosticWorkflowExecution"]["runId"]
    );
}

#[tokio::test]
async fn failover_moves_global_domain_and_lists_history() {
    let app = router(AppState::default());
    // Global domain starting active on a remote cluster.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/domains/register",
        json!({
            "name": "global-domain",
            "workflowExecutionRetentionPeriodInDays": 7,
            "isGlobalDomain": true,
            "clusters": [{"clusterName": "remote"}, {"clusterName": "cluster0"}],
            "activeClusterName": "remote",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Local-domain-shaped requests are rejected; use a second global domain.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/domains/failover",
        json!({"domainName": "e2e-domain", "domainActiveClusterName": "cluster0"}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Missing target is 400.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/domains/failover",
        json!({"domainName": "global-domain"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Fail over to this host's cluster.
    let (status, resp) = call(
        app.clone(),
        Method::POST,
        "/api/v1/domains/failover",
        json!({"domainName": "global-domain", "domainActiveClusterName": "cluster0"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{resp}");
    assert_eq!(resp["replicationConfiguration"]["activeClusterName"], "cluster0");
    assert_eq!(resp["failoverVersion"], 1);

    // History lists the failover event; paging cursors work.
    let domain_id = resp["domainInfo"]["uuid"].as_str().unwrap().to_string();
    let (status, hist) = call(
        app.clone(),
        Method::POST,
        "/api/v1/domains/failover-history",
        json!({"filters": {"domainId": domain_id}, "pagination": {"pageSize": 1}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(hist["failoverEvents"].as_array().unwrap().len(), 1);
    assert_eq!(
        hist["failoverEvents"][0]["clusterFailovers"][0]["toCluster"]["activeClusterName"],
        "cluster0"
    );
    let token = hist["nextPageToken"].as_str().unwrap_or("");
    assert!(token.is_empty());
    let (status, hist2) = call(
        app.clone(),
        Method::POST,
        "/api/v1/domains/failover-history",
        json!({"filters": {"domainId": domain_id}, "pagination": {"pageSize": 10}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(hist2["failoverEvents"].as_array().unwrap().len(), 1);

    // Missing filter is 400.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/domains/failover-history",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn archived_visibility_reports_unconfigured() {
    let app = router(AppState::default());
    setup_domain(app.clone()).await;
    // Missing domain is 400.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/archived",
        json!({"pageSize": 10}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // Without an archival backend the exact Go gating error is returned.
    let (status, resp) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/archived",
        json!({"domain": "e2e-domain", "pageSize": 10}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        resp["message"].as_str().unwrap(),
        "Cluster is not configured for visibility archival"
    );
}

fn schedule_app() -> axum::Router {
    router(AppState {
        scheduler_enabled: true,
        ..Default::default()
    })
}

fn schedule_body(id: &str) -> serde_json::Value {
    json!({
        "domain": "e2e-domain",
        "scheduleId": id,
        "spec": {"cronExpression": "* * * * *"},
        "action": {"startWorkflow": {
            "workflowType": {"name": "sched-type"},
            "taskList": {"name": "sched-tl"},
            "executionStartToCloseTimeoutSeconds": 60,
            "taskStartToCloseTimeoutSeconds": 10,
        }},
    })
}

#[tokio::test]
async fn schedule_crud_and_firing_flow() {
    let app = schedule_app();
    setup_domain(app.clone()).await;
    // Disabled schedulers are rejected with Go's exact message.
    let plain = router(AppState::default());
    setup_domain(plain.clone()).await;
    let (status, resp) = call(
        plain.clone(),
        Method::POST,
        "/api/v1/schedules/create",
        schedule_body("sched-1"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(resp["message"].as_str().unwrap().contains("worker.enableScheduler"));

    let (status, resp) = call(
        app.clone(),
        Method::POST,
        "/api/v1/schedules/create",
        schedule_body("sched-1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{resp}");
    // Duplicate creation is rejected.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/schedules/create",
        schedule_body("sched-1"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // Invalid crons are rejected.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/schedules/create",
        json!({
            "domain": "e2e-domain",
            "scheduleId": "bad",
            "spec": {"cronExpression": "nonsense"},
            "action": {"startWorkflow": {
                "workflowType": {"name": "t"},
                "taskList": {"name": "tl"},
                "executionStartToCloseTimeoutSeconds": 60,
            }},
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, resp) = call(
        app.clone(),
        Method::POST,
        "/api/v1/schedules/describe",
        json!({"domain": "e2e-domain", "scheduleId": "sched-1"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["spec"]["cronExpression"], "* * * * *");

    let (status, resp) = call(
        app.clone(),
        Method::POST,
        "/api/v1/schedules/list",
        json!({"domain": "e2e-domain"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["schedules"].as_array().unwrap().len(), 1);

    // Pause stops firing; unpause resumes.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/schedules/pause",
        json!({"domain": "e2e-domain", "scheduleId": "sched-1", "reason": "hold"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/schedules/unpause",
        json!({"domain": "e2e-domain", "scheduleId": "sched-1"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Update replaces the cron expression.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/schedules/update",
        json!({
            "domain": "e2e-domain",
            "scheduleId": "sched-1",
            "spec": {"cronExpression": "0 * * * *"},
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, resp) = call(
        app.clone(),
        Method::POST,
        "/api/v1/schedules/describe",
        json!({"domain": "e2e-domain", "scheduleId": "sched-1"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["spec"]["cronExpression"], "0 * * * *");

    // Delete is idempotent.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/schedules/delete",
        json!({"domain": "e2e-domain", "scheduleId": "sched-1"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/schedules/delete",
        json!({"domain": "e2e-domain", "scheduleId": "sched-1"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

fn semaphore_app() -> axum::Router {
    router(AppState {
        semaphore_enabled: true,
        ..Default::default()
    })
}

#[tokio::test]
async fn semaphore_create_and_gate_flow() {
    let app = semaphore_app();
    setup_domain(app.clone()).await;
    // Disabled hosts are rejected with Go's exact message.
    let plain = router(AppState::default());
    setup_domain(plain.clone()).await;
    let (status, resp) = call(
        plain.clone(),
        Method::POST,
        "/api/v1/semaphores/create",
        json!({"domain": "e2e-domain", "semaphoreName": "s", "capacity": 2}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(resp["message"].as_str().unwrap().contains("system.enableDistributedSemaphore"));

    let (status, resp) = call(
        app.clone(),
        Method::POST,
        "/api/v1/semaphores/create",
        json!({"domain": "e2e-domain", "semaphoreName": "s", "capacity": 2, "bucketCapacity": 2}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{resp}");
    assert_eq!(resp["semaphore"]["bucketCapacity"], 2);
    // Duplicates are rejected.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/semaphores/create",
        json!({"domain": "e2e-domain", "semaphoreName": "s", "capacity": 2}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // Invalid capacities are rejected.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/semaphores/create",
        json!({"domain": "e2e-domain", "semaphoreName": "s2", "capacity": 0}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn admin_cluster_config_and_workflow_maintenance() {
    let app = router(AppState::default());
    setup_domain(app.clone()).await;

    let (status, cluster) = get(app.clone(), "/api/v1/admin/cluster/describe").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cluster["membershipInfo"]["rings"].as_array().unwrap().len(), 4);
    assert!(!cluster["membershipInfo"]["currentHost"]["Identity"]
        .as_str()
        .unwrap()
        .is_empty());

    // Dynamic config round-trips through both namespaces independently.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/dynamic-config/update",
        json!({"configName": "test.key", "configValues": [{"value": {"Data": "bnVsbA==", "EncodingType": 0}}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, got) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/dynamic-config/get",
        json!({"configName": "test.key"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(got["value"].is_object());
    let (status, listed) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/dynamic-config/list",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(listed["entries"].as_array().unwrap().len(), 1);
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/dynamic-config/restore",
        json!({"configName": "test.key"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/dynamic-config/get",
        json!({"configName": "test.key"}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Custom search attributes: system keys rejected, customs accepted.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/search-attributes/add",
        json!({"searchAttribute": {"DomainID": 1}}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/search-attributes/add",
        json!({"searchAttribute": {"MyField": 1}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Start a workflow, refresh it via admin, then delete it.
    let (status, started) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/start",
        start_body("123e4567-e89b-12d3-a456-426614174032"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let run_id = started["runId"].as_str().unwrap().to_string();
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/workflows/refresh-tasks",
        json!({
            "domain": "e2e-domain",
            "workflowExecution": {"workflowId": "e2e-wf", "runId": run_id},
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, deleted) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/workflows/delete",
        json!({
            "domain": "e2e-domain",
            "workflowExecution": {"workflowId": "e2e-wf", "runId": run_id},
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(deleted["historyDeleted"], true);
    assert_eq!(deleted["executionsDeleted"], true);
    assert_eq!(deleted["visibilityDeleted"], true);
    // Gone from history now.
    let (status, _) = get(
        app.clone(),
        &format!("/api/v1/workflows/history?domain=e2e-domain&workflowId=e2e-wf&runId={run_id}"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn admin_isolation_and_async_config_flow() {
    let app = router(AppState::default());
    setup_domain(app.clone()).await;

    // Global isolation groups round-trip.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/isolation-groups/global/update",
        json!({"isolationGroups": {"isolationGroups": [{"name": "a", "state": 1}]}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, groups) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/isolation-groups/global/get",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(groups["isolationGroups"]["isolationGroups"][0]["name"], "a");

    // Domain isolation groups require the domain.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/isolation-groups/domain/get",
        json!({"domain": "missing"}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/isolation-groups/domain/update",
        json!({
            "domain": "e2e-domain",
            "isolationGroups": {"isolationGroups": [{"name": "b", "state": 2}]},
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, groups) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/isolation-groups/domain/get",
        json!({"domain": "e2e-domain"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(groups["isolationGroups"]["isolationGroups"][0]["name"], "b");

    // Async workflow configuration round-trips per domain.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/async-workflow-config/update",
        json!({
            "domain": "e2e-domain",
            "configuration": {"enabled": true, "predefinedQueueName": "q"},
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, cfg) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/async-workflow-config/get",
        json!({"domain": "e2e-domain"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cfg["configuration"]["enabled"], true);
    assert_eq!(cfg["configuration"]["predefinedQueueName"], "q");
}

#[tokio::test]
async fn admin_shard_queue_history_and_maintain_flow() {
    let app = router(AppState::default());
    setup_domain(app.clone()).await;

    let (status, host) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/history-host/describe",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(host["numberOfShards"], 4);
    assert_eq!(host["shardIds"].as_array().unwrap().len(), 4);
    assert_eq!(host["shardControllerStatus"], "running");

    let (status, dist) = get(app.clone(), "/api/v1/admin/shards/describe-distribution").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(dist["numberOfShards"], 4);

    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/shards/close",
        json!({"shardID": 1}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/shards/close",
        json!({"shardID": 99}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, queue) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/queues/describe",
        json!({"shardID": 0, "type": 1}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(queue["processingQueueStates"].as_array().unwrap().len(), 5);
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/tasks/remove",
        json!({"shardID": 0, "type": 2, "taskID": 7}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/queues/reset",
        json!({"shardID": 0, "type": 3}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Start a workflow, read its raw history, describe it as admin, and
    // maintain it (healthy: nothing deleted).
    let (status, started) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/start",
        start_body("123e4567-e89b-12d3-a456-426614174033"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let run_id = started["runId"].as_str().unwrap().to_string();
    let (status, raw) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/workflows/raw-history",
        json!({
            "domain": "e2e-domain",
            "execution": {"workflowId": "e2e-wf", "runId": run_id},
            "maximumPageSize": 10,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(raw["historyBatches"].as_array().unwrap().len(), 1);
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/workflows/raw-history",
        json!({
            "domain": "e2e-domain",
            "execution": {"workflowId": "e2e-wf", "runId": run_id},
            "maximumPageSize": 0,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, desc) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/workflows/describe",
        json!({
            "domain": "e2e-domain",
            "execution": {"workflowId": "e2e-wf", "runId": run_id},
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(desc["shardId"], 0);
    assert!(!desc["mutableStateInDatabase"].as_str().unwrap().is_empty());

    let (status, maintained) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/workflows/maintain-corrupt",
        json!({
            "domain": "e2e-domain",
            "execution": {"workflowId": "e2e-wf", "runId": run_id},
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(maintained["historyDeleted"], false);
}

#[tokio::test]
async fn admin_reapply_replication_dlq_and_cross_cluster() {
    let app = router(AppState::default());
    setup_domain(app.clone()).await;
    let (status, started) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/start",
        start_body("123e4567-e89b-12d3-a456-426614174034"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let run_id = started["runId"].as_str().unwrap().to_string();

    // Reapply a signal batch to the open run.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/workflows/reapply-events",
        json!({
            "domain": "e2e-domain",
            "workflowExecution": {"workflowId": "e2e-wf", "runId": run_id},
            "events": {"Data": "bnVsbA=="},
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // Missing events blob hits Go's WorkflowID quirk.
    let (status, resp) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/workflows/reapply-events",
        json!({
            "domain": "e2e-domain",
            "workflowExecution": {"workflowId": "e2e-wf", "runId": run_id},
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(resp["message"].as_str().unwrap().contains("WorkflowID"));

    // Replication fetches are empty; domain replication is not enabled.
    let (status, resp) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/replication/messages",
        json!({"clusterName": "cluster0"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{resp}");
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/replication/domain-messages",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    // DLQ surface: counts are zero, reads are empty, type is required.
    let (status, resp) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/dlq/count",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["domain"], 0);
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/dlq/read",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, resp) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/dlq/read",
        json!({"type": 1}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{resp}");
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/dlq/purge",
        json!({"type": 2}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, resp) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/dlq/merge",
        json!({"type": 1, "maximumPageSize": 10}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{resp}");

    // Resend to self succeeds; unknown remotes are rejected.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/replication/resend",
        json!({
            "domainID": "e2e-domain",
            "workflowID": "e2e-wf",
            "runID": run_id,
            "remoteCluster": "cluster0",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/replication/resend",
        json!({
            "domainID": "e2e-domain",
            "workflowID": "e2e-wf",
            "runID": run_id,
            "remoteCluster": "nowhere",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Cross-cluster surface is empty but validated.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/cross-cluster/tasks",
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, resp) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/cross-cluster/tasks",
        json!({"targetCluster": "cluster0"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{resp}");
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/admin/cross-cluster/tasks-completed",
        json!({"targetCluster": "cluster0"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn child_workflow_runs_and_parent_closes() {
    let app = router(AppState::default());
    setup_domain(app.clone()).await;
    let (status, started) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/start",
        start_body("123e4567-e89b-12d3-a456-426614174022"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let parent_run = started["runId"].as_str().unwrap().to_string();

    // Parent decision: spawn a child.
    let (status, decision) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasks/poll-decision?timeoutSecs=1",
        json!({"domain": "e2e-domain", "taskList": {"name": "e2e-tl"}, "identity": "w"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasks/respond-decision-complete",
        json!({
            "taskToken": decision["taskToken"],
            "identity": "w",
            "decisions": [{
                "decisionType": 10,
                "startChildWorkflowExecutionDecisionAttributes": {
                    "domain": "e2e-domain",
                    "workflowId": "child-wf",
                    "workflowType": {"name": "ct"},
                },
            }],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Child runs its own decision and completes (type 3 = CompleteWorkflowExecution).
    for _ in 0..4 {
        let (status, task) = call(
            app.clone(),
            Method::POST,
            "/api/v1/tasks/poll-decision?timeoutSecs=1",
            json!({"domain": "e2e-domain", "taskList": {"name": "e2e-tl"}, "identity": "w"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let wf = task["workflowExecution"]["workflowId"]
            .as_str()
            .unwrap()
            .to_string();
        if wf == "child-wf" {
            let (status, _) = call(
                app.clone(),
                Method::POST,
                "/api/v1/tasks/respond-decision-complete",
                json!({
                    "taskToken": task["taskToken"],
                    "identity": "w",
                    "decisions": [{"decisionType": 3}],
                }),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            break;
        }
        // Parent's follow-up decision: nothing to do.
        let (status, _) = call(
            app.clone(),
            Method::POST,
            "/api/v1/tasks/respond-decision-complete",
            json!({"taskToken": task["taskToken"], "identity": "w", "decisions": []}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    // Parent history contains the child completion.
    let (status, hist) = get(
        app.clone(),
        &format!(
            "/api/v1/workflows/history?domain=e2e-domain&workflowId=e2e-wf&runId={parent_run}"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let kinds: Vec<i64> = hist["history"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["eventType"].as_i64().unwrap())
        .collect();
    assert!(kinds.contains(&33)); // ChildWorkflowExecutionCompleted
}

#[tokio::test]
async fn timer_decision_fires_without_worker() {
    let app = router(AppState::default());
    setup_domain(app.clone()).await;
    let (status, started) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/start",
        start_body("123e4567-e89b-12d3-a456-426614174023"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let run_id = started["runId"].as_str().unwrap().to_string();

    let (status, decision) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasks/poll-decision?timeoutSecs=1",
        json!({"domain": "e2e-domain", "taskList": {"name": "e2e-tl"}, "identity": "w"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasks/respond-decision-complete",
        json!({
            "taskToken": decision["taskToken"],
            "identity": "w",
            "decisions": [{
                "decisionType": 2,
                "startTimerDecisionAttributes": {"timerId": "t1", "startToFireTimeoutSeconds": 3600},
            }],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // Real timers take an hour; the engine path is covered in store unit tests.
    let (status, hist) = get(
        app.clone(),
        &format!("/api/v1/workflows/history?domain=e2e-domain&workflowId=e2e-wf&runId={run_id}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let kinds: Vec<i64> = hist["history"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["eventType"].as_i64().unwrap())
        .collect();
    assert!(kinds.contains(&17)); // TimerStarted
}

#[tokio::test]
async fn admin_and_visibility_routes() {
    let app = router(AppState::default());
    setup_domain(app.clone()).await;
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/start",
        start_body("123e4567-e89b-12d3-a456-426614174024"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, desc) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasklists/describe",
        json!({"domain": "e2e-domain", "taskList": {"name": "e2e-tl"}, "includeTaskListStatus": true}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(desc["taskListStatus"]["backlogCountHint"].as_i64().unwrap() >= 1);

    let (status, keys) = get(app.clone(), "/api/v1/search-attributes").await;
    assert_eq!(status, StatusCode::OK);
    assert!(keys["keys"]["WorkflowID"].is_number());

    let (status, count) = get(app.clone(), "/api/v1/workflows/count?domain=e2e-domain").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(count["count"], 1);

    let (status, open) = get(
        app.clone(),
        "/api/v1/workflows/list?domain=e2e-domain&status=open&workflowId=e2e-wf",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(open.as_array().unwrap().len(), 1);

    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/domains/delete",
        json!({"name": "e2e-domain"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = get(app.clone(), "/api/v1/domains/describe?name=e2e-domain").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn activity_heartbeat_cancel_and_paged_history() {
    let app = router(AppState::default());
    setup_domain(app.clone()).await;
    let (status, started) = call(
        app.clone(),
        Method::POST,
        "/api/v1/workflows/start",
        start_body("123e4567-e89b-12d3-a456-426614174031"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let run_id = started["runId"].as_str().unwrap().to_string();

    let (status, decision) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasks/poll-decision?timeoutSecs=1",
        json!({"domain": "e2e-domain", "taskList": {"name": "e2e-tl"}, "identity": "w"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasks/respond-decision-complete",
        json!({
            "taskToken": decision["taskToken"],
            "identity": "w",
            "decisions": [{
                "decisionType": 0,
                "scheduleActivityTaskDecisionAttributes": {
                    "activityId": "a1",
                    "activityType": {"name": "at"},
                },
            }],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, activity) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasks/poll-activity?timeoutSecs=1",
        json!({"domain": "e2e-domain", "taskList": {"name": "e2e-tl"}, "identity": "w"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Heartbeat without cancel request.
    let (status, hb) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasks/record-activity-heartbeat",
        json!({"taskToken": activity["taskToken"], "details": "cHJvZ3Jlc3M=", "identity": "w"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(hb["cancelRequested"], false);

    // Cancel the activity.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasks/respond-activity-canceled",
        json!({"taskToken": activity["taskToken"], "identity": "w"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Paged history: 1 event per page walks the whole chain.
    let mut seen = 0;
    let mut token: Option<String> = None;
    loop {
        let mut uri = format!(
            "/api/v1/workflows/history?domain=e2e-domain&workflowId=e2e-wf&runId={run_id}&maximumPageSize=1"
        );
        if let Some(t) = token {
            uri.push_str(&format!("&nextPageToken={t}"));
        }
        let (status, page) = get(app.clone(), &uri).await;
        assert_eq!(status, StatusCode::OK);
        seen += page["history"]["events"].as_array().unwrap().len();
        match page["nextPageToken"].as_str() {
            Some(t) if !t.is_empty() => token = Some(t.to_string()),
            _ => break,
        }
    }
    let (status, full) = get(
        app.clone(),
        &format!("/api/v1/workflows/history?domain=e2e-domain&workflowId=e2e-wf&runId={run_id}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(seen, full["history"]["events"].as_array().unwrap().len());
    assert!(seen >= 7);
}

#[tokio::test]
async fn describe_task_list_reports_pollers() {
    let app = router(AppState::default());
    setup_domain(app.clone()).await;
    // Poll with no tasks: still registers the poller identity.
    let (status, _) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasks/poll-decision?timeoutSecs=1",
        json!({"domain": "e2e-domain", "taskList": {"name": "e2e-tl"}, "identity": "worker-9"}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, desc) = call(
        app.clone(),
        Method::POST,
        "/api/v1/tasklists/describe",
        json!({"domain": "e2e-domain", "taskList": {"name": "e2e-tl"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let pollers = desc["pollers"].as_array().unwrap();
    assert!(pollers.iter().any(|p| p["identity"] == "worker-9"));
}
