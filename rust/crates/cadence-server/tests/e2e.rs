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
        vec!["0", "4", "5", "6", "9", "4", "10", "11", "5", "6", "1"]
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
