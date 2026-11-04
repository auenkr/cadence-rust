//! Port of engineimpl/{record_decision_task_started,respond_decision_task_completed,respond_decision_task_failed}.go.
use super::{deps::*, runtime::*, types::*};

impl HistoryEngine {
    /// `RecordDecisionTaskStarted`
    pub async fn record_decision_task_started(&self, request: &RecordDecisionTaskStartedRequest) -> HResult<RecordDecisionTaskStartedResponse> {
        self.decision_handler.handle_decision_task_started(request).await
    }
    /// `RespondDecisionTaskCompleted`
    pub async fn respond_decision_task_completed(&self, req: &HistoryRespondDecisionTaskCompletedRequest) -> HResult<HistoryRespondDecisionTaskCompletedResponse> {
        self.decision_handler.handle_decision_task_completed(req).await
    }
    /// `RespondDecisionTaskFailed`
    pub async fn respond_decision_task_failed(&self, req: &HistoryRespondDecisionTaskFailedRequest) -> HResult<()> {
        self.decision_handler.handle_decision_task_failed(req).await
    }
}
