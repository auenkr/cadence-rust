//! Port of `go/service/history/queue/action.go`.
use super::deps::{CrossClusterTaskRequest, CrossClusterTaskResponse};
use super::processing_queue_state::ProcessingQueueState;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActionType {
    Reset = 1,
    GetState,
    GetTasks,
    UpdateTask,
}

#[derive(Clone, Debug)]
pub struct Action {
    pub action_type: ActionType,
    pub reset_action_attributes: Option<ResetActionAttributes>,
    pub get_state_action_attributes: Option<GetStateActionAttributes>,
    pub get_tasks_attributes: Option<GetTasksAttributes>,
    pub update_task_attributes: Option<UpdateTasksAttributes>,
}

#[derive(Clone, Debug, Default)]
pub struct ActionResult {
    pub action_type: Option<ActionType>,
    pub reset_action_result: Option<ResetActionResult>,
    pub get_state_action_result: Option<GetStateActionResult>,
    pub get_tasks_result: Option<GetTasksResult>,
    pub update_task_result: Option<UpdateTasksResult>,
}

#[derive(Clone, Debug, Default)]
pub struct ResetActionAttributes;
#[derive(Clone, Debug, Default)]
pub struct ResetActionResult;
#[derive(Clone, Debug, Default)]
pub struct GetStateActionAttributes;
#[derive(Clone, Debug, Default)]
pub struct GetStateActionResult {
    pub states: Vec<ProcessingQueueState>,
}
#[derive(Clone, Debug, Default)]
pub struct GetTasksAttributes;
#[derive(Clone, Debug, Default)]
pub struct GetTasksResult {
    pub task_requests: Vec<CrossClusterTaskRequest>,
}
#[derive(Clone, Debug, Default)]
pub struct UpdateTasksAttributes {
    pub task_responses: Vec<CrossClusterTaskResponse>,
}
#[derive(Clone, Debug, Default)]
pub struct UpdateTasksResult;

impl Action {
    fn base(t: ActionType) -> Action {
        Action {
            action_type: t,
            reset_action_attributes: None,
            get_state_action_attributes: None,
            get_tasks_attributes: None,
            update_task_attributes: None,
        }
    }
    pub fn new_reset_action() -> Action {
        Action { reset_action_attributes: Some(ResetActionAttributes), ..Self::base(ActionType::Reset) }
    }
    pub fn new_get_state_action() -> Action {
        Action {
            get_state_action_attributes: Some(GetStateActionAttributes),
            ..Self::base(ActionType::GetState)
        }
    }
    pub fn new_get_tasks_action() -> Action {
        Action { get_tasks_attributes: Some(GetTasksAttributes), ..Self::base(ActionType::GetTasks) }
    }
    pub fn new_update_tasks_action(task_responses: Vec<CrossClusterTaskResponse>) -> Action {
        Action {
            update_task_attributes: Some(UpdateTasksAttributes { task_responses }),
            ..Self::base(ActionType::UpdateTask)
        }
    }
}
