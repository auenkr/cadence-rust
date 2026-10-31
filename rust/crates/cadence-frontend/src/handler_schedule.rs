//! Schedule RPC validation and identifiers ported from
//! `go/service/frontend/api/handler_schedule.go`.

use cadence_api::{
    errors::BadRequestError,
    schedule::CreateScheduleRequest,
    validation,
};

const WORKFLOW_ID_PREFIX: &str = "cadence-scheduler:";
pub const DEFAULT_LIST_SCHEDULES_PAGE_SIZE: i32 = 10;

/// Stable scheduler workflow ID used by Go to address one schedule.
#[must_use]
pub fn scheduler_workflow_id(schedule_id: &str) -> String {
    format!("{WORKFLOW_ID_PREFIX}{schedule_id}")
}

/// Validate a create request in Go's order. `scheduler_enabled` is evaluated
/// for the request domain by the live dynamic-config property.
pub fn validate_create_request(
    request: &CreateScheduleRequest,
    scheduler_enabled: bool,
) -> Result<(), BadRequestError> {
    if request.domain.is_empty() {
        return Err(validation::err_domain_not_set());
    }
    if !scheduler_enabled {
        return Err(BadRequestError::new(format!(
            "Schedules are not enabled for domain {:?}. Set dynamic config worker.enableScheduler=true for this domain to enable them.",
            request.domain
        )));
    }
    if request.schedule_id.is_empty() {
        return Err(BadRequestError::new("ScheduleID is not set on request."));
    }
    let spec = request
        .spec
        .as_ref()
        .ok_or_else(|| BadRequestError::new("Spec is not set on request."))?;
    if spec.cron_expression.is_empty() {
        return Err(BadRequestError::new("CronExpression is not set on request."));
    }
    let now = cadence_common::cron::naive_from_nanos(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0),
    );
    cadence_common::cron::validate_schedule(&spec.cron_expression, now)
        .map_err(BadRequestError::new)?;
    validation::validate_schedule_spec(Some(spec))?;
    validation::validate_schedule_action_complete(request.action.as_ref())?;
    validation::validate_schedule_policies(request.policies.as_ref())?;
    validation::validate_schedule_search_attributes(request.search_attributes.as_ref())
}

/// Go uses ten when ListSchedules receives zero and passes positive sizes
/// through to the scheduler. Negative values remain unchanged for the caller's
/// validation/error mapping.
#[must_use]
pub fn effective_list_page_size(page_size: i32) -> i32 {
    if page_size == 0 {
        DEFAULT_LIST_SCHEDULES_PAGE_SIZE
    } else {
        page_size
    }
}

/// Ongoing backfill output is omitted when the scheduler has none.
#[must_use]
pub fn ongoing_backfills_for_response(
    backfills: &[cadence_api::schedule::BackfillInfo],
) -> Option<Vec<cadence_api::schedule::BackfillInfo>> {
    (!backfills.is_empty()).then(|| backfills.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_validation_matches_go_first_error_order() {
        let mut request = CreateScheduleRequest::default();
        assert_eq!(validate_create_request(&request, false).unwrap_err().message,
            "Domain not set on request.");
        request.domain = "payments".into();
        assert!(validate_create_request(&request, false).unwrap_err().message.contains("worker.enableScheduler=true"));
        assert_eq!(validate_create_request(&request, true).unwrap_err().message,
            "ScheduleID is not set on request.");
    }

    #[test]
    fn scheduler_id_page_size_and_backfill_presence_match_go() {
        assert_eq!(scheduler_workflow_id("daily"), "cadence-scheduler:daily");
        assert_eq!(effective_list_page_size(0), 10);
        assert_eq!(effective_list_page_size(-1), -1);
        assert_eq!(ongoing_backfills_for_response(&[]), None);
    }

    #[test]
    fn complete_valid_schedule_passes() {
        let request: CreateScheduleRequest = serde_json::from_str(
            r#"{"domain":"payments","scheduleId":"daily","spec":{"cronExpression":"0 0 * * *"},"action":{"startWorkflow":{"workflowType":{"name":"settle"},"taskList":{"name":"payments"},"executionStartToCloseTimeoutSeconds":60}}}"#,
        ).unwrap();
        assert!(validate_create_request(&request, true).is_ok());
    }
}
