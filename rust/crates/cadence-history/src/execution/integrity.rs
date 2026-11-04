//! History integrity scans from Go `execution/integrity.go`.
//!
//! The page source must read from event 1, preserving the persistence history
//! reader's transaction-chain validation. Errors are propagated without returning
//! a partial result.
use std::collections::BTreeSet;
use cadence_api::{enums::EventType, service_types::HistoryEvent};

pub trait HistoryPageSource {
    type Error;
    fn page(&mut self, first_event_id: i64, next_event_id: i64, token: Vec<u8>) -> Result<(Vec<HistoryEvent>, Vec<u8>), Self::Error>;
}

pub fn get_resurrected_timers<S: HistoryPageSource>(source: &mut S, pending: &BTreeSet<String>, next_event_id: i64) -> Result<BTreeSet<String>, S::Error> {
    let mut found = BTreeSet::new();
    if pending.is_empty() { return Ok(found); }
    scan(source, next_event_id, |event| {
        let id = match event.event_type {
            Some(EventType::TimerFired) => event.timer_fired_event_attributes.as_ref().map(|a| &a.timer_id),
            Some(EventType::TimerCanceled) => event.timer_canceled_event_attributes.as_ref().map(|a| &a.timer_id),
            _ => None,
        };
        if let Some(id) = id.filter(|id| !id.is_empty() && pending.contains(*id)) { found.insert(id.clone()); }
    })?;
    Ok(found)
}

pub fn get_resurrected_activities<S: HistoryPageSource>(source: &mut S, pending: &BTreeSet<i64>, next_event_id: i64) -> Result<BTreeSet<i64>, S::Error> {
    let mut found = BTreeSet::new();
    if pending.is_empty() { return Ok(found); }
    scan(source, next_event_id, |event| {
        let id = match event.event_type {
            Some(EventType::ActivityTaskCompleted) => event.activity_task_completed_event_attributes.as_ref().map(|a| a.scheduled_event_id),
            Some(EventType::ActivityTaskFailed) => event.activity_task_failed_event_attributes.as_ref().map(|a| a.scheduled_event_id),
            Some(EventType::ActivityTaskTimedOut) => event.activity_task_timed_out_event_attributes.as_ref().map(|a| a.scheduled_event_id),
            Some(EventType::ActivityTaskCanceled) => event.activity_task_canceled_event_attributes.as_ref().map(|a| a.scheduled_event_id),
            _ => None,
        };
        if let Some(id) = id.filter(|id| *id != 0 && pending.contains(id)) { found.insert(id); }
    })?;
    Ok(found)
}

fn scan<S: HistoryPageSource>(source: &mut S, next_event_id: i64, mut visit: impl FnMut(&HistoryEvent)) -> Result<(), S::Error> {
    let mut token = Vec::new();
    loop {
        let (events, next_token) = source.page(1, next_event_id, token)?;
        for event in &events { visit(event); }
        if next_token.is_empty() { return Ok(()); }
        token = next_token;
    }
}
