//! NDC state rebuild sequencing from Go `execution/state_rebuilder.go`.
use cadence_api::service_types::HistoryEvent;
use super::context::WorkflowExecutionKey;
pub const NDC_DEFAULT_PAGE_SIZE: usize = 100;

pub struct HistoryBatchPage { pub batches: Vec<Vec<HistoryEvent>>, pub next_token: Vec<u8>, pub size: i64 }
/// Persistence/domain/context operations needed while replaying a historical
/// branch. `apply_events` supplies no new-run history, matching Go rebuilds.
pub trait RebuildContext {
    type State;
    type Error;
    fn load_domain(&mut self, domain_id: &str) -> Result<(), Self::Error>;
    fn initialize_builders(&mut self, domain_id: &str) -> Self::State;
    fn page(&mut self, branch: &[u8], first: i64, next: i64, token: Vec<u8>, domain_id: &str, page_size: usize) -> Result<HistoryBatchPage, Self::Error>;
    fn apply_events(&mut self, state: &mut Self::State, target: &WorkflowExecutionKey, events: &[HistoryEvent], request_id: &str) -> Result<(), Self::Error>;
    fn last_version_history_item(&self, state: &Self::State) -> Result<Option<(i64, i64)>, Self::Error>;
    fn set_current_branch_token(&mut self, state: &mut Self::State, branch: Vec<u8>) -> Result<(), Self::Error>;
    fn close_passive_snapshot(&mut self, state: &mut Self::State, now: i64) -> Result<(), Self::Error>;
    fn refresh_tasks(&mut self, state: &mut Self::State, now: i64) -> Result<(), Self::Error>;
    fn set_start_timestamp(&mut self, state: &mut Self::State, now: i64);
    fn empty_history_error(&self) -> Self::Error;
    fn bad_request(&self, message: String) -> Self::Error;
}
pub struct RebuildRequest<'a> {
    pub now: i64,
    pub base_branch_token: &'a [u8],
    pub base_last_event_id: i64,
    pub base_last_event_version: i64,
    pub target: &'a WorkflowExecutionKey,
    pub request_id: &'a str,
}
pub struct StateRebuilder<C> { pub context: C, pub rebuilt_history_size: i64 }
impl<C: RebuildContext> StateRebuilder<C> {
    pub fn new(context: C) -> Self { Self { context, rebuilt_history_size: 0 } }
    pub fn rebuild(&mut self, request: RebuildRequest<'_>, target_branch: impl FnOnce() -> Result<Vec<u8>, C::Error>) -> Result<(C::State, i64), C::Error> {
        self.context.load_domain(&request.target.domain_id)?;
        let mut token = Vec::new();
        let mut state = None;
        loop {
            let page = self.context.page(request.base_branch_token, 1, request.base_last_event_id + 1, token, &request.target.domain_id, NDC_DEFAULT_PAGE_SIZE)?;
            self.rebuilt_history_size += page.size;
            for batch in page.batches {
                let state = state.get_or_insert_with(|| self.context.initialize_builders(&request.target.domain_id));
                self.context.apply_events(state, request.target, &batch, request.request_id)?;
            }
            if page.next_token.is_empty() { break; }
            token = page.next_token;
        }
        let mut state = state.ok_or_else(|| self.context.empty_history_error())?;
        if let Some((id, version)) = self.context.last_version_history_item(&state)? {
            if (id, version) != (request.base_last_event_id, request.base_last_event_version) {
                return Err(self.context.bad_request(format!("nDCStateRebuilder unable to rebuild mutable state to event ID: {}, version: {}, baseLastEventID + baseLastEventVersion is not the same as the last event of the last batch, event ID: {id}, version :{version} ,typically because of attemptting to rebuild to a middle of a batch", request.base_last_event_id, request.base_last_event_version)));
            }
        }
        self.context.set_current_branch_token(&mut state, target_branch()?)?;
        self.context.close_passive_snapshot(&mut state, request.now)?;
        self.context.refresh_tasks(&mut state, request.now)?;
        self.context.set_start_timestamp(&mut state, request.now);
        Ok((state, self.rebuilt_history_size))
    }
}
