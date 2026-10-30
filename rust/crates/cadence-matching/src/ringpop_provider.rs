//! Bounded inbound Ringpop v0.10.0 JOIN and direct PING state.
//!
//! The node owns accepted membership changes, SWIM probing, and state timers;
//! bootstrap and readiness remain controlled by its lifecycle owner.
use cadence_common::ringpop_bootstrap_selection::BootstrapSelection;
use cadence_common::ringpop_disseminator::Disseminator;
use cadence_common::ringpop_member::should_process_gossip;
use cadence_common::ringpop_memberlist::Memberlist;
use cadence_common::ringpop_memberlist::MemberlistIter;
use cadence_common::ringpop_state_transitions::{
    StateTimeouts, StateTransitionScheduler, TransitionAction, TransitionState,
};
use cadence_common::ringpop_wire::{
    Change, JoinRequest, JoinResponse, Member, Ping, PingRequest, PingResponse, Timestamp, ALIVE,
    FAULTY, LEAVE, SUSPECT, TOMBSTONE,
};
use rand::seq::SliceRandom;
use serde::{de::DeserializeOwned, Deserialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::{future::Future, pin::Pin};
use tokio::sync::{mpsc, watch, Notify};

use crate::ringpop_tchannel::{RingpopError, RingpopHandler, RingpopResponse};
use crate::tchannel_json_client::{self, JsonCallError};
use std::time::Duration;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RingpopRequestCounts {
    pub join: u64,
    pub ping: u64,
    pub ping_req: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize)]
pub struct JoinGroupResult {
    /// One-based count of JoinGroup calls on this sender, including empty groups.
    pub attempt: i64,
    pub successes: Vec<String>,
    pub failures: Vec<String>,
}

/// One completed indirect probe result, corresponding to Go's `interface{}`
/// values emitted by `sendPingRequests`.
#[derive(Debug)]
pub enum PingRequestResult {
    Response(PingResponse),
    Error(JsonCallError),
}

/// Mutable state owned by one bootstrap sender across JOIN retry attempts.
pub struct JoinGroupState {
    pub selection: BootstrapSelection,
    attempts: i64,
    delayer: ExponentialJoinDelayer,
}

/// Injectable time and delay boundary for the Go JoinCluster retry loop.
pub trait JoinClock {
    fn reset(&mut self);
    fn elapsed(&mut self) -> Duration;
    fn sleep_nanos<'a>(
        &'a mut self,
        duration_nanos: i64,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
    fn random_below(&mut self, upper_exclusive: u64) -> u64;
}

pub struct SystemJoinClock(std::time::Instant);

impl Default for SystemJoinClock {
    fn default() -> Self {
        Self(std::time::Instant::now())
    }
}

impl JoinClock for SystemJoinClock {
    fn reset(&mut self) {
        self.0 = std::time::Instant::now();
    }

    fn elapsed(&mut self) -> Duration {
        self.0.elapsed()
    }

    fn sleep_nanos<'a>(
        &'a mut self,
        duration_nanos: i64,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(tokio::time::sleep(if duration_nanos > 0 {
            Duration::from_nanos(duration_nanos as u64)
        } else {
            Duration::ZERO
        }))
    }

    fn random_below(&mut self, upper_exclusive: u64) -> u64 {
        rand::Rng::gen_range(&mut rand::thread_rng(), 0..upper_exclusive)
    }
}

/// Ringpop's exponential JOIN delay, including its shifting jitter window.
/// Go converts durations to whole milliseconds before calculating the window.
pub struct ExponentialJoinDelayer {
    initial_ms: i64,
    max_ms: i64,
    next_min_ms: i64,
    num_delays: u32,
    max_reached: bool,
}

impl Default for ExponentialJoinDelayer {
    fn default() -> Self {
        Self::new(Duration::from_millis(100), Duration::from_secs(60))
    }
}

impl ExponentialJoinDelayer {
    pub fn new(initial: Duration, max: Duration) -> Self {
        Self::new_signed(
            initial.as_nanos().min(i64::MAX as u128) as i64,
            max.as_nanos().min(i64::MAX as u128) as i64,
        )
    }

    pub fn new_signed(initial_nanos: i64, max_nanos: i64) -> Self {
        Self {
            initial_ms: initial_nanos / 1_000_000,
            max_ms: max_nanos / 1_000_000,
            next_min_ms: 0,
            num_delays: 0,
            max_reached: false,
        }
    }

    pub fn delay_nanos(&mut self, clock: &mut impl JoinClock) -> i64 {
        let uncapped = (self.initial_ms as f64) * 2f64.powf(self.num_delays as f64);
        let capped = (self.max_ms as f64).min(uncapped);
        let upper = (capped - self.next_min_ms as f64) as i64;
        let jittered = if capped == self.next_min_ms as f64 {
            capped as i64
        } else if upper > 0 {
            (clock.random_below(upper as u64) as i64).wrapping_add(self.next_min_ms)
        } else {
            // Go's Intn panics for a nonpositive bound. Keep that option edge
            // observable instead of silently normalizing invalid durations.
            panic!("invalid JOIN delay random bound: {upper}")
        };
        self.max_reached |= uncapped >= self.max_ms as f64;
        self.next_min_ms = capped as i64;
        let duration = jittered.wrapping_mul(1_000_000);
        self.num_delays = self.num_delays.wrapping_add(1);
        duration
    }

    pub fn max_delay_reached(&self) -> bool {
        self.max_reached
    }
}

/// The error and partial successes returned by Go's `JoinCluster` on failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinClusterError(pub String, pub Vec<String>);

fn go_duration_string(nanos: i64) -> String {
    let sign = if nanos < 0 { "-" } else { "" };
    let magnitude = nanos.unsigned_abs();
    if magnitude == 0 {
        return "0s".to_owned();
    }
    if magnitude >= 60_000_000_000 {
        let total_seconds = magnitude / 1_000_000_000;
        let hours = total_seconds / 3_600;
        let minutes = (total_seconds % 3_600) / 60;
        let seconds = total_seconds % 60;
        let fractional = magnitude % 1_000_000_000;
        let second_text = if fractional == 0 {
            format!("{seconds}s")
        } else {
            let fraction = format!("{fractional:09}").trim_end_matches('0').to_owned();
            format!("{seconds}.{fraction}s")
        };
        return if hours > 0 {
            format!("{sign}{hours}h{minutes}m{second_text}")
        } else {
            format!("{sign}{minutes}m{second_text}")
        };
    }
    let (unit, scale) = if magnitude >= 1_000_000_000 {
        ("s", 1_000_000_000)
    } else if magnitude >= 1_000_000 {
        ("ms", 1_000_000)
    } else if magnitude >= 1_000 {
        ("µs", 1_000)
    } else {
        return format!("{sign}{magnitude}ns");
    };
    let whole = magnitude / scale;
    let fraction = magnitude % scale;
    if fraction == 0 {
        format!("{sign}{whole}{unit}")
    } else {
        let digits = match scale {
            1_000_000_000 => format!("{fraction:09}").trim_end_matches('0').to_owned(),
            1_000_000 => format!("{fraction:06}"),
            _ => format!("{fraction:03}"),
        };
        format!("{sign}{whole}.{}{}", digits.trim_end_matches('0'), unit)
    }
}

/// Drive complete JoinGroup attempts until the Go success threshold or the
/// post-group strict deadline check is reached. `clock` controls elapsed time
/// and retry sleep; dropping this future cancels the current group naturally.
pub async fn join_cluster(
    node: &RingpopNode,
    sender: &mut JoinGroupState,
    clock: &mut impl JoinClock,
) -> Result<Vec<String>, JoinClusterError> {
    clock.reset();
    let mut joined = Vec::new();
    if sender.selection.single_node {
        return Ok(joined);
    }
    let max_nanos = sender.selection.options.max_join_duration_nanos;
    loop {
        if node.is_destroyed() {
            return Err(JoinClusterError(
                "node destroyed while attempting to join cluster".to_owned(),
                Vec::new(),
            ));
        }
        let group = node.join_group(sender, &joined).await;
        joined.extend(group.successes);
        if i64::try_from(joined.len()).unwrap_or(i64::MAX) >= sender.selection.effective_size {
            return Ok(joined);
        }
        let elapsed = clock.elapsed();
        let elapsed_nanos = elapsed.as_nanos().min(i64::MAX as u128) as i64;
        if elapsed_nanos > max_nanos {
            return Err(JoinClusterError(
                format!(
                    "join duration of {} exceeded max {}",
                    go_duration_string(elapsed_nanos),
                    go_duration_string(max_nanos)
                ),
                joined,
            ));
        }
        // Delayer belongs to the sender across loop iterations.
        let delay = sender.delayer.delay_nanos(clock);
        clock.sleep_nanos(delay).await;
    }
}

impl JoinGroupState {
    pub fn new(selection: BootstrapSelection) -> Self {
        Self {
            selection,
            attempts: 0,
            delayer: ExponentialJoinDelayer::default(),
        }
    }

    pub fn attempts(&self) -> i64 {
        self.attempts
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RingpopLifecycle {
    Bootstrapping,
    Ready,
    Stopped,
}

#[derive(Clone)]
pub struct RingpopNode {
    state: Arc<Mutex<State>>,
    runtime: Arc<Mutex<RuntimeControl>>,
}

#[derive(Default)]
struct RuntimeControl {
    generation: Arc<AtomicU64>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    stop_tx: Option<watch::Sender<bool>>,
    transition_stop_tx: Option<watch::Sender<bool>>,
    stop_done: Option<watch::Sender<bool>>,
    stopping: bool,
    executor: Option<tokio::runtime::Handle>,
    self_evicted: bool,
    reverse_jobs: Arc<AtomicUsize>,
    reverse_tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for RuntimeControl {
    fn drop(&mut self) {
        for task in self.tasks.drain(..).chain(self.reverse_tasks.drain(..)) {
            task.abort();
        }
    }
}

struct State {
    app: String,
    address: String,
    ready: bool,
    stopped: bool,
    destroyed: bool,
    requires_app_in_ping: bool,
    ping_timeout: Duration,
    ping_request_timeout: Duration,
    ping_request_size: usize,
    transition_started_at: tokio::time::Instant,
    min_protocol_period: Duration,
    protocol_metrics: ProtocolMetrics,
    transition_notify: Arc<Notify>,
    membership_changes: watch::Sender<u64>,
    memberlist: Memberlist,
    member_iterator: MemberlistIter,
    state_transitions: StateTransitionScheduler,
    disseminator: Disseminator,
    counts: RingpopRequestCounts,
}

/// Go gossip's `metrics.NewUniformSample(10)` and adaptive period state.
#[derive(Debug)]
struct ProtocolMetrics {
    count: u64,
    samples_nanos: Vec<u64>,
    last_period: Option<tokio::time::Instant>,
    last_rate: Duration,
}

impl ProtocolMetrics {
    fn new(min_period: Duration) -> Self {
        Self {
            count: 1,
            samples_nanos: vec![duration_nanos_u64(min_period)],
            last_period: None,
            last_rate: min_period,
        }
    }

    fn record(&mut self, period_end: tokio::time::Instant, duration: Duration) {
        self.last_period = Some(period_end);
        self.count = self.count.saturating_add(1);
        let value = duration_nanos_u64(duration);
        if self.samples_nanos.len() < 10 {
            self.samples_nanos.push(value);
        } else {
            let index = rand::Rng::gen_range(&mut rand::thread_rng(), 0..self.count);
            if index < self.samples_nanos.len() as u64 {
                self.samples_nanos[index as usize] = value;
            }
        }
    }

    fn adjust_rate(&mut self, min_period: Duration) {
        let mut ordered = self.samples_nanos.clone();
        ordered.sort_unstable();
        if ordered.is_empty() {
            self.last_rate = min_period;
            return;
        }
        // go-metrics SamplePercentiles uses p*(n+1) and interpolates adjacent
        // samples; p50 is doubled and floored at minProtocolPeriod.
        let position = ordered.len() as f64 * 0.5 + 0.5;
        let percentile = if position < 1.0 {
            ordered[0] as f64
        } else if position >= ordered.len() as f64 {
            *ordered.last().expect("nonempty timing sample") as f64
        } else {
            let lower_index = position as usize - 1;
            let lower = ordered[lower_index] as f64;
            let upper = ordered[lower_index + 1] as f64;
            lower + (position - position.floor()) * (upper - lower)
        };
        let twice_p50 = (percentile * 2.0).min(u64::MAX as f64) as u64;
        self.last_rate = min_period.max(Duration::from_nanos(twice_p50));
    }
}

fn duration_nanos_u64(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

fn next_protocol_delay(
    min_period: Duration,
    last_period: tokio::time::Instant,
    last_rate: Duration,
    now: tokio::time::Instant,
) -> Duration {
    min_period.max(
        last_period
            .checked_add(last_rate)
            .unwrap_or(last_period)
            .saturating_duration_since(now),
    )
}

impl State {
    fn monotonic_now_ns(&self) -> i64 {
        self.transition_started_at
            .elapsed()
            .as_nanos()
            .min(i64::MAX as u128) as i64
    }

    /// The Rust equivalent of Go's Memberlist.Update plus Node.handleChanges.
    /// All accepted updates flow through this method so timer ownership and
    /// dissemination stay in step with membership state.
    fn apply_changes<F, R>(
        &mut self,
        changes: &[Change],
        next_incarnation: F,
        insert_position: R,
    ) -> Vec<Change>
    where
        F: FnMut() -> i64,
        R: FnMut(usize) -> usize,
    {
        if self.stopped || changes.is_empty() {
            return Vec::new();
        }
        let update = self
            .memberlist
            .update(changes, next_incarnation, insert_position);
        if update.accepted.is_empty() {
            return Vec::new();
        }
        self.membership_changes
            .send_modify(|generation| *generation = generation.wrapping_add(1));
        let pingable = self
            .memberlist
            .members()
            .iter()
            .filter(|member| {
                member.address != self.address
                    && (member.status == ALIVE || member.status == SUSPECT)
            })
            .count();
        self.disseminator.adjust_max_propagations(pingable);
        let now_ns = self.monotonic_now_ns();
        let accepted = update.accepted;
        for change in &accepted {
            self.disseminator.record(change.clone());
            match change.status.as_str() {
                ALIVE | LEAVE => {
                    self.state_transitions.cancel(&change.address);
                }
                SUSPECT => {
                    self.state_transitions.schedule(
                        change.address.clone(),
                        change.incarnation,
                        TransitionState::Suspect,
                        now_ns,
                    );
                }
                FAULTY => {
                    self.state_transitions.schedule(
                        change.address.clone(),
                        change.incarnation,
                        TransitionState::Faulty,
                        now_ns,
                    );
                }
                TOMBSTONE => {
                    self.state_transitions.schedule(
                        change.address.clone(),
                        change.incarnation,
                        TransitionState::Tombstone,
                        now_ns,
                    );
                }
                _ => {}
            }
        }
        self.transition_notify.notify_one();
        accepted
    }

    fn record_local_change(&mut self, change: Change) {
        self.membership_changes
            .send_modify(|generation| *generation = generation.wrapping_add(1));
        let pingable = self
            .memberlist
            .members()
            .iter()
            .filter(|member| {
                member.address != self.address
                    && (member.status == ALIVE || member.status == SUSPECT)
            })
            .count();
        self.disseminator.adjust_max_propagations(pingable);
        self.disseminator.record(change.clone());
        match change.status.as_str() {
            ALIVE | LEAVE => {
                self.state_transitions.cancel(&change.address);
            }
            SUSPECT => {
                self.state_transitions.schedule(
                    change.address,
                    change.incarnation,
                    TransitionState::Suspect,
                    self.monotonic_now_ns(),
                );
            }
            FAULTY => {
                self.state_transitions.schedule(
                    change.address,
                    change.incarnation,
                    TransitionState::Faulty,
                    self.monotonic_now_ns(),
                );
            }
            TOMBSTONE => {
                self.state_transitions.schedule(
                    change.address,
                    change.incarnation,
                    TransitionState::Tombstone,
                    self.monotonic_now_ns(),
                );
            }
            _ => {}
        }
        self.transition_notify.notify_one();
    }

    fn apply_due_transitions<F, R>(
        &mut self,
        now_ns: i64,
        mut next_incarnation: F,
        mut insert_position: R,
    ) where
        F: FnMut() -> i64,
        R: FnMut(usize) -> usize,
    {
        let due = self.state_transitions.advance(now_ns);
        for action in due {
            match action.action {
                TransitionAction::Evict => {
                    if self.memberlist.remove_member(&action.address) {
                        self.membership_changes
                            .send_modify(|generation| *generation = generation.wrapping_add(1));
                    }
                }
                TransitionAction::MakeFaulty | TransitionAction::MakeTombstone => {
                    let Some(subject) = self.memberlist.member(&action.address).cloned() else {
                        continue;
                    };
                    let Some(local) = self.memberlist.member(&self.address).cloned() else {
                        continue;
                    };
                    let status = if action.action == TransitionAction::MakeFaulty {
                        FAULTY
                    } else {
                        TOMBSTONE
                    };
                    let change = Change {
                        source: local.address,
                        source_incarnation: local.incarnation,
                        address: action.address,
                        incarnation: action.incarnation,
                        status: status.to_owned(),
                        labels: subject.labels,
                        timestamp: Timestamp(
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs() as i64,
                        ),
                        ..Default::default()
                    };
                    self.apply_changes(&[change], &mut next_incarnation, &mut insert_position);
                }
            }
        }
    }
}

impl RingpopNode {
    pub fn new(app: impl Into<String>, local_member: Member, requires_app_in_ping: bool) -> Self {
        Self::new_with_ping_timeout(
            app,
            local_member,
            requires_app_in_ping,
            Duration::from_millis(1500),
        )
    }

    /// Construct with the direct-ping deadline used while relaying PING-REQ.
    /// The default matches ringpop-go v0.10.0's 1500ms `pingTimeout`.
    pub fn new_with_ping_timeout(
        app: impl Into<String>,
        local_member: Member,
        requires_app_in_ping: bool,
        ping_timeout: Duration,
    ) -> Self {
        Self::new_with_swim_config(
            app,
            local_member,
            requires_app_in_ping,
            ping_timeout,
            Duration::from_millis(5000),
            3,
            Duration::from_millis(200),
            StateTimeouts::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_swim_config(
        app: impl Into<String>,
        local_member: Member,
        requires_app_in_ping: bool,
        ping_timeout: Duration,
        ping_request_timeout: Duration,
        ping_request_size: usize,
        min_protocol_period: Duration,
        state_timeouts: StateTimeouts,
    ) -> Self {
        let ping_timeout = if ping_timeout.is_zero() {
            Duration::from_millis(1500)
        } else {
            ping_timeout
        };
        let ping_request_timeout = if ping_request_timeout.is_zero() {
            Duration::from_millis(5000)
        } else {
            ping_request_timeout
        };
        let ping_request_size = if ping_request_size == 0 {
            3
        } else {
            ping_request_size
        };
        let min_protocol_period = if min_protocol_period.is_zero() {
            Duration::from_millis(200)
        } else {
            min_protocol_period
        };
        let local_address = local_member.address.clone();
        let mut memberlist = Memberlist::new(local_member);
        let member_iterator = MemberlistIter::new(&mut memberlist, |upper| {
            rand::Rng::gen_range(&mut rand::thread_rng(), 0..upper)
        });
        let mut disseminator = Disseminator::default();
        let (membership_changes, _) = watch::channel(0u64);
        // swim.NewNode records its initial local member with an empty source;
        // preserve this pre-bootstrap delta and its nil/empty wire behavior.
        disseminator.record(Change {
            address: local_address.clone(),
            incarnation: memberlist
                .member(&local_address)
                .expect("local member")
                .incarnation,
            status: memberlist
                .member(&local_address)
                .expect("local member")
                .status
                .clone(),
            labels: memberlist
                .member(&local_address)
                .expect("local member")
                .labels
                .clone(),
            ..Default::default()
        });
        Self {
            state: Arc::new(Mutex::new(State {
                app: app.into(),
                address: local_address.clone(),
                ready: false,
                stopped: false,
                destroyed: false,
                requires_app_in_ping,
                ping_timeout,
                ping_request_timeout,
                ping_request_size,
                transition_started_at: tokio::time::Instant::now(),
                min_protocol_period,
                protocol_metrics: ProtocolMetrics::new(min_protocol_period),
                transition_notify: Arc::new(Notify::new()),
                membership_changes,
                memberlist,
                member_iterator,
                state_transitions: StateTransitionScheduler::new(local_address, state_timeouts),
                disseminator,
                counts: RingpopRequestCounts::default(),
            })),
            runtime: Arc::new(Mutex::new(RuntimeControl::default())),
        }
    }

    /// Bootstrap owner controls readiness. `false` also supports draining.
    pub fn set_ready(&self, ready: bool) {
        self.state.lock().expect("Ringpop state poisoned").ready = ready;
    }

    /// Set labels after Bootstrap and propagate the resulting local member
    /// change with Go's `updateLocalMember` incarnation/checksum semantics.
    pub fn set_local_labels(&self, labels: &BTreeMap<String, String>) {
        let mut state = self.state.lock().expect("Ringpop state poisoned");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(i64::MAX as u128) as i64;
        if let Some(change) = state.memberlist.set_local_labels(labels, now) {
            state.record_local_change(change);
        }
    }

    /// Run Ringpop's proactive self-eviction before the Matching drain.
    /// The local member becomes FAULTY, then a randomized 40% subset (bounded
    /// by the dissemination limit) receives the change through direct PING.
    pub async fn self_evict(&self) -> Result<(), &'static str> {
        {
            let mut runtime = self.runtime.lock().expect("Ringpop runtime poisoned");
            if runtime.self_evicted {
                return Err("Ringpop self eviction already started");
            }
            runtime.self_evicted = true;
        }
        let (targets, timeout) = {
            let mut state = self.state.lock().expect("Ringpop state poisoned");
            if state.stopped || state.destroyed {
                return Err("Ringpop node stopped");
            }
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(i64::MAX as u128) as i64;
            if let Some(change) = state.memberlist.set_local_status(FAULTY, now) {
                state.record_local_change(change);
            }
            let mut pingable: Vec<_> = state
                .memberlist
                .members()
                .iter()
                .filter(|member| {
                    member.address != state.address
                        && (member.status == ALIVE || member.status == SUSPECT)
                })
                .map(|member| member.address.clone())
                .collect();
            let count = pingable.len();
            let max_by_ratio = ((count as f64) * 0.4).ceil() as usize;
            let number = state
                .disseminator
                .max_propagations()
                .min(count)
                .min(max_by_ratio);
            pingable.shuffle(&mut rand::thread_rng());
            pingable.truncate(number);
            (pingable, state.ping_timeout)
        };
        let mut tasks = tokio::task::JoinSet::new();
        for target in targets {
            let node = self.clone();
            tasks.spawn(async move { node.ping(&target, timeout).await });
        }
        while tasks.join_next().await.is_some() {}
        Ok(())
    }

    pub fn stop(&self) {
        let mut runtime = self.runtime.lock().expect("Ringpop runtime poisoned");
        let async_stop_in_progress = runtime.stopping;
        runtime.generation.fetch_add(1, Ordering::SeqCst);
        {
            let mut state = self.state.lock().expect("Ringpop state poisoned");
            state.stopped = true;
            state.state_transitions.disable();
        }
        if let Some(stop_tx) = runtime.stop_tx.take() {
            stop_tx.send_replace(true);
        }
        if let Some(stop_tx) = runtime.transition_stop_tx.take() {
            stop_tx.send_replace(true);
        }
        if !async_stop_in_progress {
            if let Some(done_tx) = &runtime.stop_done {
                done_tx.send_replace(true);
            }
            runtime.stopping = false;
        }
        for task in runtime.tasks.drain(..) {
            task.abort();
        }
        for task in runtime.reverse_tasks.drain(..) {
            task.abort();
        }
    }

    /// Stop SWIM cooperatively and wait for any in-flight probe to complete.
    /// This matches Go's `gossip.Stop` wait boundary; callers shutting down a
    /// production node should await this method before closing its transport.
    pub async fn stop_and_wait(&self) {
        let handle = tokio::runtime::Handle::try_current().ok().or_else(|| {
            self.runtime
                .lock()
                .expect("Ringpop runtime poisoned")
                .executor
                .clone()
        });
        let (tasks, reverse_tasks, existing_stop, done_tx, transition_stop_tx) = {
            let mut runtime = self.runtime.lock().expect("Ringpop runtime poisoned");
            if runtime.stopping {
                let done_rx = runtime
                    .stop_done
                    .as_ref()
                    .expect("stopping runtime has a completion channel")
                    .subscribe();
                (Vec::new(), Vec::new(), Some(done_rx), None, None)
            } else {
                runtime.stopping = true;
                if let Some(stop_tx) = &runtime.stop_tx {
                    stop_tx.send_replace(true);
                }
                let transition_stop_tx = runtime.transition_stop_tx.clone();
                let done_tx = runtime
                    .stop_done
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| watch::channel(false).0);
                if runtime.tasks.is_empty() && runtime.reverse_tasks.is_empty() {
                    runtime.generation.fetch_add(1, Ordering::SeqCst);
                    let mut state = self.state.lock().expect("Ringpop state poisoned");
                    state.state_transitions.disable();
                    state.stopped = true;
                    runtime.stopping = false;
                    done_tx.send_replace(true);
                    return;
                }
                let tasks = std::mem::take(&mut runtime.tasks);
                let reverse_tasks = std::mem::take(&mut runtime.reverse_tasks);
                (
                    tasks,
                    reverse_tasks,
                    None,
                    Some(done_tx),
                    transition_stop_tx,
                )
            }
        };
        if let Some(mut done_rx) = existing_stop {
            while !*done_rx.borrow() {
                if done_rx.changed().await.is_err() {
                    return;
                }
            }
            return;
        }
        let Some(done_tx) = done_tx else { return };
        let Some(handle) = handle else {
            // Started runtimes always have a Tokio executor to own cleanup.
            // If the future is polled outside one, leave completion available
            // to a later caller rather than falsely declaring shutdown done.
            return;
        };
        let node = self.clone();
        let completion = done_tx.clone();
        handle.spawn(async move {
            let mut tasks = tasks.into_iter();
            // Probe and rate tasks share the ordinary stop signal. Keep the
            // transition task live until probes drain, matching Node.Stop's
            // gossip.Stop-then-stateTransitions.Stop ordering.
            if let Some(probe) = tasks.next() {
                let _ = probe.await;
            }
            if let Some(rate) = tasks.next() {
                let _ = rate.await;
            }
            {
                let runtime = node.runtime.lock().expect("Ringpop runtime poisoned");
                runtime.generation.fetch_add(1, Ordering::SeqCst);
                if let Some(stop_tx) = transition_stop_tx {
                    stop_tx.send_replace(true);
                }
                let mut state = node.state.lock().expect("Ringpop state poisoned");
                state.state_transitions.disable();
                state.stopped = true;
                state.transition_notify.notify_waiters();
            }
            for task in tasks {
                let _ = task.await;
            }
            for task in reverse_tasks {
                let _ = task.await;
            }
            let mut runtime = node.runtime.lock().expect("Ringpop runtime poisoned");
            runtime.stopping = false;
            if let Some(stop_tx) = runtime.stop_tx.take() {
                stop_tx.send_replace(true);
            }
            runtime.transition_stop_tx = None;
            runtime.executor = None;
            completion.send_replace(true);
        });
        let mut done_rx = done_tx.subscribe();
        while !*done_rx.borrow() {
            if done_rx.changed().await.is_err() {
                return;
            }
        }
    }

    pub fn destroy(&self) {
        self.state.lock().expect("Ringpop state poisoned").destroyed = true;
        self.stop();
    }

    pub async fn destroy_and_wait(&self) {
        self.state.lock().expect("Ringpop state poisoned").destroyed = true;
        self.stop_and_wait().await;
    }

    /// Start one owned probe loop and one owned state-transition loop. The
    /// readiness bit remains independently controlled by bootstrap owners.
    pub fn start_swim(&self) -> Result<(), &'static str> {
        self.start_swim_with_transport(Arc::new(crate::ringpop_runtime::TChannelProbeTransport))
    }

    pub fn start_swim_with_transport(
        &self,
        transport: Arc<dyn crate::ringpop_runtime::ProbeTransport>,
    ) -> Result<(), &'static str> {
        let handle = tokio::runtime::Handle::try_current().map_err(|_| "Tokio runtime required")?;
        let mut runtime = self.runtime.lock().expect("Ringpop runtime poisoned");
        if runtime.stopping {
            return Err("SWIM shutdown in progress");
        }
        if runtime.tasks.iter().any(|task| !task.is_finished()) {
            return Ok(());
        }
        runtime.tasks.clear();
        let generation = runtime.generation.fetch_add(1, Ordering::SeqCst) + 1;
        runtime.executor = Some(handle.clone());
        let (stop_tx, stop_rx) = watch::channel(false);
        let (transition_stop_tx, transition_stop_rx) = watch::channel(false);
        let (stop_done, _) = watch::channel(false);
        runtime.stop_tx = Some(stop_tx);
        runtime.transition_stop_tx = Some(transition_stop_tx);
        runtime.stop_done = Some(stop_done);
        let state = Arc::clone(&self.state);
        {
            let mut state_guard = state.lock().expect("Ringpop state poisoned");
            state_guard.stopped = false;
            state_guard.state_transitions.enable();
        }

        let probe_state = Arc::clone(&state);
        let probe_generation = Arc::clone(&runtime.generation);
        let probe_transport = Arc::clone(&transport);
        let mut probe_stop = stop_rx.clone();
        runtime.tasks.push(handle.spawn(async move {
            let node = RingpopNode {
                state: probe_state.clone(),
                runtime: Arc::new(Mutex::new(RuntimeControl::default())),
            };
            let initial_period = probe_state
                .lock()
                .expect("Ringpop state poisoned")
                .min_protocol_period;
            let initial_delay = rand::Rng::gen_range(
                &mut rand::thread_rng(),
                0..=initial_period.as_nanos().min(u64::MAX as u128) as u64,
            );
            let mut delay = Duration::from_nanos(initial_delay);
            let mut first_period = true;
            loop {
                if probe_generation.load(Ordering::SeqCst) != generation
                    || probe_state.lock().expect("Ringpop state poisoned").stopped
                    || *probe_stop.borrow()
                {
                    break;
                }
                {
                    let state = probe_state.lock().expect("Ringpop state poisoned");
                    let min_period = state.min_protocol_period;
                    if first_period {
                        first_period = false;
                    } else if let Some(last_period) = state.protocol_metrics.last_period {
                        delay = next_protocol_delay(
                            min_period,
                            last_period,
                            state.protocol_metrics.last_rate,
                            tokio::time::Instant::now(),
                        );
                    } else {
                        delay = min_period;
                    }
                }
                let period_start = tokio::time::Instant::now();
                let _ = crate::ringpop_runtime::ping_next_member_with(
                    &node,
                    probe_transport.as_ref(),
                    |upper| rand::Rng::gen_range(&mut rand::thread_rng(), 0..upper),
                )
                .await;
                let period_end = tokio::time::Instant::now();
                probe_state
                    .lock()
                    .expect("Ringpop state poisoned")
                    .protocol_metrics
                    .record(period_end, period_end.duration_since(period_start));
                // Go computes the delay before its callback and waits after
                // callback completion. The first delay is randomized.
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    changed = probe_stop.changed() => {
                        if changed.is_err() || *probe_stop.borrow() { break; }
                    }
                }
            }
        }));

        let rate_state = Arc::clone(&state);
        let rate_generation = Arc::clone(&runtime.generation);
        let mut rate_stop = stop_rx.clone();
        runtime.tasks.push(handle.spawn(async move {
            loop {
                if rate_generation.load(Ordering::SeqCst) != generation {
                    break;
                }
                {
                    let mut state = rate_state.lock().expect("Ringpop state poisoned");
                    let min_period = state.min_protocol_period;
                    state.protocol_metrics.adjust_rate(min_period);
                }
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    changed = rate_stop.changed() => {
                        if changed.is_err() || *rate_stop.borrow() { break; }
                    }
                }
            }
        }));

        let timer_state = Arc::clone(&state);
        let timer_generation = Arc::clone(&runtime.generation);
        let mut timer_stop = transition_stop_rx;
        runtime.tasks.push(handle.spawn(async move {
            loop {
                if timer_generation.load(Ordering::SeqCst) != generation {
                    break;
                }
                let (deadline, now_ns, notifier) = {
                    let state = timer_state.lock().expect("Ringpop state poisoned");
                    if state.stopped {
                        break;
                    }
                    let deadline = state
                        .state_transitions
                        .timers()
                        .values()
                        .filter(|timer| !timer.fired)
                        .map(|timer| timer.deadline_ns)
                        .min();
                    (
                        deadline,
                        state.monotonic_now_ns(),
                        Arc::clone(&state.transition_notify),
                    )
                };
                let wait = deadline
                    .map(|deadline| Duration::from_nanos(deadline.saturating_sub(now_ns).max(0) as u64))
                    .unwrap_or(Duration::from_secs(86_400));
                tokio::select! {
                    _ = tokio::time::sleep(wait) => {
                        let mut state = timer_state.lock().expect("Ringpop state poisoned");
                        let now = state.monotonic_now_ns();
                        state.apply_due_transitions(
                            now,
                            || std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as i64,
                            |len| rand::Rng::gen_range(&mut rand::thread_rng(), 0..len),
                        );
                    }
                    _ = notifier.notified() => {}
                    changed = timer_stop.changed() => {
                        if changed.is_err() || *timer_stop.borrow() { break; }
                    }
                }
            }
        }));
        Ok(())
    }

    pub fn is_destroyed(&self) -> bool {
        self.state.lock().expect("Ringpop state poisoned").destroyed
    }

    pub fn lifecycle(&self) -> RingpopLifecycle {
        let state = self.state.lock().expect("Ringpop state poisoned");
        if state.stopped {
            RingpopLifecycle::Stopped
        } else if state.ready {
            RingpopLifecycle::Ready
        } else {
            RingpopLifecycle::Bootstrapping
        }
    }

    pub fn is_ready(&self) -> bool {
        self.state.lock().expect("Ringpop state poisoned").ready
    }

    pub fn request_counts(&self) -> RingpopRequestCounts {
        self.state.lock().expect("Ringpop state poisoned").counts
    }

    /// Send one Ringpop v0.10.0 JOIN call to the selected seed and apply its
    /// returned membership with the same `AddJoinList` semantics as Go.
    /// This deliberately does not select hosts, retry, or change readiness.
    pub async fn join_seed(
        &self,
        seed: &str,
        timeout: Duration,
    ) -> Result<JoinResponse, JsonCallError> {
        self.join_seed_signed(seed, i64::try_from(timeout.as_nanos()).unwrap_or(i64::MAX))
            .await
    }

    /// Signed-duration form used by Ringpop's Go-compatible bootstrap options.
    /// Nonpositive durations remain signed in JOIN JSON and expire immediately.
    pub async fn join_seed_signed(
        &self,
        seed: &str,
        timeout_nanos: i64,
    ) -> Result<JoinResponse, JsonCallError> {
        let request = {
            let node = self.state.lock().expect("Ringpop state poisoned");
            let local = node.memberlist.member(&node.address).expect("local member");
            JoinRequest {
                app: node.app.clone(),
                source: local.address.clone(),
                incarnation: local.incarnation,
                timeout: timeout_nanos,
                labels: local.labels.clone(),
            }
        };
        let response = tchannel_json_client::call(
            seed,
            "ringpop",
            "/protocol/join",
            None,
            &serde_json::to_value(&request).expect("JoinRequest serialization"),
            if timeout_nanos <= 0 {
                Duration::ZERO
            } else {
                Duration::from_nanos(timeout_nanos as u64)
            },
        )
        .await?;
        // Decode the complete response before touching membership. Go's
        // encoding/json likewise completes the response decode before
        // JoinGroup calls AddJoinList; malformed JSON cannot partially apply.
        let join: JoinResponse = serde_json::from_value(response.body)
            .map_err(|error| JsonCallError::Transport(anyhow::anyhow!(error)))?;
        let changes = join.membership.as_deref().unwrap_or_default();
        let mut node = self.state.lock().expect("Ringpop state poisoned");
        // Go memberlist.Update returns without applying anything when the
        // node stopped while the JOIN request was in flight.
        if node.stopped {
            return Ok(join);
        }
        apply_join_list(&mut node, changes, wall_clock_millis, |len| {
            rand::Rng::gen_range(&mut rand::thread_rng(), 0..len)
        });
        Ok(join)
    }

    /// Run one Ringpop v0.10.0 partition-heal attempt against a discovery host.
    /// JOIN membership is inspected without being merged until the conflict
    /// check decides whether the two sides can safely converge.
    pub async fn attempt_heal(&self, target: &str) -> Result<Vec<String>, String> {
        let (request, local_members) = {
            let node = self.state.lock().expect("Ringpop state poisoned");
            let local = node.memberlist.member(&node.address).expect("local member");
            let request = JoinRequest {
                app: node.app.clone(),
                source: local.address.clone(),
                incarnation: local.incarnation,
                timeout: 1_000_000_000,
                labels: local.labels.clone(),
            };
            let local_members = membership_as_changes(node.memberlist.members(), local);
            (request, local_members)
        };
        let response = tchannel_json_client::call(
            target,
            "ringpop",
            "/protocol/join",
            None,
            &serde_json::to_value(request).map_err(|e| e.to_string())?,
            Duration::from_secs(1),
        )
        .await
        .map_err(|e| e.to_string())?;
        let remote: JoinResponse =
            serde_json::from_value(response.body).map_err(|e| e.to_string())?;
        let remote_members = remote.membership.unwrap_or_default();
        let local_snapshot = local_members;
        let (changes_a, changes_b) =
            changes_that_need_reincarnation(&local_snapshot, &remote_members);
        if !changes_a.is_empty() || !changes_b.is_empty() {
            {
                let mut node = self.state.lock().expect("Ringpop state poisoned");
                node.apply_changes(&changes_a, wall_clock_millis, |len| {
                    rand::Rng::gen_range(&mut rand::thread_rng(), 0..len)
                });
            }
            if !changes_b.is_empty() {
                self.send_ping_changes(target, changes_b).await?;
            }
        } else {
            {
                let mut node = self.state.lock().expect("Ringpop state poisoned");
                node.apply_changes(&remote_members, wall_clock_millis, |len| {
                    rand::Rng::gen_range(&mut rand::thread_rng(), 0..len)
                });
            }
            let merged = {
                let node = self.state.lock().expect("Ringpop state poisoned");
                let local = node.memberlist.member(&node.address).expect("local member");
                membership_as_changes(node.memberlist.members(), local)
            };
            self.send_ping_changes(target, merged).await?;
        }
        Ok(remote_members
            .iter()
            .filter(|member| member.status == ALIVE || member.status == SUSPECT)
            .map(|member| member.address.clone())
            .collect())
    }

    async fn send_ping_changes(&self, target: &str, changes: Vec<Change>) -> Result<(), String> {
        let request = {
            let node = self.state.lock().expect("Ringpop state poisoned");
            let local = node.memberlist.member(&node.address).expect("local member");
            Ping {
                changes: Some(changes),
                checksum: node.memberlist.checksum(),
                source: local.address.clone(),
                source_incarnation: local.incarnation,
                app: node.app.clone(),
            }
        };
        let response = tchannel_json_client::call(
            target,
            "ringpop",
            "/protocol/ping",
            None,
            &serde_json::to_value(request).map_err(|e| e.to_string())?,
            Duration::from_secs(1),
        )
        .await
        .map_err(|e| e.to_string())?;
        serde_json::from_value::<Ping>(response.body).map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Execute one Go `JoinGroup`: select once, launch every selected JOIN
    /// concurrently, wait for all outcomes, and apply successful memberships
    /// through `join_seed`'s atomic AddJoinList path. Outcome order is inherently
    /// concurrent, as in Go; callers should treat each vector as a collection.
    pub async fn join_group(
        &self,
        sender: &mut JoinGroupState,
        joined: &[String],
    ) -> JoinGroupResult {
        sender.attempts = sender.attempts.wrapping_add(1);
        let targets = sender.selection.select_group(joined);
        eprintln!(
            "Matching Ringpop JOIN attempt {} started: targets={:?}",
            sender.attempts, targets
        );
        let timeout = sender.selection.options.timeout_nanos;
        let mut tasks = tokio::task::JoinSet::new();
        for target in targets {
            let node = self.clone();
            tasks.spawn(async move {
                let outcome = node.join_seed_signed(&target, timeout).await;
                (target, outcome.is_ok())
            });
        }
        let mut result = JoinGroupResult {
            attempt: sender.attempts,
            ..Default::default()
        };
        while let Some(outcome) = tasks.join_next().await {
            if let Ok((target, true)) = outcome {
                result.successes.push(target);
            } else if let Ok((target, false)) = outcome {
                result.failures.push(target);
            }
        }
        eprintln!(
            "Matching Ringpop JOIN attempt {} completed: successes={:?} failures={:?}",
            result.attempt, result.successes, result.failures
        );
        result
    }

    /// Send one direct Ringpop v0.10.0 PING. Dissemination counters advance
    /// only after a successful TChannel response and complete JSON decode,
    /// matching `sendPing`'s IssueAsSender/bumpPiggybackCounters ordering.
    pub async fn ping(&self, target: &str, timeout: Duration) -> Result<Ping, JsonCallError> {
        let (request, sent_changes) = {
            let node = self.state.lock().expect("Ringpop state poisoned");
            let local = node.memberlist.member(&node.address).expect("local member");
            let changes = node.disseminator.issue_sender();
            let request = Ping {
                changes: Some(changes.clone()),
                checksum: node.memberlist.checksum(),
                source: local.address.clone(),
                source_incarnation: local.incarnation,
                app: node.app.clone(),
            };
            (request, changes)
        };
        let response = tchannel_json_client::call(
            target,
            "ringpop",
            "/protocol/ping",
            None,
            &serde_json::to_value(&request).expect("Ping serialization"),
            timeout,
        )
        .await?;
        // Fully decode first. A malformed response must neither merge state
        // nor count this packet as a successful dissemination.
        let ping: Ping = serde_json::from_value(response.body)
            .map_err(|error| JsonCallError::Transport(anyhow::anyhow!(error)))?;
        let mut node = self.state.lock().expect("Ringpop state poisoned");
        // `sendPing` invokes its successful piggyback callback before
        // `pingNextMember` applies the response through Memberlist.Update.
        node.disseminator.complete_sender(&sent_changes, true);
        node.apply_changes(
            ping.changes.as_deref().unwrap_or_default(),
            || {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64
            },
            |len| rand::Rng::gen_range(&mut rand::thread_rng(), 0..len),
        );
        // Go's sendPing callback above succeeds even if the node stopped while
        // the response was in flight; memberlist.Update itself is stop guarded.
        Ok(ping)
    }

    /// Send one Ringpop v0.10.0 PING-REQ through `peer` to `target`.
    /// A decoded response merges its membership delta even when `pingStatus`
    /// is false. Go's PING-REQ sender advances dissemination counters on RPC
    /// or JSON decode error, but not on success; that callback is independent
    /// of the stopped-state memberlist guard.
    pub async fn ping_request(
        &self,
        peer: &str,
        target: &str,
        timeout: Duration,
    ) -> Result<PingResponse, JsonCallError> {
        let (request, sent_changes) = {
            let node = self.state.lock().expect("Ringpop state poisoned");
            let local = node.memberlist.member(&node.address).expect("local member");
            let changes = node.disseminator.issue_sender();
            (
                PingRequest {
                    source: local.address.clone(),
                    source_incarnation: local.incarnation,
                    target: target.to_owned(),
                    checksum: node.memberlist.checksum(),
                    changes: Some(changes.clone()),
                },
                changes,
            )
        };
        let request = serde_json::to_value(&request).expect("PingRequest serialization");
        let callback_state = self.state.clone();
        let peer = peer.to_owned();
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        // Go's MakeCall runs independently of SendPingRequest's timeout
        // select. Keep the transport and its error callback alive if the
        // caller times out or is cancelled; a late successful response is
        // discarded because only the caller applies response membership.
        tokio::spawn(async move {
            let result = async {
                let response = tchannel_json_client::call(
                    &peer,
                    "ringpop",
                    "/protocol/ping-req",
                    None,
                    &request,
                    timeout,
                )
                .await?;
                serde_json::from_value::<PingResponse>(response.body)
                    .map_err(|error| JsonCallError::Transport(anyhow::anyhow!(error)))
            }
            .await;
            if result.is_err() {
                callback_state
                    .lock()
                    .expect("Ringpop state poisoned")
                    .disseminator
                    .complete_sender(&sent_changes, true);
            }
            let _ = result_tx.send(result);
        });
        let result = match tokio::time::timeout(timeout, result_rx).await {
            Err(_) => {
                return Err(JsonCallError::Transport(anyhow::anyhow!(
                    "ping request timed out"
                )))
            }
            Ok(Err(_)) => {
                return Err(JsonCallError::Transport(anyhow::anyhow!(
                    "ping request sender task terminated"
                )))
            }
            Ok(Ok(Err(error))) => return Err(error),
            Ok(Ok(Ok(response))) => response,
        };
        let mut node = self.state.lock().expect("Ringpop state poisoned");
        // Unlike direct PING, a successful PING-REQ does not bump sender
        // piggyback counters in the pinned Go implementation.
        node.apply_changes(
            result.changes.as_deref().unwrap_or_default(),
            || {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64
            },
            |len| rand::Rng::gen_range(&mut rand::thread_rng(), 0..len),
        );
        Ok(result)
    }

    /// Launch one PING-REQ per randomly selected pingable helper. The helpers
    /// run independently; dropping this receiver after the first positive
    /// response does not cancel outstanding calls.
    pub fn send_ping_requests(
        &self,
        target: &str,
        amount: usize,
        timeout: Duration,
    ) -> mpsc::Receiver<PingRequestResult> {
        let (tx, rx) = mpsc::channel(amount.max(1));
        let mut helpers: Vec<String> = {
            let node = self.state.lock().expect("Ringpop state poisoned");
            node.memberlist
                .members()
                .iter()
                .filter(|member| {
                    member.address != node.address
                        && member.address != target
                        && (member.status == ALIVE || member.status == SUSPECT)
                })
                .map(|member| member.address.clone())
                .collect()
        };
        helpers.shuffle(&mut rand::thread_rng());
        // Go appends a pingable member before checking `len >= n`, so its
        // zero-sized request still selects one helper. Negative sizes panic in
        // Go before requests are launched; this unsigned API cannot express it.
        helpers.truncate(amount.max(1));
        for helper in helpers {
            let sender = tx.clone();
            let node = self.clone();
            let target = target.to_owned();
            tokio::spawn(async move {
                let outcome = match node.ping_request(&helper, &target, timeout).await {
                    Ok(response) => PingRequestResult::Response(response),
                    Err(error) => PingRequestResult::Error(error),
                };
                let _ = sender.send(outcome).await;
            });
        }
        drop(tx);
        rx
    }

    /// Await concurrent helpers until one reports `pingStatus=true` or all
    /// finish. Errors collected before an early success are preserved.
    pub async fn indirect_ping(
        &self,
        target: &str,
        amount: usize,
        timeout: Duration,
    ) -> (bool, Vec<JsonCallError>) {
        let mut results = self.send_ping_requests(target, amount, timeout);
        let mut errors = Vec::new();
        while let Some(result) = results.recv().await {
            match result {
                PingRequestResult::Response(response) if response.ping_status => {
                    return (true, errors)
                }
                PingRequestResult::Error(error) => errors.push(error),
                PingRequestResult::Response(_) => {}
            }
        }
        (false, errors)
    }

    /// Relay one SWIM PING on behalf of a PING-REQ caller. Go's
    /// `handlePingRequest` updates request changes, sends a normal PING (which
    /// owns its sender dissemination callback), merges target changes only on
    /// success, then responds with the relay's delta for the original caller.
    async fn relay_ping_request(&self, request: PingRequest) -> Result<PingResponse, RingpopError> {
        let (relay_ping, sent_changes, relay_address, ping_timeout) = {
            let mut node = self.state.lock().expect("Ringpop state poisoned");
            node.counts.ping_req = node.counts.ping_req.saturating_add(1);
            let relay_address = node.address.clone();
            node.apply_changes(
                request.changes.as_deref().unwrap_or_default(),
                || {
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as i64
                },
                |len| rand::Rng::gen_range(&mut rand::thread_rng(), 0..len),
            );
            let local = node
                .memberlist
                .member(&relay_address)
                .expect("local member");
            let changes = node.disseminator.issue_sender();
            (
                Ping {
                    changes: Some(changes.clone()),
                    checksum: node.memberlist.checksum(),
                    source: local.address.clone(),
                    source_incarnation: local.incarnation,
                    app: node.app.clone(),
                },
                changes,
                relay_address,
                node.ping_timeout,
            )
        };
        let response = tchannel_json_client::call(
            &request.target,
            "ringpop",
            "/protocol/ping",
            None,
            &serde_json::to_value(relay_ping).expect("Ping serialization"),
            ping_timeout,
        )
        .await;
        let (ping_ok, target_changes) = match response {
            Ok(response) => match serde_json::from_value::<Ping>(response.body) {
                Ok(ping) => {
                    let mut node = self.state.lock().expect("Ringpop state poisoned");
                    node.disseminator.complete_sender(&sent_changes, true);
                    (true, ping.changes.unwrap_or_default())
                }
                Err(_) => (false, Vec::new()),
            },
            Err(_) => (false, Vec::new()),
        };
        if ping_ok {
            let mut node = self.state.lock().expect("Ringpop state poisoned");
            node.apply_changes(
                &target_changes,
                || {
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as i64
                },
                |len| rand::Rng::gen_range(&mut rand::thread_rng(), 0..len),
            );
        }
        let mut node = self.state.lock().expect("Ringpop state poisoned");
        let local = node
            .memberlist
            .member(&relay_address)
            .expect("local member")
            .clone();
        let checksum = node.memberlist.checksum();
        let members = node.memberlist.members().to_vec();
        let (changes, _) = node.disseminator.issue_receiver(
            &request.source,
            request.source_incarnation,
            request.checksum,
            checksum,
            &members,
            &relay_address,
            local.incarnation,
        );
        Ok(PingResponse {
            ping_status: ping_ok,
            target: request.target,
            changes: Some(changes),
        })
    }

    /// Stable snapshot boundary for bootstrap owners and compatibility checks.
    pub fn membership_snapshot(&self) -> (Vec<Member>, u32) {
        let node = self.state.lock().expect("Ringpop state poisoned");
        (
            node.memberlist.members().to_vec(),
            node.memberlist.checksum(),
        )
    }

    /// Subscribe to accepted Ringpop membership changes. The initial watch
    /// value is a snapshot boundary; receivers should refresh immediately and
    /// then refresh whenever the generation changes.
    pub fn subscribe_membership_changes(&self) -> watch::Receiver<u64> {
        self.state
            .lock()
            .expect("Ringpop state poisoned")
            .membership_changes
            .subscribe()
    }

    pub fn dissemination_counts(&self) -> BTreeMap<String, usize> {
        self.state
            .lock()
            .expect("Ringpop state poisoned")
            .disseminator
            .propagation_counts()
    }

    pub fn max_propagations(&self) -> usize {
        self.state
            .lock()
            .expect("Ringpop state poisoned")
            .disseminator
            .max_propagations()
    }

    pub(crate) fn next_pingable_member_with<R>(&self, random_below: R) -> Option<Member>
    where
        R: FnMut(usize) -> usize,
    {
        let mut state = self.state.lock().expect("Ringpop state poisoned");
        let State {
            memberlist,
            member_iterator,
            ..
        } = &mut *state;
        member_iterator.next(memberlist, random_below)
    }

    pub(crate) fn ping_timeout(&self) -> Duration {
        self.state
            .lock()
            .expect("Ringpop state poisoned")
            .ping_timeout
    }

    pub(crate) fn ping_request_timeout(&self) -> Duration {
        self.state
            .lock()
            .expect("Ringpop state poisoned")
            .ping_request_timeout
    }

    pub(crate) fn ping_request_size(&self) -> usize {
        self.state
            .lock()
            .expect("Ringpop state poisoned")
            .ping_request_size
    }

    pub(crate) fn make_suspect(&self, address: &str, incarnation: i64) {
        let mut state = self.state.lock().expect("Ringpop state poisoned");
        let local = state
            .memberlist
            .member(&state.address)
            .expect("local member")
            .clone();
        let labels = state
            .memberlist
            .member(address)
            .and_then(|member| member.labels.clone());
        let change = Change {
            source: local.address,
            source_incarnation: local.incarnation,
            address: address.to_owned(),
            incarnation,
            status: SUSPECT.to_owned(),
            labels,
            timestamp: Timestamp(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64,
            ),
            ..Default::default()
        };
        state.apply_changes(
            &[change],
            || {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64
            },
            |len| rand::Rng::gen_range(&mut rand::thread_rng(), 0..len),
        );
    }

    pub async fn ping_next_member(&self) -> crate::ringpop_runtime::ProbeOutcome {
        crate::ringpop_runtime::ping_next_member_with(
            self,
            &crate::ringpop_runtime::TChannelProbeTransport,
            |upper| rand::Rng::gen_range(&mut rand::thread_rng(), 0..upper),
        )
        .await
    }

    pub fn handler(&self) -> RingpopHandler {
        let state = Arc::clone(&self.state);
        let runtime = Arc::clone(&self.runtime);
        RingpopHandler::new(move |method, _headers, body| {
            let state = Arc::clone(&state);
            let runtime = Arc::clone(&runtime);
            async move {
                let result = match method.as_str() {
                    "/protocol/join" => handle_join(&state, &body),
                    "/protocol/ping" => {
                        let (response, reverse_target) = handle_ping_with_full_sync(
                            &state,
                            &body,
                            || {
                                std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_millis() as i64
                            },
                            |len| rand::Rng::gen_range(&mut rand::thread_rng(), 0..len),
                        )?;
                        if let Some(target) = reverse_target {
                            schedule_reverse_full_sync(&runtime, state.clone(), target);
                        }
                        Ok(response)
                    }
                    "/protocol/ping-req" => {
                        let request: PingRequest = decode(&body)?;
                        {
                            let node = state.lock().expect("Ringpop state poisoned");
                            if !node.ready {
                                return Err(RingpopError::Application(
                                    "node is not ready to handle requests".into(),
                                ));
                            }
                        }
                        let owner = RingpopNode {
                            state: Arc::clone(&state),
                            runtime: Arc::clone(&runtime),
                        };
                        let response = owner.relay_ping_request(request).await?;
                        encode(&response)
                    }
                    _ => Err(RingpopError::System(format!(
                        "unknown Ringpop method: {method}"
                    ))),
                }?;
                Ok(RingpopResponse {
                    headers: None,
                    body: result,
                })
            }
        })
    }

    /// Deterministic direct-handler entry for tests and owners that inject the
    /// same clock and insertion source used by their memberlist lifecycle.
    pub fn handle_ping_with<F, R>(
        &self,
        body: &[u8],
        next_incarnation: F,
        insert_position: R,
    ) -> Result<Vec<u8>, RingpopError>
    where
        F: FnMut() -> i64,
        R: FnMut(usize) -> usize,
    {
        handle_ping(&self.state, body, next_incarnation, insert_position)
    }
}

/// Apply the exact accepted-change pipeline used by Go's
/// `Node.handleChanges` followed by `memberlist.AddJoinList`.
fn apply_join_list<F, R>(
    node: &mut State,
    changes: &[Change],
    next_incarnation: F,
    insert_position: R,
) where
    F: FnMut() -> i64,
    R: FnMut(usize) -> usize,
{
    let local_address = node.address.clone();
    for change in node.apply_changes(changes, next_incarnation, insert_position) {
        if change.address != local_address {
            node.disseminator.clear_address(&change.address);
        }
    }
}

/// Return the Go-compatible wall-clock incarnation timestamp in milliseconds.
fn wall_clock_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn decode<T: DeserializeOwned + Default>(body: &[u8]) -> Result<T, RingpopError> {
    if body.is_empty() {
        return Ok(T::default());
    }
    let mut deserializer = serde_json::Deserializer::from_slice(body);
    Option::<T>::deserialize(&mut deserializer)
        .map(|value| value.unwrap_or_default())
        .map_err(|e| RingpopError::System(e.to_string()))
}

fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, RingpopError> {
    serde_json::to_vec(value).map_err(|e| RingpopError::System(e.to_string()))
}

fn membership_as_changes(members: &[Member], local: &Member) -> Vec<Change> {
    members
        .iter()
        .map(|member| {
            Change {
                source: local.address.clone(),
                source_incarnation: local.incarnation,
                address: member.address.clone(),
                incarnation: member.incarnation,
                status: member.status.clone(),
                tombstone: false,
                labels: member.labels.clone(),
                timestamp: Default::default(),
            }
            .validate_outgoing()
        })
        .collect()
}

fn changes_that_need_reincarnation(
    local: &[Change],
    remote: &[Change],
) -> (Vec<Change>, Vec<Change>) {
    let mut changes_for_local = Vec::new();
    let mut changes_for_remote = Vec::new();
    for remote_change in remote {
        let Some(local_change) = local
            .iter()
            .find(|change| change.address == remote_change.address)
        else {
            continue;
        };
        let local_member = Member {
            address: local_change.address.clone(),
            incarnation: local_change.incarnation,
            status: local_change.status.clone(),
            labels: local_change.labels.clone(),
        };
        let remote_member = Member {
            address: remote_change.address.clone(),
            incarnation: remote_change.incarnation,
            status: remote_change.status.clone(),
            labels: remote_change.labels.clone(),
        };
        let local_pingable = matches!(local_member.status.as_str(), ALIVE | SUSPECT);
        let remote_pingable = matches!(remote_member.status.as_str(), ALIVE | SUSPECT);
        if remote_pingable
            && should_process_gossip(Some(&remote_member), &local_member)
            && !local_pingable
        {
            let mut change = local_change.clone();
            change.source.clear();
            change.source_incarnation = 0;
            change.status = SUSPECT.to_owned();
            changes_for_remote.push(change);
        }
        if local_pingable
            && should_process_gossip(Some(&local_member), &remote_member)
            && !remote_pingable
        {
            let mut change = remote_change.clone();
            change.source.clear();
            change.source_incarnation = 0;
            change.status = SUSPECT.to_owned();
            changes_for_local.push(change);
        }
    }
    (changes_for_local, changes_for_remote)
}

fn handle_join(state: &Arc<Mutex<State>>, body: &[u8]) -> Result<Vec<u8>, RingpopError> {
    let request: JoinRequest = decode(body)?;
    let mut node = state.lock().expect("Ringpop state poisoned");
    node.counts.join = node.counts.join.saturating_add(1);
    if node.address == request.source {
        return Err(RingpopError::Application(format!(
            "A node tried joining a cluster by attempting to join itself. The node ,{}, must join someone else.", request.source
        )));
    }
    if node.app != request.app {
        return Err(RingpopError::Application(format!(
            "A node tried joining a different app cluster. The expected app, {}, did not match the actual app ,{}", node.app, request.app
        )));
    }
    let local = node
        .memberlist
        .member(&node.address)
        .expect("local member")
        .clone();
    let membership = node
        .memberlist
        .members()
        .iter()
        .map(|member| {
            Change {
                source: local.address.clone(),
                source_incarnation: local.incarnation,
                address: member.address.clone(),
                incarnation: member.incarnation,
                status: member.status.clone(),
                tombstone: false,
                labels: member.labels.clone(),
                timestamp: Default::default(),
            }
            .validate_outgoing()
        })
        .collect();
    encode(&JoinResponse {
        app: node.app.clone(),
        coordinator: node.address.clone(),
        membership: Some(membership),
        membership_checksum: node.memberlist.checksum(),
    })
}

fn handle_ping<F, R>(
    state: &Arc<Mutex<State>>,
    body: &[u8],
    next_incarnation: F,
    insert_position: R,
) -> Result<Vec<u8>, RingpopError>
where
    F: FnMut() -> i64,
    R: FnMut(usize) -> usize,
{
    handle_ping_with_full_sync(state, body, next_incarnation, insert_position)
        .map(|(response, _)| response)
}

fn handle_ping_with_full_sync<F, R>(
    state: &Arc<Mutex<State>>,
    body: &[u8],
    next_incarnation: F,
    insert_position: R,
) -> Result<(Vec<u8>, Option<String>), RingpopError>
where
    F: FnMut() -> i64,
    R: FnMut(usize) -> usize,
{
    let request: Ping = decode(body)?;
    let mut node = state.lock().expect("Ringpop state poisoned");
    if !node.ready {
        return Err(RingpopError::Application(
            "node is not ready to handle requests".into(),
        ));
    }
    if request.app.is_empty() {
        if node.requires_app_in_ping {
            return Err(RingpopError::Application(
                "Pinged ringpop requires app name".into(),
            ));
        }
    } else if request.app != node.app {
        return Err(RingpopError::Application(
            "Pinged ringpop has a different app name".into(),
        ));
    }
    node.counts.ping = node.counts.ping.saturating_add(1);
    let ping_number = node.counts.ping;
    let local_address = node.address.clone();
    let accepted = node.apply_changes(
        request.changes.as_deref().unwrap_or_default(),
        next_incarnation,
        insert_position,
    );
    let local_incarnation = node
        .memberlist
        .member(&local_address)
        .expect("local member")
        .incarnation;
    let local_checksum = node.memberlist.checksum();
    let members = node.memberlist.members().to_vec();
    // Keep a small production-gate trace of actual Go-to-Rust SWIM traffic.
    // The first few successful PINGs distinguish a Ringpop transport/protocol
    // failure from a Matching resolver or domain-cache failure without making
    // discovery or readiness more permissive.
    if ping_number <= 8 {
        let accepted: Vec<_> = accepted
            .iter()
            .map(|change| format!("{}:{}", change.address, change.status))
            .collect();
        let members: Vec<_> = members
            .iter()
            .map(|member| format!("{}:{}", member.address, member.status))
            .collect();
        eprintln!(
            "Matching Ringpop received PING #{ping_number} from {}; accepted_changes={accepted:?}; members={members:?}",
            request.source
        );
    }
    let (changes, full_sync) = node.disseminator.issue_receiver(
        &request.source,
        request.source_incarnation,
        request.checksum,
        local_checksum,
        &members,
        &local_address,
        local_incarnation,
    );
    let response = encode(&Ping {
        checksum: local_checksum,
        changes: Some(changes),
        source: local_address,
        source_incarnation: local_incarnation,
        app: String::new(),
    })?;
    Ok((response, full_sync.then_some(request.source)))
}

fn schedule_reverse_full_sync(
    runtime: &Arc<Mutex<RuntimeControl>>,
    state: Arc<Mutex<State>>,
    target: String,
) {
    let mut owner = runtime.lock().expect("Ringpop runtime poisoned");
    if owner.stopping || owner.reverse_jobs.load(Ordering::SeqCst) >= 5 {
        return;
    }
    owner.reverse_tasks.retain(|task| !task.is_finished());
    owner.reverse_jobs.fetch_add(1, Ordering::SeqCst);
    let runtime = Arc::clone(runtime);
    let node = RingpopNode {
        state,
        runtime: Arc::clone(&runtime),
    };
    let task = tokio::spawn(async move {
        let _ = node.join_seed(&target, Duration::from_secs(1)).await;
        node.runtime
            .lock()
            .expect("Ringpop runtime poisoned")
            .reverse_jobs
            .fetch_sub(1, Ordering::SeqCst);
    });
    owner.reverse_tasks.push(task);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn partition_heal_joins_two_live_protocol_peers_and_converges_membership() {
        use crate::{grpc_matching::Matching, handlers::AppState};
        use tokio::net::TcpListener;

        let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address_a = listener_a.local_addr().unwrap().to_string();
        let address_b = listener_b.local_addr().unwrap().to_string();
        let node_a = RingpopNode::new(
            "heal-test",
            Member {
                address: address_a.clone(),
                status: ALIVE.into(),
                incarnation: 100,
                labels: None,
            },
            false,
        );
        let node_b = RingpopNode::new(
            "heal-test",
            Member {
                address: address_b.clone(),
                status: ALIVE.into(),
                incarnation: 200,
                labels: None,
            },
            false,
        );
        node_a.set_ready(true);
        node_b.set_ready(true);
        let server_a = tokio::spawn(crate::tchannel::serve_matching_with_ringpop_listener(
            listener_a,
            Matching::new(AppState::default()),
            node_a.handler(),
        ));
        let server_b = tokio::spawn(crate::tchannel::serve_matching_with_ringpop_listener(
            listener_b,
            Matching::new(AppState::default()),
            node_b.handler(),
        ));

        assert_eq!(node_a.membership_snapshot().0.len(), 1);
        assert_eq!(node_b.membership_snapshot().0.len(), 1);
        let reached = node_a.attempt_heal(&address_b).await.unwrap();
        assert!(reached.contains(&address_b));
        for node in [&node_a, &node_b] {
            let (members, _) = node.membership_snapshot();
            assert_eq!(members.len(), 2);
            assert!(members
                .iter()
                .any(|member| member.address == address_a && member.status == ALIVE));
            assert!(members
                .iter()
                .any(|member| member.address == address_b && member.status == ALIVE));
        }

        node_a.destroy_and_wait().await;
        node_b.destroy_and_wait().await;
        server_a.abort();
        server_b.abort();
        let _ = server_a.await;
        let _ = server_b.await;
    }

    #[tokio::test]
    async fn receiver_full_sync_starts_reverse_join_and_converges_two_live_peers() {
        use crate::{grpc_matching::Matching, handlers::AppState};
        use tokio::net::TcpListener;

        let listener_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address_a = listener_a.local_addr().unwrap().to_string();
        let address_b = listener_b.local_addr().unwrap().to_string();
        let node_a = RingpopNode::new(
            "reverse-test",
            Member {
                address: address_a.clone(),
                status: ALIVE.into(),
                incarnation: 10,
                labels: None,
            },
            false,
        );
        let node_b = RingpopNode::new(
            "reverse-test",
            Member {
                address: address_b.clone(),
                status: ALIVE.into(),
                incarnation: 20,
                labels: None,
            },
            false,
        );
        node_a.set_ready(true);
        node_b.set_ready(true);
        node_b.state.lock().unwrap().disseminator.clear();
        let server_a = tokio::spawn(crate::tchannel::serve_matching_with_ringpop_listener(
            listener_a,
            Matching::new(AppState::default()),
            node_a.handler(),
        ));
        let server_b = tokio::spawn(crate::tchannel::serve_matching_with_ringpop_listener(
            listener_b,
            Matching::new(AppState::default()),
            node_b.handler(),
        ));

        let request = Ping {
            changes: Some(Vec::new()),
            checksum: 1,
            source: address_a.clone(),
            source_incarnation: 10,
            app: "reverse-test".into(),
        };
        let response = crate::tchannel_json_client::call(
            &address_b,
            "ringpop",
            "/protocol/ping",
            None,
            &serde_json::to_value(request).unwrap(),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        let response: Ping = serde_json::from_value(response.body).unwrap();
        let response_changes = response.changes.unwrap();
        assert_eq!(response_changes.len(), 1);
        // Apply the reply as the actual Ping sender does after a successful
        // TChannel response; the receiver independently launches reverse JOIN.
        node_a
            .state
            .lock()
            .unwrap()
            .apply_changes(&response_changes, wall_clock_millis, |len| {
                rand::Rng::gen_range(&mut rand::thread_rng(), 0..len)
            });

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if node_a.membership_snapshot().0.len() == 2
                    && node_b.membership_snapshot().0.len() == 2
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("reverse full sync did not merge the two partition memberships");
        let (members_a, checksum_a) = node_a.membership_snapshot();
        let (members_b, checksum_b) = node_b.membership_snapshot();
        assert_eq!(checksum_a, checksum_b);
        assert!(members_a.iter().any(|member| member.address == address_b));
        assert!(members_b.iter().any(|member| member.address == address_a));

        node_a.destroy_and_wait().await;
        node_b.destroy_and_wait().await;
        server_a.abort();
        server_b.abort();
        let _ = server_a.await;
        let _ = server_b.await;
    }

    #[test]
    fn go_heal_partition_reincarnates_overriding_faulty_winner_before_merge() {
        let member_change = |address: &str, status: &str, incarnation| Change {
            address: address.into(),
            status: status.into(),
            incarnation,
            source: "partition-peer".into(),
            source_incarnation: 9,
            ..Default::default()
        };
        // Go nodesThatNeedToReincarnate: a higher-incarnation Faulty entry in
        // A would overwrite B's pingable Alive member, so A is gossiped as
        // Suspect to B before the partitions merge.
        let local = vec![member_change("shared", FAULTY, 20)];
        let remote = vec![member_change("shared", ALIVE, 10)];
        let (for_local, for_remote) = changes_that_need_reincarnation(&local, &remote);
        assert!(for_local.is_empty());
        assert_eq!(for_remote.len(), 1);
        assert_eq!(for_remote[0].status, SUSPECT);
        assert!(for_remote[0].source.is_empty());
        assert_eq!(for_remote[0].source_incarnation, 0);

        // Symmetric conflict: B's winning Faulty entry must be sent as
        // Suspect to A while A retains its pingable entry.
        let local = vec![member_change("shared", ALIVE, 10)];
        let remote = vec![member_change("shared", FAULTY, 20)];
        let (for_local, for_remote) = changes_that_need_reincarnation(&local, &remote);
        assert_eq!(for_local.len(), 1);
        assert!(for_remote.is_empty());
        assert_eq!(for_local[0].status, SUSPECT);
        assert!(for_local[0].source.is_empty());
    }

    #[test]
    fn protocol_rate_uses_go_sample_percentile_and_minimum_floor() {
        let minimum = Duration::from_nanos(7);
        let oracle: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/ringpop_swim_runtime_oracle.json"
        ))
        .unwrap();
        let mut metrics = ProtocolMetrics::new(minimum);
        metrics.samples_nanos = vec![10, 20, 30, 40];
        metrics.adjust_rate(Duration::ZERO);
        // p50 position is 0.5 * (4 + 1) = 2.5; interpolate 20 and 30,
        // then multiply by two.
        assert_eq!(metrics.last_rate, Duration::from_nanos(50));
        assert_eq!(
            metrics.last_rate.as_nanos(),
            oracle["cadenceRateNanos"].as_u64().unwrap() as u128
        );
        metrics.samples_nanos = vec![1, 2, 3];
        metrics.adjust_rate(minimum);
        assert_eq!(metrics.last_rate, minimum);
    }

    #[test]
    fn protocol_delay_matches_go_target_and_floor() {
        let oracle: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/ringpop_swim_runtime_oracle.json"
        ))
        .unwrap();
        let start = tokio::time::Instant::now();
        let minimum = Duration::from_nanos(1);
        assert_eq!(
            next_protocol_delay(
                minimum,
                start,
                Duration::from_millis(20),
                start + Duration::from_millis(7),
            ),
            Duration::from_millis(13)
        );
        let mut metrics = ProtocolMetrics::new(minimum);
        assert_eq!(
            next_protocol_delay(
                minimum,
                start,
                Duration::from_millis(20),
                start + Duration::from_millis(30),
            ),
            minimum
        );
        assert_eq!(
            minimum.as_nanos(),
            oracle["cadenceBehindFloorNanos"].as_u64().unwrap() as u128
        );
        assert!(oracle["cadenceInitialInRange"].as_bool().unwrap());
        for _ in 0..12 {
            metrics.record(tokio::time::Instant::now(), Duration::from_nanos(1));
        }
        assert_eq!(
            metrics.samples_nanos.len(),
            oracle["cadenceSampleSize"].as_u64().unwrap() as usize
        );
    }

    #[derive(Default)]
    struct FakeJoinClock {
        elapsed: Duration,
        elapsed_reads: Vec<Duration>,
        sleeps: Vec<i64>,
        random: u64,
        sleep_started: Option<Arc<std::sync::atomic::AtomicBool>>,
        destroy_on_sleep: Option<RingpopNode>,
    }

    impl JoinClock for FakeJoinClock {
        fn reset(&mut self) {}
        fn elapsed(&mut self) -> Duration {
            let value = self.elapsed_reads.first().copied().unwrap_or(self.elapsed);
            if !self.elapsed_reads.is_empty() {
                self.elapsed_reads.remove(0);
            }
            value
        }
        fn sleep_nanos<'a>(
            &'a mut self,
            duration_nanos: i64,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
            self.sleeps.push(duration_nanos);
            if let Some(node) = self.destroy_on_sleep.take() {
                node.destroy();
            }
            if let Some(started) = &self.sleep_started {
                started.store(true, std::sync::atomic::Ordering::SeqCst);
                Box::pin(tokio::time::sleep(Duration::from_nanos(
                    duration_nanos.max(0) as u64,
                )))
            } else {
                Box::pin(std::future::ready(()))
            }
        }
        fn random_below(&mut self, upper_exclusive: u64) -> u64 {
            self.random.min(upper_exclusive - 1)
        }
    }

    #[test]
    fn exponential_join_delayer_matches_go_window_and_cap() {
        let mut clock = FakeJoinClock {
            random: 9,
            ..Default::default()
        };
        let mut delay =
            ExponentialJoinDelayer::new(Duration::from_millis(100), Duration::from_millis(250));
        assert_eq!(delay.delay_nanos(&mut clock), 9_000_000);
        assert_eq!(delay.delay_nanos(&mut clock), 109_000_000);
        assert_eq!(delay.delay_nanos(&mut clock), 209_000_000);
        assert_eq!(delay.delay_nanos(&mut clock), 250_000_000);
        assert_eq!(delay.delay_nanos(&mut clock), 250_000_000);
        assert!(delay.max_delay_reached());
        assert!(clock.sleeps.is_empty());
    }

    #[test]
    fn exponential_join_delayer_matches_zero_and_negative_duration_edges() {
        let mut clock = FakeJoinClock::default();
        let mut zero_initial = ExponentialJoinDelayer::new_signed(0, 1_000_000);
        assert_eq!(zero_initial.delay_nanos(&mut clock), 0);
        assert!(!zero_initial.max_delay_reached());
        let mut zero_cap = ExponentialJoinDelayer::new_signed(1_000_000, 0);
        assert_eq!(zero_cap.delay_nanos(&mut clock), 0);
        assert!(zero_cap.max_delay_reached());

        for (initial, max) in [(-1_000_000, 1_000_000), (1_000_000, -1_000_000)] {
            let mut invalid = ExponentialJoinDelayer::new_signed(initial, max);
            assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                invalid.delay_nanos(&mut clock);
            }))
            .is_err());
        }
    }

    #[tokio::test]
    async fn join_cluster_checks_success_before_deadline_and_keeps_negative_threshold() {
        use cadence_common::ringpop_bootstrap_selection::{BootstrapSelection, Options};
        let node = node();
        let selection = BootstrapSelection::new(
            "127.0.0.1:7933",
            &["127.0.0.2:7933".to_owned()],
            Options::from_signed(1, -1, -2, 1),
        );
        let mut sender = JoinGroupState::new(selection);
        let mut clock = FakeJoinClock::default();
        assert_eq!(
            join_cluster(&node, &mut sender, &mut clock).await.unwrap(),
            Vec::<String>::new()
        );
        assert_eq!(sender.attempts(), 1);
        assert!(clock.elapsed_reads.is_empty());
        assert!(clock.sleeps.is_empty());

        let self_only = BootstrapSelection::new("127.0.0.1:7933", &[], Options::default());
        assert!(self_only.single_node);
        let mut self_sender = JoinGroupState::new(self_only);
        assert_eq!(
            join_cluster(&node, &mut self_sender, &mut clock)
                .await
                .unwrap(),
            Vec::<String>::new()
        );
        assert_eq!(self_sender.attempts(), 0);
    }

    #[tokio::test]
    async fn join_cluster_deadline_is_strict_and_checked_after_each_group() {
        use cadence_common::ringpop_bootstrap_selection::{BootstrapSelection, Options};
        let node = node();
        let selection = BootstrapSelection::new(
            "127.0.0.1:7933",
            &["127.0.0.1:1".to_owned()],
            Options::from_signed(1, 1, 3, 1),
        );
        let mut sender = JoinGroupState::new(selection);
        let mut clock = FakeJoinClock {
            elapsed_reads: vec![Duration::from_nanos(1), Duration::from_nanos(2)],
            random: 0,
            ..Default::default()
        };
        let error = join_cluster(&node, &mut sender, &mut clock)
            .await
            .unwrap_err();
        assert_eq!(error.0, "join duration of 2ns exceeded max 1ns");
        assert!(error.1.is_empty());
        assert_eq!(sender.attempts(), 2);
        assert_eq!(clock.sleeps, vec![0]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn join_cluster_retry_sleep_yields_and_is_cancelled_on_drop() {
        use cadence_common::ringpop_bootstrap_selection::{BootstrapSelection, Options};
        use std::sync::atomic::{AtomicBool, Ordering};

        let node = node();
        let selection = BootstrapSelection::new(
            "127.0.0.1:7933",
            &["127.0.0.1:1".to_owned()],
            Options::from_signed(1, 1, 10_000, 1),
        );
        let mut sender = JoinGroupState::new(selection);
        let started = Arc::new(AtomicBool::new(false));
        let mut clock = FakeJoinClock {
            random: u64::MAX,
            sleep_started: Some(started.clone()),
            ..Default::default()
        };
        let join = tokio::spawn(async move { join_cluster(&node, &mut sender, &mut clock).await });
        tokio::time::timeout(Duration::from_millis(50), async {
            while !started.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("retry delay should start");

        // This task runs on the same single-thread executor while the retry is
        // sleeping. Dropping the JoinCluster future interrupts that delay.
        let other = tokio::spawn(async {
            tokio::task::yield_now().await;
            42
        });
        assert_eq!(other.await.unwrap(), 42);
        join.abort();
        assert!(join.await.unwrap_err().is_cancelled());
    }

    #[test]
    fn go_duration_string_preserves_fractional_precision() {
        assert_eq!(go_duration_string(1_001_234), "1.001234ms");
        assert_eq!(go_duration_string(1_000_001), "1.000001ms");
        assert_eq!(go_duration_string(1_234), "1.234µs");
        assert_eq!(go_duration_string(1_001), "1.001µs");
        assert_eq!(go_duration_string(1_000_000_001), "1.000000001s");
        assert_eq!(go_duration_string(-1_234), "-1.234µs");
    }

    #[tokio::test]
    async fn join_cluster_distinguishes_destroyed_from_stopped() {
        use cadence_common::ringpop_bootstrap_selection::{BootstrapSelection, Options};
        let node = node();
        node.stop();
        assert!(!node.is_destroyed());
        node.destroy();
        let selection = BootstrapSelection::new(
            "127.0.0.1:7933",
            &["127.0.0.1:1".to_owned()],
            Options::default(),
        );
        let mut sender = JoinGroupState::new(selection);
        let mut clock = FakeJoinClock::default();
        let error = join_cluster(&node, &mut sender, &mut clock)
            .await
            .unwrap_err();
        assert_eq!(error.0, "node destroyed while attempting to join cluster");
        assert!(error.1.is_empty());
        assert_eq!(sender.attempts(), 0);
    }

    #[tokio::test]
    async fn join_cluster_discards_partial_success_when_destroyed_between_groups() {
        use cadence_common::ringpop_bootstrap_selection::{BootstrapSelection, Options};
        let peer_member = Member {
            address: "success-peer:7933".into(),
            status: ALIVE.into(),
            incarnation: 1,
            labels: None,
        };
        let success_body = serde_json::to_vec(&JoinResponse {
            app: "cadence-matching".into(),
            coordinator: peer_member.address.clone(),
            membership: Some(vec![Change {
                address: peer_member.address.clone(),
                incarnation: 1,
                status: ALIVE.into(),
                labels: None,
                ..Default::default()
            }]),
            membership_checksum: 0,
        })
        .unwrap();
        let (success_addr, success_server) =
            outbound_test_server(RingpopHandler::new(move |_, _, _| {
                let body = success_body.clone();
                async move {
                    Ok(RingpopResponse {
                        headers: None,
                        body,
                    })
                }
            }))
            .await;
        let (failure_addr, failure_server) =
            outbound_test_server(RingpopHandler::new(|_, _, _| async move {
                Err(RingpopError::Application("seed rejected JOIN".into()))
            }))
            .await;

        let node = node();
        let selection = BootstrapSelection::new(
            "127.0.0.1:7933",
            &[success_addr, failure_addr],
            Options::from_signed(1_000_000_000, 2, 10_000, 1),
        );
        let mut sender = JoinGroupState::new(selection);
        let mut clock = FakeJoinClock {
            random: 0,
            destroy_on_sleep: Some(node.clone()),
            ..Default::default()
        };
        let error = join_cluster(&node, &mut sender, &mut clock)
            .await
            .unwrap_err();
        assert_eq!(error.0, "node destroyed while attempting to join cluster");
        assert!(
            error.1.is_empty(),
            "destroyed result must discard prior successes"
        );
        assert_eq!(sender.attempts(), 1);
        success_server.abort();
        failure_server.abort();
    }

    #[tokio::test]
    async fn join_cluster_deadline_retains_partial_success() {
        use cadence_common::ringpop_bootstrap_selection::{BootstrapSelection, Options};
        let peer_member = Member {
            address: "success-peer:7933".into(),
            status: ALIVE.into(),
            incarnation: 1,
            labels: None,
        };
        let success_body = serde_json::to_vec(&JoinResponse {
            app: "cadence-matching".into(),
            coordinator: peer_member.address.clone(),
            membership: Some(vec![Change {
                address: peer_member.address.clone(),
                incarnation: 1,
                status: ALIVE.into(),
                labels: None,
                ..Default::default()
            }]),
            membership_checksum: 0,
        })
        .unwrap();
        let (success_addr, success_server) =
            outbound_test_server(RingpopHandler::new(move |_, _, _| {
                let body = success_body.clone();
                async move {
                    Ok(RingpopResponse {
                        headers: None,
                        body,
                    })
                }
            }))
            .await;
        let (failure_addr, failure_server) =
            outbound_test_server(RingpopHandler::new(|_, _, _| async move {
                Err(RingpopError::Application("seed rejected JOIN".into()))
            }))
            .await;

        let node = node();
        let selection = BootstrapSelection::new(
            "127.0.0.1:7933",
            &[success_addr.clone(), failure_addr],
            Options::from_signed(1_000_000_000, -1, 2, 1),
        );
        let mut sender = JoinGroupState::new(selection);
        let mut clock = FakeJoinClock {
            elapsed: Duration::from_nanos(1),
            ..Default::default()
        };
        let error = join_cluster(&node, &mut sender, &mut clock)
            .await
            .unwrap_err();
        assert_eq!(error.0, "join duration of 1ns exceeded max -1ns");
        assert_eq!(error.1, vec![success_addr]);
        assert_eq!(sender.attempts(), 1);
        success_server.abort();
        failure_server.abort();
    }
    use crate::ringpop_tchannel::{RingpopHandler, RingpopResponse};
    use cadence_common::ringpop_wire::{Change, Ping, ALIVE};
    use std::collections::BTreeMap;

    #[derive(serde::Deserialize)]
    struct JoinAddJoinListOracle {
        cases: Vec<JoinAddJoinListCase>,
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct JoinAddJoinListCase {
        name: String,
        now_millis: i64,
        local: Member,
        members: Vec<Member>,
        input: Vec<Change>,
        final_members: Vec<Member>,
        checksum: u32,
        initial_queued: BTreeMap<String, JoinQueuedOracle>,
        queued: BTreeMap<String, JoinQueuedOracle>,
        max_p: usize,
    }

    #[derive(serde::Deserialize)]
    struct JoinQueuedOracle {
        change: Change,
        propagated: usize,
    }

    #[test]
    fn join_addjoinlist_matches_original_go_refutation_fixtures() {
        let oracle: JoinAddJoinListOracle = serde_json::from_str(include_str!(
            "../tests/fixtures/ringpop_join_addjoinlist.json"
        ))
        .expect("original Go AddJoinList fixture");
        assert_eq!(oracle.cases.len(), 4);
        for case in oracle.cases {
            let mut members = case.members.clone();
            members.sort_by(|a, b| a.address.cmp(&b.address));
            let local = case.local.clone();
            let initial_remote = members
                .into_iter()
                .filter(|member| member.address != local.address)
                .collect();
            let memberlist = Memberlist::from_snapshot(local.clone(), initial_remote);
            let mut disseminator = Disseminator::new(15);
            for (address, pending) in &case.initial_queued {
                disseminator.record(pending.change.clone());
                for _ in 0..pending.propagated {
                    disseminator.complete_sender(std::slice::from_ref(&pending.change), true);
                }
                assert_eq!(
                    disseminator.propagation_counts().get(address),
                    Some(&pending.propagated),
                    "{} initial counter for {address}",
                    case.name
                );
            }
            let test_node = RingpopNode::new("test", local.clone(), false);
            let mut state = test_node.state.lock().unwrap();
            state.memberlist = memberlist;
            state.disseminator = disseminator;
            apply_join_list(&mut state, &case.input, || case.now_millis, |_| 0);
            let mut got_members = state.memberlist.members().to_vec();
            got_members.sort_by(|a, b| a.address.cmp(&b.address));
            assert_eq!(got_members, case.final_members, "{} members", case.name);
            assert_eq!(
                state.memberlist.checksum(),
                case.checksum,
                "{} checksum",
                case.name
            );
            assert_eq!(
                state.disseminator.max_propagations(),
                case.max_p,
                "{} maxP",
                case.name
            );
            let got_counts = state.disseminator.propagation_counts();
            let expected_counts: BTreeMap<_, _> = case
                .queued
                .iter()
                .map(|(address, queued)| (address.clone(), queued.propagated))
                .collect();
            assert_eq!(got_counts, expected_counts, "{} counters", case.name);
            let got_changes: BTreeMap<_, _> = state
                .disseminator
                .issue_sender()
                .into_iter()
                .map(|change| (change.address.clone(), change))
                .collect();
            let expected_changes: BTreeMap<_, _> = case
                .queued
                .into_iter()
                .map(|(address, queued)| (address, queued.change))
                .collect();
            assert_eq!(
                got_changes, expected_changes,
                "{} queued changes",
                case.name
            );
        }
    }

    fn node() -> RingpopNode {
        RingpopNode::new(
            "cadence-matching",
            Member {
                address: "127.0.0.1:7933".into(),
                status: ALIVE.into(),
                incarnation: 100,
                labels: Some(BTreeMap::from([("serviceName".into(), "matching".into())])),
            },
            true,
        )
    }

    async fn outbound_test_server(
        handler: RingpopHandler,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = probe.local_addr().unwrap();
        drop(probe);
        let task = tokio::spawn(async move {
            let _ = crate::tchannel::serve_ringpop(address, handler).await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        (address.to_string(), task)
    }

    #[tokio::test]
    async fn join_group_waits_for_mixed_outcomes_and_merges_successful_membership() {
        use cadence_common::ringpop_bootstrap_selection::{BootstrapSelection, Options};
        let success_member = Member {
            address: "success-peer:7933".into(),
            status: ALIVE.into(),
            incarnation: 1,
            labels: None,
        };
        let success_body = serde_json::to_vec(&JoinResponse {
            app: "cadence-matching".into(),
            coordinator: "success-peer:7933".into(),
            membership: Some(vec![Change {
                address: success_member.address.clone(),
                incarnation: success_member.incarnation,
                status: ALIVE.into(),
                labels: None,
                ..Default::default()
            }]),
            membership_checksum: 0,
        })
        .unwrap();
        let (success_addr, success_server) =
            outbound_test_server(RingpopHandler::new(move |method, _, _| {
                let body = success_body.clone();
                async move {
                    assert_eq!(method, "/protocol/join");
                    tokio::time::sleep(Duration::from_millis(80)).await;
                    Ok(RingpopResponse {
                        headers: None,
                        body,
                    })
                }
            }))
            .await;
        let (failure_addr, failure_server) =
            outbound_test_server(RingpopHandler::new(|method, _, _| async move {
                assert_eq!(method, "/protocol/join");
                tokio::time::sleep(Duration::from_millis(80)).await;
                Err(RingpopError::Application("seed rejected JOIN".into()))
            }))
            .await;

        let node = node();
        let selection = BootstrapSelection::new(
            "127.0.0.1:7933",
            &[success_addr.clone(), failure_addr.clone()],
            Options::default(),
        );
        let start = tokio::time::Instant::now();
        let mut sender = JoinGroupState::new(selection);
        let result = node.join_group(&mut sender, &[]).await;
        let elapsed = start.elapsed();
        assert_eq!(result.successes.len(), 1);
        assert_eq!(result.failures.len(), 1);
        assert!(result.successes.contains(&success_addr));
        assert!(result.failures.contains(&failure_addr));
        assert!(
            elapsed < Duration::from_millis(150),
            "requests were serialized: {elapsed:?}"
        );
        assert!(
            elapsed >= Duration::from_millis(80),
            "group returned before all responses: {elapsed:?}"
        );
        assert!(node
            .membership_snapshot()
            .0
            .iter()
            .any(|m| m.address == "success-peer:7933"));
        assert_eq!(result.attempt, 1);
        assert_eq!(sender.attempts(), 1);
        success_server.abort();
        failure_server.abort();
    }

    #[tokio::test]
    async fn join_group_drop_cancels_children_and_stop_blocks_late_merge() {
        use cadence_common::ringpop_bootstrap_selection::{BootstrapSelection, Options};
        use tokio::sync::Notify;

        let remote_change = Change {
            address: "late-peer:7933".into(),
            incarnation: 9,
            status: ALIVE.into(),
            ..Default::default()
        };
        let body = serde_json::to_vec(&JoinResponse {
            app: "cadence-matching".into(),
            coordinator: "late-peer:7933".into(),
            membership: Some(vec![remote_change]),
            membership_checksum: 0,
        })
        .unwrap();

        // Dropping the public group future drops its JoinSet and aborts every
        // outbound request future; no late response can mutate membership.
        let request_seen = Arc::new(Notify::new());
        let handler_seen = request_seen.clone();
        let delayed_body = body.clone();
        let (address, server) = outbound_test_server(RingpopHandler::new(move |_, _, _| {
            let seen = handler_seen.clone();
            let body = delayed_body.clone();
            async move {
                seen.notify_one();
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(RingpopResponse {
                    headers: None,
                    body,
                })
            }
        }))
        .await;
        let cancelled_node = node();
        let before = cancelled_node.membership_snapshot();
        let mut sender = JoinGroupState::new(BootstrapSelection::new(
            "127.0.0.1:7933",
            std::slice::from_ref(&address),
            Options::default(),
        ));
        let task_node = cancelled_node.clone();
        let group = tokio::spawn(async move { task_node.join_group(&mut sender, &[]).await });
        request_seen.notified().await;
        group.abort();
        let _ = group.await;
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(cancelled_node.membership_snapshot(), before);
        server.abort();

        // A response that completes after Stop is decoded but cannot update
        // memberlist state, matching the Go memberlist.Update stopped guard.
        let response_seen = Arc::new(Notify::new());
        let handler_seen = response_seen.clone();
        let (address, server) = outbound_test_server(RingpopHandler::new(move |_, _, _| {
            let seen = handler_seen.clone();
            let body = body.clone();
            async move {
                seen.notify_one();
                tokio::time::sleep(Duration::from_millis(80)).await;
                Ok(RingpopResponse {
                    headers: None,
                    body,
                })
            }
        }))
        .await;
        let node = node();
        let before = node.membership_snapshot();
        let request_node = node.clone();
        let call = tokio::spawn(async move {
            request_node
                .join_seed(&address, Duration::from_secs(1))
                .await
        });
        response_seen.notified().await;
        node.stop();
        assert!(call.await.unwrap().is_ok());
        assert_eq!(node.membership_snapshot(), before);
        server.abort();
    }

    #[tokio::test]
    async fn outbound_join_empty_nil_failure_malformed_and_deadline_leave_state_unchanged() {
        let cases: Vec<(RingpopHandler, bool)> = vec![
            (
                RingpopHandler::new(|_, _, _| async {
                    Ok(RingpopResponse { headers: None, body: br#"{"app":"other","coordinator":"not-checked","membership":null,"membershipChecksum":999}"#.to_vec() })
                }),
                true,
            ),
            (
                RingpopHandler::new(|_, _, _| async {
                    Ok(RingpopResponse {
                        headers: None,
                        body: br#"{"membership":[]}"#.to_vec(),
                    })
                }),
                true,
            ),
            (
                RingpopHandler::new(|_, _, _| async {
                    Err(RingpopError::Application("seed rejected JOIN".into()))
                }),
                false,
            ),
            (
                RingpopHandler::new(|_, _, _| async {
                    Ok(RingpopResponse {
                        headers: None,
                        body: b"{malformed".to_vec(),
                    })
                }),
                false,
            ),
            (
                RingpopHandler::new(|_, _, _| async {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    Ok(RingpopResponse {
                        headers: None,
                        body: br#"{"membership":[]}"#.to_vec(),
                    })
                }),
                false,
            ),
        ];
        for (index, (handler, succeeds)) in cases.into_iter().enumerate() {
            let (address, server) = outbound_test_server(handler).await;
            let node = node();
            let before = node.membership_snapshot();
            let timeout = if index == 4 {
                std::time::Duration::from_millis(50)
            } else {
                std::time::Duration::from_millis(500)
            };
            let result = node.join_seed(&address, timeout).await;
            if succeeds {
                let response = result.expect("Go-compatible nil membership response");
                if index == 0 {
                    assert!(response.membership.is_none());
                } else {
                    assert_eq!(response.membership, Some(vec![]));
                }
            } else {
                assert!(result.is_err(), "case {index} unexpectedly succeeded");
            }
            assert_eq!(
                node.membership_snapshot(),
                before,
                "case {index} partially mutated membership"
            );
            server.abort();
        }

        let handler = RingpopHandler::new(|_, _, _| async {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            Ok(RingpopResponse {
                headers: None,
                body: br#"{"membership":[]}"#.to_vec(),
            })
        });
        let (address, server) = outbound_test_server(handler).await;
        let node = node();
        let before = node.membership_snapshot();
        let cancelled_node = node.clone();
        let call = tokio::spawn(async move {
            cancelled_node
                .join_seed(&address, std::time::Duration::from_secs(2))
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        call.abort();
        assert_eq!(
            node.membership_snapshot(),
            before,
            "dropping the JOIN future partially mutated membership"
        );
        server.abort();
    }

    #[tokio::test]
    async fn outbound_ping_applies_only_decoded_response_and_bumps_only_successful_sends() {
        let accepted = Change {
            source: "peer:7933".into(),
            source_incarnation: 9,
            address: "peer:7933".into(),
            incarnation: 9,
            status: ALIVE.into(),
            ..Default::default()
        };
        let responder = |body: Vec<u8>| {
            RingpopHandler::new(move |method, _, request| {
                let body = body.clone();
                async move {
                    assert_eq!(method, "/protocol/ping");
                    let request: Ping = serde_json::from_slice(&request).unwrap();
                    assert_eq!(request.app, "cadence-matching");
                    assert_eq!(request.source, "127.0.0.1:7933");
                    assert_eq!(request.source_incarnation, 100);
                    assert_eq!(request.checksum, 0);
                    let changes = request.changes.as_deref().unwrap();
                    assert_eq!(changes.len(), 1);
                    assert_eq!(changes[0].source, "");
                    assert_eq!(changes[0].source_incarnation, 0);
                    assert_eq!(changes[0].address, "127.0.0.1:7933");
                    assert_eq!(changes[0].incarnation, 100);
                    assert_eq!(changes[0].status, ALIVE);
                    Ok(RingpopResponse {
                        headers: None,
                        body,
                    })
                }
            })
        };

        // Both null and empty response changes are valid complete PING replies.
        for changes in ["null", "[]"] {
            let node = node();
            let body = format!(r#"{{"changes":{changes},"checksum":12,"source":"peer:7933","sourceIncarnationNumber":9,"app":""}}"#).into_bytes();
            let (address, server) = outbound_test_server(responder(body)).await;
            let result = node.ping(&address, Duration::from_secs(1)).await.unwrap();
            if changes == "null" {
                assert_eq!(result.changes, None);
            } else {
                assert_eq!(result.changes, Some(vec![]));
            }
            assert_eq!(node.dissemination_counts().get("127.0.0.1:7933"), Some(&1));
            server.abort();
        }

        // Successful response changes enter handleChanges' dissemination path.
        let response_node = node();
        let response = Ping {
            changes: Some(vec![accepted.clone()]),
            checksum: 7,
            source: "peer:7933".into(),
            source_incarnation: 9,
            app: String::new(),
        };
        let (address, server) =
            outbound_test_server(responder(serde_json::to_vec(&response).unwrap())).await;
        response_node
            .ping(&address, Duration::from_secs(1))
            .await
            .unwrap();
        assert!(response_node
            .membership_snapshot()
            .0
            .iter()
            .any(|member| member.address == "peer:7933"));
        assert!(response_node
            .dissemination_counts()
            .contains_key("peer:7933"));
        let queued_peer = response_node
            .state
            .lock()
            .unwrap()
            .disseminator
            .issue_sender()
            .into_iter()
            .find(|change| change.address == "peer:7933")
            .unwrap();
        assert_eq!(queued_peer.source, "peer:7933");
        assert_eq!(queued_peer.source_incarnation, 9);
        assert_eq!(queued_peer.incarnation, 9);
        assert_eq!(
            response_node.dissemination_counts().get("127.0.0.1:7933"),
            Some(&1)
        );
        server.abort();

        // Complete the sent snapshot callback before Update refutes the
        // response's local suspicion. The replacement local change starts p=0.
        let refutation_node = node();
        let response = Ping {
            changes: Some(vec![Change {
                source: "peer:7933".into(),
                source_incarnation: 9,
                address: "127.0.0.1:7933".into(),
                incarnation: 100,
                status: "suspect".into(),
                ..Default::default()
            }]),
            checksum: 0,
            source: "peer:7933".into(),
            source_incarnation: 9,
            app: String::new(),
        };
        let (address, server) =
            outbound_test_server(responder(serde_json::to_vec(&response).unwrap())).await;
        refutation_node
            .ping(&address, Duration::from_secs(1))
            .await
            .unwrap();
        let (members, _) = refutation_node.membership_snapshot();
        let local_incarnation = members
            .iter()
            .find(|member| member.address == "127.0.0.1:7933")
            .unwrap()
            .incarnation;
        assert!(local_incarnation > 100);
        assert_eq!(refutation_node.max_propagations(), 0);
        let queued = refutation_node
            .state
            .lock()
            .unwrap()
            .disseminator
            .issue_sender()
            .into_iter()
            .find(|change| change.address == "127.0.0.1:7933")
            .unwrap();
        assert_eq!(queued.source, "127.0.0.1:7933");
        assert_eq!(queued.source_incarnation, local_incarnation);
        assert_eq!(queued.status, ALIVE);
        assert_eq!(
            refutation_node.dissemination_counts().get("127.0.0.1:7933"),
            Some(&0)
        );
        server.abort();

        // Malformed JSON, application failures, timeout and caller cancellation
        // do not apply accepted changes or bump IssueAsSender's counters.
        let bad_responses: Vec<(RingpopHandler, Duration)> = vec![
            (
                RingpopHandler::new(|_, _, _| async {
                    Ok(RingpopResponse {
                        headers: None,
                        body: b"{".to_vec(),
                    })
                }),
                Duration::from_secs(1),
            ),
            (
                RingpopHandler::new(|_, _, _| async {
                    Err(RingpopError::Application("rejected".into()))
                }),
                Duration::from_secs(1),
            ),
            (
                RingpopHandler::new(|_, _, _| async {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    Ok(RingpopResponse {
                        headers: None,
                        body: b"{}".to_vec(),
                    })
                }),
                Duration::from_millis(30),
            ),
        ];
        for (handler, timeout) in bad_responses {
            let node = node();
            let before = node.membership_snapshot();
            let (address, server) = outbound_test_server(handler).await;
            assert!(node.ping(&address, timeout).await.is_err());
            assert_eq!(node.membership_snapshot(), before);
            assert_eq!(node.dissemination_counts().get("127.0.0.1:7933"), Some(&0));
            server.abort();
        }

        let cancellation_node = node();
        let before = cancellation_node.membership_snapshot();
        let (address, server) = outbound_test_server(RingpopHandler::new(|_, _, _| async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok(RingpopResponse {
                headers: None,
                body: br#"{"changes":null}"#.to_vec(),
            })
        }))
        .await;
        let caller = cancellation_node.clone();
        let call = tokio::spawn(async move { caller.ping(&address, Duration::from_secs(2)).await });
        tokio::time::sleep(Duration::from_millis(30)).await;
        call.abort();
        assert_eq!(cancellation_node.membership_snapshot(), before);
        assert_eq!(
            cancellation_node
                .dissemination_counts()
                .get("127.0.0.1:7933"),
            Some(&0)
        );
        server.abort();

        // Stopping after send does not suppress Go's successful sender callback,
        // while Memberlist.Update's stopped guard leaves membership untouched.
        let node = node();
        let (address, server) = outbound_test_server(RingpopHandler::new(|_, _, _| async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(RingpopResponse {
                headers: None,
                body: br#"{"changes":null}"#.to_vec(),
            })
        }))
        .await;
        let caller = node.clone();
        let call = tokio::spawn(async move { caller.ping(&address, Duration::from_secs(1)).await });
        tokio::time::sleep(Duration::from_millis(30)).await;
        let before = node.membership_snapshot();
        node.stop();
        call.await.unwrap().unwrap();
        assert_eq!(node.membership_snapshot(), before);
        assert_eq!(node.dissemination_counts().get("127.0.0.1:7933"), Some(&1));
        server.abort();
    }

    #[tokio::test]
    async fn outbound_ping_req_merges_false_responses_and_bumps_only_failed_sends() {
        let changed = Change {
            source: "target:7933".into(),
            source_incarnation: 7,
            address: "target:7933".into(),
            incarnation: 7,
            status: ALIVE.into(),
            ..Default::default()
        };
        for (status, expect_member) in [(false, true), (true, true)] {
            let node = node();
            let response = PingResponse {
                ping_status: status,
                target: "target:7933".into(),
                changes: Some(vec![changed.clone()]),
            };
            let handler = RingpopHandler::new(move |method, _, request| {
                let response = response.clone();
                async move {
                    assert_eq!(method, "/protocol/ping-req");
                    let request: PingRequest = serde_json::from_slice(&request).unwrap();
                    assert_eq!(request.source, "127.0.0.1:7933");
                    assert_eq!(request.source_incarnation, 100);
                    assert_eq!(request.target, "target:7933");
                    assert_eq!(request.checksum, 0);
                    Ok(RingpopResponse {
                        headers: None,
                        body: serde_json::to_vec(&response).unwrap(),
                    })
                }
            });
            let (address, server) = outbound_test_server(handler).await;
            let result = node
                .ping_request(&address, "target:7933", Duration::from_secs(1))
                .await
                .unwrap();
            assert_eq!(result.ping_status, status);
            assert_eq!(
                node.membership_snapshot()
                    .0
                    .iter()
                    .any(|m| m.address == "target:7933"),
                expect_member
            );
            assert_eq!(node.dissemination_counts().get("127.0.0.1:7933"), Some(&0));
            server.abort();
        }

        // Go's MakeCall invokes the bump callback on either RPC or JSON decode
        // error. A malformed response and an unavailable helper both count.
        for malformed in [true, false] {
            let node = node();
            if malformed {
                let handler = RingpopHandler::new(|_, _, _| async {
                    Ok(RingpopResponse {
                        headers: None,
                        body: br#"{"pingStatus":"not-a-bool"}"#.to_vec(),
                    })
                });
                let (address, server) = outbound_test_server(handler).await;
                assert!(node
                    .ping_request(&address, "target:7933", Duration::from_secs(1))
                    .await
                    .is_err());
                server.abort();
            } else {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap().to_string();
                drop(listener);
                assert!(node
                    .ping_request(&address, "target:7933", Duration::from_millis(100))
                    .await
                    .is_err());
            }
            assert_eq!(node.dissemination_counts().get("127.0.0.1:7933"), Some(&1));
        }

        // The MakeCall-equivalent task owns its callback after the waiting
        // caller is cancelled; its late JSON decode error still advances the
        // counter without applying response membership.
        let node = node();
        let handler = RingpopHandler::new(|_, _, _| async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(RingpopResponse {
                headers: None,
                body: b"{".to_vec(),
            })
        });
        let (address, server) = outbound_test_server(handler).await;
        let caller = node.clone();
        let call = tokio::spawn(async move {
            caller
                .ping_request(&address, "target:7933", Duration::from_secs(1))
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        call.abort();
        tokio::time::timeout(Duration::from_secs(2), async {
            while node.dissemination_counts().get("127.0.0.1:7933") != Some(&1) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("detached sender callback did not run after caller cancellation");
        assert_eq!(node.membership_snapshot().0.len(), 1);
        server.abort();
    }

    #[tokio::test]
    async fn indirect_ping_runs_helpers_concurrently_and_keeps_late_work_alive() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let node = node();
        let error_handler = RingpopHandler::new(|_, _, _| async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Err(RingpopError::Application("helper rejected request".into()))
        });
        let (error_address, error_server) = outbound_test_server(error_handler).await;
        let positive_handler = RingpopHandler::new(|_, _, _| async {
            tokio::time::sleep(Duration::from_millis(80)).await;
            Ok(RingpopResponse {
                headers: None,
                body: br#"{"pingStatus":true,"target":"target:7933","changes":[]}"#.to_vec(),
            })
        });
        let (positive_address, positive_server) = outbound_test_server(positive_handler).await;
        let late_done = Arc::new(AtomicBool::new(false));
        let late_done_handler = late_done.clone();
        let late_handler = RingpopHandler::new(move |_, _, _| {
            let done = late_done_handler.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(180)).await;
                done.store(true, Ordering::SeqCst);
                Ok(RingpopResponse {
                    headers: None,
                    body: br#"{"pingStatus":false,"target":"target:7933","changes":[]}"#.to_vec(),
                })
            }
        });
        let (late_address, late_server) = outbound_test_server(late_handler).await;
        {
            let mut state = node.state.lock().unwrap();
            let changes = [&error_address, &positive_address, &late_address]
                .into_iter()
                .map(|address| Change {
                    source: address.clone(),
                    source_incarnation: 1,
                    address: address.clone(),
                    incarnation: 1,
                    status: ALIVE.into(),
                    ..Default::default()
                })
                .collect::<Vec<_>>();
            state
                .memberlist
                .update(&changes, || unreachable!(), |len| len - 1);
        }

        let started = tokio::time::Instant::now();
        let (reached, errors) = node
            .indirect_ping("target:7933", 3, Duration::from_secs(1))
            .await;
        assert!(reached);
        assert_eq!(errors.len(), 1);
        assert!(started.elapsed() < Duration::from_millis(170));
        tokio::time::timeout(Duration::from_secs(1), async {
            while !late_done.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("early success cancelled a remaining helper");
        error_server.abort();
        positive_server.abort();
        late_server.abort();
    }

    #[tokio::test]
    async fn ping_request_selection_excludes_local_target_and_dead_and_zero_still_selects_one() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let node = node();
        let mut servers = Vec::new();
        let mut counters = Vec::new();
        for _ in [ALIVE, SUSPECT, ALIVE, "faulty"] {
            let counter = Arc::new(AtomicUsize::new(0));
            let handler_counter = counter.clone();
            let handler = RingpopHandler::new(move |method, _, _| {
                let counter = handler_counter.clone();
                async move {
                    assert_eq!(method, "/protocol/ping-req");
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(RingpopResponse {
                        headers: None,
                        body: br#"{"pingStatus":false,"target":"target","changes":[]}"#.to_vec(),
                    })
                }
            });
            let (address, server) = outbound_test_server(handler).await;
            servers.push(server);
            counters.push((address, counter));
        }

        let target = counters[2].0.clone();
        let changes = counters
            .iter()
            .zip([ALIVE, SUSPECT, ALIVE, "faulty"])
            .map(|((address, _), status)| Change {
                source: address.clone(),
                source_incarnation: 1,
                address: address.clone(),
                incarnation: 1,
                status: status.into(),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        node.state
            .lock()
            .unwrap()
            .memberlist
            .update(&changes, || unreachable!(), |len| len - 1);

        // Go appends a pingable member before checking len >= n, so amount 0
        // still launches exactly one call. Random selection may choose either
        // ALIVE or SUSPECT, but never local, target, or faulty.
        let mut results = node.send_ping_requests(&target, 0, Duration::from_secs(1));
        assert!(matches!(
            results.recv().await,
            Some(PingRequestResult::Response(_))
        ));
        assert!(results.recv().await.is_none());
        let selected = counters
            .iter()
            .filter(|(address, count)| address != &target && count.load(Ordering::SeqCst) > 0)
            .count();
        assert_eq!(selected, 1);
        assert_eq!(counters[2].1.load(Ordering::SeqCst), 0);
        assert_eq!(counters[3].1.load(Ordering::SeqCst), 0);
        let mut sparse = node.send_ping_requests("absent-target", 5, Duration::from_secs(1));
        assert!(matches!(
            sparse.recv().await,
            Some(PingRequestResult::Response(_))
        ));
        assert!(matches!(
            sparse.recv().await,
            Some(PingRequestResult::Response(_))
        ));
        assert!(matches!(
            sparse.recv().await,
            Some(PingRequestResult::Response(_))
        ));
        assert!(sparse.recv().await.is_none());

        for server in servers {
            server.abort();
        }
    }

    #[tokio::test]
    async fn production_ping_req_handler_relays_direct_ping_and_returns_ping_response() {
        let node = node();
        node.set_ready(true);
        let target = RingpopHandler::new(|method, _, body| async move {
            assert_eq!(method, "/protocol/ping");
            let request: Ping = serde_json::from_slice(&body).unwrap();
            let response = Ping {
                source: "target".into(),
                source_incarnation: 42,
                checksum: 7,
                changes: Some(vec![]),
                ..request
            };
            Ok(RingpopResponse {
                headers: None,
                body: serde_json::to_vec(&response).unwrap(),
            })
        });
        let (target_address, server) = outbound_test_server(target).await;
        let handler = node.handler();
        let request = PingRequest {
            source: "requester".into(),
            source_incarnation: 11,
            target: target_address,
            checksum: 0,
            changes: Some(vec![]),
        };
        let response = handler
            .call(
                "/protocol/ping-req".into(),
                None,
                serde_json::to_vec(&request).unwrap(),
            )
            .await
            .unwrap();
        let response: PingResponse = serde_json::from_slice(&response.body).unwrap();
        assert!(response.ping_status);
        assert_eq!(response.target, request.target);
        assert!(response.changes.is_some());
        assert_eq!(node.request_counts().ping_req, 1);
        server.abort();
    }

    #[tokio::test]
    async fn ping_req_direct_failures_do_not_advance_sender_or_merge_target_changes() {
        for mode in ["error", "deadline", "malformed"] {
            let node = RingpopNode::new_with_ping_timeout(
                "cadence-matching",
                Member {
                    address: "relay:7933".into(),
                    status: ALIVE.into(),
                    incarnation: 100,
                    labels: None,
                },
                true,
                Duration::from_millis(40),
            );
            node.set_ready(true);
            let target = RingpopHandler::new(move |_, _, _| async move {
                if mode == "deadline" {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    Ok(RingpopResponse {
                        headers: None,
                        body: br#"{"changes":[]}"#.to_vec(),
                    })
                } else if mode == "malformed" {
                    Ok(RingpopResponse {
                        headers: None,
                        body: br#"{"source":7}"#.to_vec(),
                    })
                } else {
                    Err(RingpopError::Application("target rejected ping".into()))
                }
            });
            let (target_address, server) = outbound_test_server(target).await;
            let request = PingRequest {
                source: "requester:7933".into(),
                source_incarnation: 11,
                target: target_address,
                checksum: 0,
                changes: Some(vec![Change {
                    address: "requester-member".into(),
                    status: ALIVE.into(),
                    incarnation: 5,
                    ..Change::default()
                }]),
            };
            let response = node
                .handler()
                .call(
                    "/protocol/ping-req".into(),
                    None,
                    serde_json::to_vec(&request).unwrap(),
                )
                .await
                .unwrap();
            let response: PingResponse = serde_json::from_slice(&response.body).unwrap();
            assert!(!response.ping_status, "mode={mode}");
            assert!(node
                .membership_snapshot()
                .0
                .iter()
                .any(|member| member.address == "requester-member"));
            // The response's IssueAsReceiver path advances the queued relay
            // change once; a failed direct sender call must not add another
            // propagation to that same entry.
            assert_eq!(node.dissemination_counts().get("relay:7933"), Some(&1));
            server.abort();
        }
    }

    #[tokio::test]
    async fn ping_req_empty_null_and_trailing_values_match_tchannel_json_decode() {
        let ready_node = node();
        ready_node.set_ready(true);
        for body in [b"".as_slice(), b"null", b"{} {}"] {
            let response = ready_node
                .handler()
                .call("/protocol/ping-req".into(), None, body.to_vec())
                .await
                .unwrap();
            let response: PingResponse = serde_json::from_slice(&response.body).unwrap();
            assert!(!response.ping_status);
            assert_eq!(response.target, "");
        }
        assert_eq!(ready_node.request_counts().ping_req, 3);
        for body in [b"   ".as_slice(), b"{\"target\":7}"] {
            assert!(ready_node
                .handler()
                .call("/protocol/ping-req".into(), None, body.to_vec())
                .await
                .is_err());
        }
        assert_eq!(ready_node.request_counts().ping_req, 3);

        let unready = node();
        for body in [b"".as_slice(), b"null"] {
            assert!(unready
                .handler()
                .call("/protocol/ping-req".into(), None, body.to_vec())
                .await
                .is_err());
        }
        assert_eq!(unready.request_counts().ping_req, 0);
    }

    #[tokio::test]
    async fn ping_req_stopped_and_destroyed_nodes_still_relay_without_memberlist_updates() {
        for destroyed in [false, true] {
            let node = RingpopNode::new(
                "cadence-matching",
                Member {
                    address: "relay:7933".into(),
                    status: ALIVE.into(),
                    incarnation: 100,
                    labels: None,
                },
                true,
            );
            node.set_ready(true);
            if destroyed {
                node.destroy();
            } else {
                node.stop();
            }
            let target = RingpopHandler::new(|_, _, body| async move {
                let ping: Ping = serde_json::from_slice(&body).unwrap();
                Ok(RingpopResponse {
                    headers: None,
                    body: serde_json::to_vec(&Ping {
                        changes: Some(vec![Change {
                            address: "target-member".into(),
                            status: ALIVE.into(),
                            incarnation: 8,
                            ..Change::default()
                        }]),
                        ..ping
                    })
                    .unwrap(),
                })
            });
            let (target_address, server) = outbound_test_server(target).await;
            let request = PingRequest {
                source: "requester:7933".into(),
                source_incarnation: 11,
                target: target_address,
                checksum: 0,
                changes: Some(vec![Change {
                    address: "requester-member".into(),
                    status: ALIVE.into(),
                    incarnation: 5,
                    ..Change::default()
                }]),
            };
            let raw = node
                .handler()
                .call(
                    "/protocol/ping-req".into(),
                    None,
                    serde_json::to_vec(&request).unwrap(),
                )
                .await
                .unwrap();
            let response: PingResponse = serde_json::from_slice(&raw.body).unwrap();
            assert!(response.ping_status, "destroyed={destroyed}");
            let members = node.membership_snapshot().0;
            assert!(!members.iter().any(|member| {
                member.address == "requester-member" || member.address == "target-member"
            }));
            server.abort();
        }
    }

    #[tokio::test]
    async fn outbound_join_refutes_local_equal_and_newer_suspect_or_faulty_changes() {
        for (status, incarnation) in [
            ("suspect", 100),
            ("suspect", 101),
            ("faulty", 100),
            ("faulty", 101),
        ] {
            let mut membership = vec![
                serde_json::json!({"source":"seed","sourceIncarnationNumber":1,"address":"127.0.0.1:7933","incarnationNumber":incarnation,"status":status,"labels":{"serviceName":"remote"},"timestamp":0}),
                serde_json::json!({"source":"seed","sourceIncarnationNumber":1,"address":"remote-accepted","incarnationNumber":1,"status":"alive","labels":{"serviceName":"remote"},"timestamp":0}),
            ];
            membership.extend((0..9).map(|index| {
                serde_json::json!({
                    "source":"seed","sourceIncarnationNumber":1,
                    "address":format!("peer-{index}"),"incarnationNumber":1,
                    "status":"alive","labels":{"serviceName":"remote"},"timestamp":0
                })
            }));
            let response = serde_json::json!({
                "app":"seed-app", "coordinator":"seed", "membership":membership,
                "membershipChecksum":0
            });
            let handler = RingpopHandler::new(move |_, _, _| {
                let response = response.clone();
                async move {
                    Ok(RingpopResponse {
                        headers: None,
                        body: serde_json::to_vec(&response).unwrap(),
                    })
                }
            });
            let (address, server) = outbound_test_server(handler).await;
            let node = node();
            {
                let mut state = node.state.lock().unwrap();
                state.disseminator.record(Change {
                    address: "remote-accepted".into(),
                    status: ALIVE.into(),
                    incarnation: 7,
                    ..Change::default()
                });
                state.disseminator.record(Change {
                    address: "untouched".into(),
                    status: ALIVE.into(),
                    incarnation: 8,
                    ..Change::default()
                });
            }
            node.join_seed(&address, std::time::Duration::from_secs(2))
                .await
                .unwrap();
            let state = node.state.lock().unwrap();
            let local = state.memberlist.member("127.0.0.1:7933").unwrap();
            assert_eq!(
                local.status, ALIVE,
                "{status} at incarnation {incarnation} did not refute"
            );
            assert!(local.incarnation > incarnation);
            assert_eq!(local.labels.as_ref().unwrap()["serviceName"], "matching");
            let pending = state.disseminator.propagation_counts();
            assert_eq!(pending.get("127.0.0.1:7933"), Some(&0));
            assert_eq!(pending.get("untouched"), Some(&0));
            assert!(!pending.contains_key("remote-accepted"));
            // Ten accepted remote alive members are pingable, so Go's
            // 15*ceil(log10(n+1)) adjusts maxP from 15 to 30.
            assert_eq!(state.disseminator.max_propagations(), 30);
            drop(state);
            server.abort();
        }
    }

    #[tokio::test]
    async fn outbound_join_response_after_stop_is_not_applied() {
        let handler = RingpopHandler::new(|_, _, _| async {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            Ok(RingpopResponse {
                headers: None,
                body: br#"{"membership":[{"source":"seed","sourceIncarnationNumber":1,"address":"peer","incarnationNumber":1,"status":"alive"}]}"#.to_vec(),
            })
        });
        let (address, server) = outbound_test_server(handler).await;
        let node = node();
        let before = node.membership_snapshot();
        let joiner = {
            let node = node.clone();
            tokio::spawn(async move {
                node.join_seed(&address, std::time::Duration::from_secs(2))
                    .await
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        node.stop();
        joiner.await.unwrap().unwrap();
        assert_eq!(node.membership_snapshot(), before);
        assert_eq!(node.dissemination_counts().get("127.0.0.1:7933"), Some(&0));
        assert!(!node.dissemination_counts().contains_key("peer"));
        server.abort();
    }

    #[test]
    fn join_returns_go_shaped_membership_and_checksum_without_setting_ready() {
        let node = node();
        let response = handle_join(
            &node.state,
            br#"{"app":"cadence-matching","source":"127.0.0.2:7933"}"#,
        )
        .unwrap();
        let response: JoinResponse = serde_json::from_slice(&response).unwrap();
        assert_eq!(response.coordinator, "127.0.0.1:7933");
        assert_eq!(response.membership.unwrap().len(), 1);
        assert_eq!(
            response.membership_checksum,
            node.state.lock().unwrap().memberlist.checksum()
        );
        assert!(!node.is_ready());
    }

    #[test]
    fn join_rejects_unknown_app_self_join_and_malformed_body_with_pinned_messages() {
        let node = node();
        for (body, expected) in [
            (br#"{"app":"wrong","source":"remote"}"#.as_slice(), "A node tried joining a different app cluster. The expected app, cadence-matching, did not match the actual app ,wrong"),
            (br#"{"app":"cadence-matching","source":"127.0.0.1:7933"}"#.as_slice(), "A node tried joining a cluster by attempting to join itself. The node ,127.0.0.1:7933, must join someone else."),
        ] {
            let error = handle_join(&node.state, body).unwrap_err();
            assert_eq!(error, RingpopError::Application(expected.into()));
        }
        assert!(matches!(
            handle_join(&node.state, b"{"),
            Err(RingpopError::System(_))
        ));
        assert_eq!(node.request_counts().join, 2);
    }

    #[test]
    fn ping_requires_readiness_then_merges_and_returns_receiver_checksum() {
        let node = node();
        let request = Ping {
            app: "cadence-matching".into(),
            source: "127.0.0.2:7933".into(),
            source_incarnation: 200,
            checksum: 1,
            changes: Some(vec![Change {
                source: "127.0.0.2:7933".into(),
                source_incarnation: 200,
                address: "127.0.0.2:7933".into(),
                incarnation: 200,
                status: ALIVE.into(),
                ..Default::default()
            }]),
        };
        let body = serde_json::to_vec(&request).unwrap();
        assert_eq!(
            handle_ping(&node.state, &body, || 101, |_| 0).unwrap_err(),
            RingpopError::Application("node is not ready to handle requests".into())
        );
        assert_eq!(node.request_counts().ping, 0);
        node.set_ready(true);
        let raw = node.handle_ping_with(&body, || 101, |_| 0).unwrap();
        let response: Ping = serde_json::from_slice(&raw).unwrap();
        assert_eq!(response.source, "127.0.0.1:7933");
        assert_eq!(response.source_incarnation, 100);
        assert_eq!(response.app, "");
        assert!(node
            .state
            .lock()
            .unwrap()
            .memberlist
            .member("127.0.0.2:7933")
            .is_some());
        assert_eq!(
            response.checksum,
            node.state.lock().unwrap().memberlist.checksum()
        );
        let initial_reply_changes = response.changes.unwrap();
        assert_eq!(initial_reply_changes.len(), 1);
        assert_eq!(
            initial_reply_changes[0].source, "",
            "Go records its initial local delta without a source"
        );
        assert_eq!(node.dissemination_counts().get("127.0.0.1:7933"), Some(&1));
        // Drain the constructor's local delta exactly at Ringpop's 15-ping
        // propagation bound. The next checksum mismatch has no sender delta
        // to return and therefore exercises MembershipAsChanges full sync.
        for _ in 0..14 {
            node.handle_ping_with(&body, || 101, |_| 0).unwrap();
        }
        let full_sync_raw = node.handle_ping_with(&body, || 101, |_| 0).unwrap();
        let full_sync: Ping = serde_json::from_slice(&full_sync_raw).unwrap();
        let reply_changes = full_sync.changes.unwrap();
        assert_eq!(
            reply_changes.len(),
            2,
            "checksum mismatch with no sender deltas triggers full sync"
        );
        assert!(reply_changes
            .iter()
            .any(|c| c.address == "127.0.0.2:7933" && c.source == "127.0.0.1:7933"));
        assert!(reply_changes
            .iter()
            .any(|c| c.address == "127.0.0.1:7933" && c.status == ALIVE));
        assert_eq!(node.request_counts().ping, 16);
    }

    #[test]
    fn ping_app_errors_match_ringpop_and_malformed_json_is_rejected() {
        let node = node();
        node.set_ready(true);
        let unknown = serde_json::to_vec(&Ping {
            source: "remote".into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            handle_ping(&node.state, &unknown, || 1, |_| 0).unwrap_err(),
            RingpopError::Application("Pinged ringpop requires app name".into())
        );
        let wrong = serde_json::to_vec(&Ping {
            app: "other".into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            handle_ping(&node.state, &wrong, || 1, |_| 0).unwrap_err(),
            RingpopError::Application("Pinged ringpop has a different app name".into())
        );
        assert!(matches!(
            handle_ping(&node.state, b"{", || 1, |_| 0),
            Err(RingpopError::System(_))
        ));
        assert_eq!(node.request_counts().ping, 0);
    }

    #[test]
    fn original_go_handler_json_fixture_matches_join_and_ping_responses() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixtures/ringpop_inbound_go.json"))
                .unwrap();
        let node = RingpopNode::new(
            "test",
            Member {
                address: "<local>".into(),
                status: ALIVE.into(),
                incarnation: 0,
                labels: None,
            },
            true,
        );
        let join = handle_join(
            &node.state,
            br#"{"app":"test","source":"127.0.0.2:7933","incarnationNumber":200}"#,
        )
        .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&join).unwrap(),
            fixture["join"]
        );
        for (key, body) in [
            (
                "join_wrong_app",
                br#"{"app":"wrong","source":"127.0.0.2:7933"}"#.as_slice(),
            ),
            (
                "join_self",
                br#"{"app":"test","source":"<local>"}"#.as_slice(),
            ),
        ] {
            let error = handle_join(&node.state, body).unwrap_err();
            assert_eq!(
                error,
                RingpopError::Application(fixture[key].as_str().unwrap().to_owned())
            );
        }
        assert_eq!(
            handle_ping(&node.state, br#"{}"#, || 0, |_| 0).unwrap_err(),
            RingpopError::Application(fixture["ping_before_ready"].as_str().unwrap().to_owned())
        );
        node.set_ready(true);
        let ping = handle_ping(&node.state, br#"{"source":"127.0.0.2:7933","sourceIncarnationNumber":200,"checksum":0,"changes":null,"app":"test"}"#, || 0, |_| 0).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&ping).unwrap(),
            fixture["ping"]
        );
        let wrong_ping = br#"{"app":"wrong","source":"127.0.0.2:7933"}"#;
        assert_eq!(
            handle_ping(&node.state, wrong_ping, || 0, |_| 0).unwrap_err(),
            RingpopError::Application(fixture["ping_wrong_app"].as_str().unwrap().to_owned())
        );
    }

    #[test]
    fn join_snapshot_preserves_member_labels_and_tombstone_wire_normalization() {
        let node = node();
        node.set_ready(true);
        let labels = BTreeMap::from([
            ("zone".to_owned(), "west".to_owned()),
            ("rack".to_owned(), "r2".to_owned()),
        ]);
        let alive = Ping {
            app: "cadence-matching".into(),
            source: "127.0.0.2:7933".into(),
            source_incarnation: 200,
            changes: Some(vec![Change {
                source: "127.0.0.2:7933".into(),
                source_incarnation: 200,
                address: "127.0.0.2:7933".into(),
                incarnation: 200,
                status: ALIVE.into(),
                labels: Some(labels.clone()),
                ..Default::default()
            }]),
            ..Default::default()
        };
        node.handle_ping_with(
            &serde_json::to_vec(&alive).unwrap(),
            || unreachable!(),
            |_| 0,
        )
        .unwrap();
        let tombstone = Ping {
            app: "cadence-matching".into(),
            source: "127.0.0.3:7933".into(),
            source_incarnation: 300,
            changes: Some(vec![Change {
                source: "127.0.0.3:7933".into(),
                source_incarnation: 300,
                address: "127.0.0.2:7933".into(),
                incarnation: 201,
                status: "faulty".into(),
                tombstone: true,
                labels: Some(labels.clone()),
                ..Default::default()
            }]),
            ..Default::default()
        };
        node.handle_ping_with(
            &serde_json::to_vec(&tombstone).unwrap(),
            || unreachable!(),
            |_| 0,
        )
        .unwrap();
        let raw = handle_join(
            &node.state,
            br#"{"app":"cadence-matching","source":"127.0.0.4:7933"}"#,
        )
        .unwrap();
        let response: JoinResponse = serde_json::from_slice(&raw).unwrap();
        let remote = response
            .membership
            .unwrap()
            .into_iter()
            .find(|change| change.address == "127.0.0.2:7933")
            .unwrap();
        assert_eq!(remote.status, "faulty");
        assert!(remote.tombstone);
        assert_eq!(remote.labels, Some(labels));
        assert_eq!(remote.source, "127.0.0.1:7933");
        assert_eq!(remote.source_incarnation, 100);
    }

    #[test]
    fn ping_local_refutation_uses_injected_incarnation_in_response_and_full_state() {
        let node = node();
        node.set_ready(true);
        let request = Ping {
            app: "cadence-matching".into(),
            source: "127.0.0.2:7933".into(),
            source_incarnation: 200,
            changes: Some(vec![Change {
                source: "127.0.0.2:7933".into(),
                source_incarnation: 200,
                address: "127.0.0.1:7933".into(),
                incarnation: 101,
                status: "suspect".into(),
                ..Default::default()
            }]),
            ..Default::default()
        };
        let raw = node
            .handle_ping_with(
                &serde_json::to_vec(&request).unwrap(),
                || 111,
                |_| unreachable!(),
            )
            .unwrap();
        let response: Ping = serde_json::from_slice(&raw).unwrap();
        assert_eq!(response.source_incarnation, 111);
        assert_eq!(
            node.state
                .lock()
                .unwrap()
                .memberlist
                .member("127.0.0.1:7933")
                .unwrap()
                .incarnation,
            111
        );
        assert_eq!(
            response.checksum,
            node.state.lock().unwrap().memberlist.checksum()
        );
    }

    #[tokio::test]
    async fn handler_dispatches_only_registered_go_method_names_and_stop_preserves_ready() {
        let node = node();
        node.set_ready(true);
        let handler = node.handler();
        let join = handler
            .call(
                "/protocol/join".into(),
                None,
                br#"{"app":"cadence-matching","source":"remote"}"#.to_vec(),
            )
            .await
            .unwrap();
        assert!(serde_json::from_slice::<JoinResponse>(&join.body).is_ok());
        for method in ["join", "ping"] {
            assert!(matches!(
                handler.call(method.into(), None, b"{}".to_vec()).await,
                Err(RingpopError::System(_))
            ));
        }
        node.stop();
        assert_eq!(node.lifecycle(), RingpopLifecycle::Stopped);
        assert!(node.is_ready(), "Go Stop leaves the Ready flag intact");
        let ping = br#"{"app":"cadence-matching","source":"remote"}"#;
        assert!(node.handle_ping_with(ping, || 1, |_| 0).is_ok());
        node.set_ready(false);
        assert_eq!(node.lifecycle(), RingpopLifecycle::Stopped);
        assert_eq!(
            node.handle_ping_with(ping, || 1, |_| 0).unwrap_err(),
            RingpopError::Application("node is not ready to handle requests".into())
        );
    }

    #[tokio::test(start_paused = true)]
    async fn accepted_suspect_cascades_through_faulty_tombstone_and_eviction() {
        let node = RingpopNode::new_with_swim_config(
            "test",
            Member {
                address: "local:7933".into(),
                status: ALIVE.into(),
                incarnation: 10,
                labels: None,
            },
            false,
            Duration::from_millis(1),
            Duration::from_millis(1),
            3,
            Duration::from_millis(1),
            StateTimeouts {
                suspect_ns: 5,
                faulty_ns: 10,
                tombstone_ns: 5,
            },
        );
        node.set_ready(true);
        let request = Ping {
            app: "test".into(),
            source: "seed:7933".into(),
            source_incarnation: 1,
            changes: Some(vec![Change {
                source: "seed:7933".into(),
                source_incarnation: 1,
                address: "target:7933".into(),
                incarnation: 1,
                status: ALIVE.into(),
                ..Default::default()
            }]),
            ..Default::default()
        };
        node.handle_ping_with(&serde_json::to_vec(&request).unwrap(), || 11, |_| 0)
            .unwrap();
        node.make_suspect("target:7933", 1);
        let mut trace = vec!["suspect".to_owned()];

        tokio::time::advance(Duration::from_nanos(5)).await;
        {
            let mut state = node.state.lock().unwrap();
            let now = state.monotonic_now_ns();
            state.apply_due_transitions(now, || 12, |_| 0);
            assert_eq!(
                state.memberlist.member("target:7933").unwrap().status,
                FAULTY
            );
            assert_eq!(
                state
                    .state_transitions
                    .timers()
                    .get("target:7933")
                    .unwrap()
                    .deadline_ns,
                15
            );
            trace.push(
                state
                    .memberlist
                    .member("target:7933")
                    .unwrap()
                    .status
                    .clone(),
            );
        }
        tokio::time::advance(Duration::from_nanos(9)).await;
        {
            let mut state = node.state.lock().unwrap();
            let now = state.monotonic_now_ns();
            state.apply_due_transitions(now, || 12, |_| 0);
            assert_eq!(
                state.memberlist.member("target:7933").unwrap().status,
                FAULTY
            );
            trace.push(
                state
                    .memberlist
                    .member("target:7933")
                    .unwrap()
                    .status
                    .clone(),
            );
        }
        tokio::time::advance(Duration::from_nanos(1)).await;
        {
            let mut state = node.state.lock().unwrap();
            let now = state.monotonic_now_ns();
            state.apply_due_transitions(now, || 12, |_| 0);
            assert_eq!(
                state.memberlist.member("target:7933").unwrap().status,
                TOMBSTONE
            );
            trace.push(
                state
                    .memberlist
                    .member("target:7933")
                    .unwrap()
                    .status
                    .clone(),
            );
        }
        tokio::time::advance(Duration::from_nanos(5)).await;
        {
            let mut state = node.state.lock().unwrap();
            let now = state.monotonic_now_ns();
            state.apply_due_transitions(now, || 12, |_| 0);
            assert!(state.memberlist.member("target:7933").is_none());
            assert!(state.memberlist.member("local:7933").is_some());
            trace.push("evicted".to_owned());
        }
        let oracle: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/ringpop_swim_runtime_oracle.json"
        ))
        .unwrap();
        assert_eq!(serde_json::to_value(trace).unwrap(), oracle["cascade"]);
    }

    #[tokio::test(start_paused = true)]
    async fn alive_cancels_timer_and_stop_restart_does_not_restore_cleared_timer() {
        let node = RingpopNode::new_with_swim_config(
            "test",
            Member {
                address: "local:7933".into(),
                status: ALIVE.into(),
                incarnation: 10,
                labels: None,
            },
            false,
            Duration::from_millis(1),
            Duration::from_millis(1),
            3,
            Duration::from_secs(3600),
            StateTimeouts {
                suspect_ns: 5,
                faulty_ns: 10,
                tombstone_ns: 5,
            },
        );
        node.set_ready(true);
        let request = Ping {
            app: "test".into(),
            source: "seed:7933".into(),
            source_incarnation: 1,
            changes: Some(vec![Change {
                source: "seed:7933".into(),
                source_incarnation: 1,
                address: "target:7933".into(),
                incarnation: 1,
                status: SUSPECT.into(),
                ..Default::default()
            }]),
            ..Default::default()
        };
        node.handle_ping_with(&serde_json::to_vec(&request).unwrap(), || 11, |_| 0)
            .unwrap();
        let alive = Ping {
            app: "test".into(),
            source: "seed:7933".into(),
            source_incarnation: 2,
            changes: Some(vec![Change {
                source: "seed:7933".into(),
                source_incarnation: 2,
                address: "target:7933".into(),
                incarnation: 2,
                status: ALIVE.into(),
                ..Default::default()
            }]),
            ..Default::default()
        };
        node.handle_ping_with(&serde_json::to_vec(&alive).unwrap(), || 12, |_| 0)
            .unwrap();
        let alive_status = node
            .membership_snapshot()
            .0
            .into_iter()
            .find(|member| member.address == "target:7933")
            .unwrap()
            .status;
        let oracle: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/ringpop_swim_runtime_oracle.json"
        ))
        .unwrap();
        assert_eq!(
            serde_json::to_value(vec![alive_status]).unwrap(),
            oracle["cancelled"]
        );
        assert!(!node
            .state
            .lock()
            .unwrap()
            .state_transitions
            .timers()
            .contains_key("target:7933"));

        node.make_suspect("target:7933", 2);
        node.stop();
        tokio::time::advance(Duration::from_secs(1)).await;
        {
            let mut state = node.state.lock().unwrap();
            let now = state.monotonic_now_ns();
            state.apply_due_transitions(now, || 13, |_| 0);
            assert_eq!(
                state.memberlist.member("target:7933").unwrap().status,
                SUSPECT
            );
        }
        node.start_swim().unwrap();
        assert!(node.is_ready());
        tokio::time::advance(Duration::from_secs(1)).await;
        {
            let mut state = node.state.lock().unwrap();
            let now = state.monotonic_now_ns();
            state.apply_due_transitions(now, || 13, |_| 0);
            assert_eq!(
                state.memberlist.member("target:7933").unwrap().status,
                SUSPECT
            );
        }
        node.destroy();

        assert_eq!(
            serde_json::to_value(vec!["suspect", "suspect"]).unwrap(),
            oracle["stopped"]
        );
    }

    #[tokio::test]
    async fn start_stop_restart_destroy_own_both_runtime_tasks_without_changing_ready() {
        let node = node();
        node.set_ready(true);
        node.start_swim().unwrap();
        assert_eq!(node.runtime.lock().unwrap().tasks.len(), 3);
        assert!(node.is_ready());
        node.stop();
        assert!(node.runtime.lock().unwrap().tasks.is_empty());
        assert!(node.is_ready());
        node.start_swim().unwrap();
        assert_eq!(node.runtime.lock().unwrap().tasks.len(), 3);
        node.destroy();
        assert!(node.runtime.lock().unwrap().tasks.is_empty());
        assert!(node.is_destroyed());
        assert!(node.is_ready());
        tokio::task::yield_now().await;
    }

    #[tokio::test(start_paused = true)]
    async fn owned_timer_loop_applies_due_transition_and_cascades_eviction() {
        let node = RingpopNode::new_with_swim_config(
            "test",
            Member {
                address: "local:7933".into(),
                status: ALIVE.into(),
                incarnation: 10,
                labels: None,
            },
            false,
            Duration::from_millis(1),
            Duration::from_millis(1),
            3,
            Duration::from_secs(3600),
            StateTimeouts {
                suspect_ns: 5_000_000,
                faulty_ns: 5_000_000,
                tombstone_ns: 5_000_000,
            },
        );
        node.set_ready(true);
        let request = Ping {
            app: "test".into(),
            source: "seed:7933".into(),
            source_incarnation: 1,
            changes: Some(vec![Change {
                source: "seed:7933".into(),
                source_incarnation: 1,
                address: "target:7933".into(),
                incarnation: 1,
                status: FAULTY.into(),
                ..Default::default()
            }]),
            ..Default::default()
        };
        node.handle_ping_with(&serde_json::to_vec(&request).unwrap(), || 11, |_| 0)
            .unwrap();
        node.start_swim().unwrap();
        // Let the owned timer task register its Tokio sleep before advancing.
        // Tokio's timer wheel resolves deadlines at millisecond granularity;
        // sub-millisecond logical timeouts can therefore miss this wakeup.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(5)).await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            node.membership_snapshot()
                .0
                .iter()
                .find(|member| member.address == "target:7933")
                .unwrap()
                .status,
            TOMBSTONE
        );
        tokio::time::advance(Duration::from_millis(5)).await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(node
            .membership_snapshot()
            .0
            .iter()
            .all(|member| member.address != "target:7933"));
        node.destroy();
    }
}
