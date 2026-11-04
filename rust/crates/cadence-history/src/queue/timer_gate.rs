//! Port of `go/common/clock/timer_gate.go` (`TimerGate`, `LocalTimerGate`, `EventTimerGate`) as used by
//! the timer queue processors. One struct covers both Go implementations:
//! * local gate (`clock.NewTimerGate(timeSource)`): fires when wall clock reaches the armed time;
//! * event gate (`clock.NewEventTimerGate(currentTime)`): fires when `set_current_time` moves the
//!   remote clock to or past the armed time.
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

use super::deps::Time;

/// Source of "now" for local gates (go `clock.TimeSource`).
pub type NowFn = Arc<dyn Fn() -> Time + Send + Sync>;

struct GateState {
    /// armed fire time; `None` when idle (go: timer not armed / already fired)
    next: Option<Time>,
    /// event gate only: the remote cluster's current time
    current: Time,
    stopped: bool,
}

pub struct TimerGate {
    now: Option<NowFn>,
    state: Mutex<GateState>,
    notify: Notify,
}

impl TimerGate {
    /// Go `clock.NewTimerGate(timeSource)`.
    pub fn new_local(now: NowFn) -> Arc<Self> {
        Arc::new(TimerGate {
            now: Some(now),
            state: Mutex::new(GateState { next: None, current: Time::ZERO, stopped: false }),
            notify: Notify::new(),
        })
    }

    /// Go `clock.NewEventTimerGate(currentTime)`.
    pub fn new_event(current_time: Time) -> Arc<Self> {
        Arc::new(TimerGate {
            now: None,
            state: Mutex::new(GateState { next: None, current: current_time, stopped: false }),
            notify: Notify::new(),
        })
    }

    /// Go `Update`: arms the gate for `t` unless an earlier fire time is pending.
    /// Returns true when the gate was (re)armed.
    pub fn update(&self, t: Time) -> bool {
        let mut s = self.state.lock().unwrap();
        if s.stopped {
            return false;
        }
        match s.next {
            Some(n) if !t.before(n) => false,
            _ => {
                s.next = Some(t);
                self.notify.notify_one();
                true
            }
        }
    }

    /// Go `EventTimerGate.SetCurrentTime`: moves the remote clock forward (never backwards).
    pub fn set_current_time(&self, t: Time) {
        let mut s = self.state.lock().unwrap();
        if t.after(s.current) {
            s.current = t;
            self.notify.notify_one();
        }
    }

    /// Go `Close`/`Stop`.
    pub fn stop(&self) {
        self.state.lock().unwrap().stopped = true;
        self.notify.notify_one();
    }

    fn current(&self, s: &GateState) -> Time {
        match &self.now {
            Some(f) => f(),
            None => s.current,
        }
    }

    /// Go `<-gate.Chan()`; resolves when the gate fires, pending forever once stopped.
    pub async fn fired(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            // register interest before inspecting state so no wake-up is lost
            notified.as_mut().enable();
            let (next, now, stopped) = {
                let s = self.state.lock().unwrap();
                (s.next, self.current(&s), s.stopped)
            };
            if stopped {
                std::future::pending::<()>().await;
            }
            match next {
                None => notified.await,
                Some(t) => {
                    if !now.before(t) {
                        let mut s = self.state.lock().unwrap();
                        if s.next == Some(t) {
                            s.next = None;
                        }
                        return;
                    }
                    if self.now.is_some() {
                        let wait = Duration::from_nanos(t.sub_nanos(now).max(0) as u64);
                        tokio::select! {
                            _ = tokio::time::sleep(wait) => {}
                            _ = &mut notified => {}
                        }
                    } else {
                        notified.await;
                    }
                }
            }
        }
    }
}
