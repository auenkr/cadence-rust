//! Port of queuev2/pause_controller.go

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;

use super::deps::{zero_time, Time, TimeSource, Timer};

/// PauseController allows pausing and resuming a background job.
pub trait PauseController: Send + Sync {
    /// Stop the controller without notifying subscribers; no-op if not paused.
    fn stop(&self);
    /// Pause for the given duration. Zero/negative durations are ignored. If already
    /// paused, the pause can only be extended.
    fn pause(&self, d: Duration);
    /// Resume immediately; no-op if not paused.
    fn resume(&self);
    fn is_paused(&self) -> bool;
    fn subscribe(&self, id: &str, ch: mpsc::Sender<()>);
    fn unsubscribe(&self, id: &str);
}

struct PauseState {
    subscribers: HashMap<String, mpsc::Sender<()>>,
    pause_until: Time,
    timer: Option<Arc<dyn Timer>>,
}

pub struct PauseControllerImpl {
    time_source: Arc<dyn TimeSource>,
    state: Arc<Mutex<PauseState>>,
}

pub fn new_pause_controller(time_source: Arc<dyn TimeSource>) -> Arc<dyn PauseController> {
    Arc::new(PauseControllerImpl {
        time_source,
        state: Arc::new(Mutex::new(PauseState {
            subscribers: HashMap::new(),
            pause_until: zero_time(),
            timer: None,
        })),
    })
}

fn notify_subscribers(st: &PauseState) {
    for ch in st.subscribers.values() {
        let _ = ch.try_send(());
    }
}

fn stop_timer_locked(st: &PauseState) {
    if let Some(t) = &st.timer {
        t.stop();
    }
}

impl PauseController for PauseControllerImpl {
    fn is_paused(&self) -> bool {
        self.state.lock().unwrap().timer.is_some()
    }

    fn subscribe(&self, id: &str, ch: mpsc::Sender<()>) {
        self.state
            .lock()
            .unwrap()
            .subscribers
            .insert(id.to_string(), ch);
    }

    fn unsubscribe(&self, id: &str) {
        self.state.lock().unwrap().subscribers.remove(id);
    }

    fn stop(&self) {
        let mut st = self.state.lock().unwrap();
        stop_timer_locked(&st);
        st.timer = None;
        st.pause_until = zero_time();
    }

    fn pause(&self, duration: Duration) {
        if duration.is_zero() {
            return;
        }
        let mut st = self.state.lock().unwrap();
        let new_pause_until = self.time_source.now()
            + chrono::Duration::from_std(duration).unwrap_or(chrono::Duration::MAX);
        if new_pause_until < st.pause_until {
            return;
        }
        stop_timer_locked(&st);
        let shared = self.state.clone();
        st.timer = Some(self.time_source.after_func(
            duration,
            Box::new(move || {
                let mut st = shared.lock().unwrap();
                st.timer = None;
                st.pause_until = zero_time();
                notify_subscribers(&st);
            }),
        ));
        st.pause_until = new_pause_until;
    }

    fn resume(&self) {
        let mut st = self.state.lock().unwrap();
        if st.timer.is_none() {
            return;
        }
        stop_timer_locked(&st);
        st.timer = None;
        st.pause_until = zero_time();
        notify_subscribers(&st);
    }
}
