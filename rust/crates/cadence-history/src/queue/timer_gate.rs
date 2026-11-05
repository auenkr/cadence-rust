//! Ports of `go/common/clock/timer_gate.go` (`NewTimerGate`) and
//! `go/common/clock/event_timer_gate.go` (`NewEventTimerGate`) in the shape the timer queue
//! processor needs (`Chan()` becomes `recv()`).
//!
//! Rust-forced differences: Go's `time.Timer` is a tokio sleep task guarded by a generation counter.
use std::sync::{Arc, Mutex as StdMutex};

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::deps::Time;

/// Go `clock.TimerGate`.
#[async_trait]
pub trait TimerGate: Send + Sync {
    /// Go `<-gate.Chan()`; waits for the next fire.
    async fn recv(&self) -> Time;
    /// Go `Update`: true when the gate was (re)armed for `t`.
    fn update(&self, t: Time) -> bool;
    /// Go `Stop`.
    fn stop(&self);
}

pub type NowFn = Arc<dyn Fn() -> Time + Send + Sync>;

struct RealInner {
    fire_time: Time,
    active: bool,
    gen: u64,
    handle: Option<JoinHandle<()>>,
}

/// Go `timerGateImpl`.
pub struct RealTimerGate {
    now: NowFn,
    inner: Arc<StdMutex<RealInner>>,
    tx: mpsc::Sender<Time>,
    rx: tokio::sync::Mutex<mpsc::Receiver<Time>>,
}

impl RealTimerGate {
    /// Go `NewTimerGate`.
    pub fn new(now: NowFn) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(1);
        Arc::new(RealTimerGate {
            now,
            inner: Arc::new(StdMutex::new(RealInner { fire_time: Time::ZERO, active: false, gen: 0, handle: None })),
            tx,
            rx: tokio::sync::Mutex::new(rx),
        })
    }

    /// Go `FireAfter`.
    pub fn fire_after(&self, t: Time) -> bool {
        self.inner.lock().unwrap().fire_time.after(t)
    }

    fn drain(&self) {
        if let Ok(mut rx) = self.rx.try_lock() {
            while rx.try_recv().is_ok() {}
        }
    }

    fn arm(&self, g: &mut RealInner, fire_time: Time) {
        g.gen += 1;
        g.active = true;
        g.fire_time = fire_time;
        let my_gen = g.gen;
        let delay = fire_time.sub_nanos((self.now)()).max(0) as u64;
        let inner = self.inner.clone();
        let tx = self.tx.clone();
        let now = self.now.clone();
        g.handle = Some(tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_nanos(delay)).await;
            let mut g = inner.lock().unwrap();
            if g.gen == my_gen && g.active {
                g.active = false;
                let _ = tx.try_send(now());
            }
        }));
    }
}

#[async_trait]
impl TimerGate for RealTimerGate {
    async fn recv(&self) -> Time {
        let mut rx = self.rx.lock().await;
        match rx.recv().await {
            Some(t) => t,
            None => std::future::pending().await,
        }
    }

    fn update(&self, fire_time: Time) -> bool {
        let mut g = self.inner.lock().unwrap();
        if g.active {
            if g.fire_time.before(fire_time) {
                // the timer is active and the next wake up time does not have to be updated
                return false;
            }
            if let Some(h) = g.handle.take() {
                h.abort();
            }
            g.active = false;
        }
        self.drain();
        // the timer was already fired / never active, or has to wake up sooner
        self.arm(&mut g, fire_time);
        true
    }

    fn stop(&self) {
        let mut g = self.inner.lock().unwrap();
        g.fire_time = Time::ZERO;
        g.active = false;
        g.gen += 1;
        if let Some(h) = g.handle.take() {
            h.abort();
        }
        self.drain();
    }
}

struct EventInner {
    current_time: Time,
    fire_time: Time,
}

/// Go `eventTimerGateImpl`: time is supplied by `set_current_time`.
pub struct EventTimerGate {
    inner: StdMutex<EventInner>,
    tx: mpsc::Sender<Time>,
    rx: tokio::sync::Mutex<mpsc::Receiver<Time>>,
}

impl EventTimerGate {
    /// Go `NewEventTimerGate`.
    pub fn new(current_time: Time) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(1);
        Arc::new(EventTimerGate {
            inner: StdMutex::new(EventInner { current_time, fire_time: Time::ZERO }),
            tx,
            rx: tokio::sync::Mutex::new(rx),
        })
    }

    fn fire(&self, t: Time) {
        let _ = self.tx.try_send(t);
    }

    /// Go `FireAfter`.
    pub fn fire_after(&self, now: Time) -> bool {
        let g = self.inner.lock().unwrap();
        g.current_time.before(g.fire_time) && g.fire_time.after(now)
    }

    /// Go `SetCurrentTime`.
    pub fn set_current_time(&self, current_time: Time) -> bool {
        let mut g = self.inner.lock().unwrap();
        if !g.current_time.before(current_time) {
            // new current time is <= current time
            return false;
        }
        if !g.current_time.before(g.fire_time) {
            // current time already >= next wakeup time; avoid duplicate fire
            g.current_time = current_time;
            return true;
        }
        g.current_time = current_time;
        if !g.current_time.before(g.fire_time) {
            let t = g.current_time;
            self.fire(t);
        }
        true
    }
}

#[async_trait]
impl TimerGate for EventTimerGate {
    async fn recv(&self) -> Time {
        let mut rx = self.rx.lock().await;
        match rx.recv().await {
            Some(t) => t,
            None => std::future::pending().await,
        }
    }

    fn update(&self, next_time: Time) -> bool {
        let mut g = self.inner.lock().unwrap();
        let active = g.current_time.before(g.fire_time);
        if active {
            if g.fire_time.before(next_time) {
                // current time < next wake up time < next time
                return false;
            }
            if g.current_time.before(next_time) {
                // current time < next time <= next wake up time
                g.fire_time = next_time;
                return true;
            }
            // next time <= current time < next wake up time
            g.fire_time = next_time;
            let t = g.current_time;
            self.fire(t);
            return true;
        }
        // the timer has already fired / never active
        if !g.current_time.before(next_time) {
            // next time is <= current time, need to fire immediately
            let t = g.current_time;
            self.fire(t);
        } else {
            g.fire_time = next_time;
        }
        true
    }

    fn stop(&self) {
        self.inner.lock().unwrap().fire_time = Time::ZERO;
    }
}
