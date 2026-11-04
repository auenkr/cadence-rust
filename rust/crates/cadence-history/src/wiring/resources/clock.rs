//! Real wall-clock `TimeSource` for every History module (Go `clock.NewRealTimeSource`),
//! plus the tokio-backed `clock.Timer` / `clock.TimerGate` used by queuev2.
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::queuev2::deps as q2;

fn unix_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// One wall clock implementing the `TimeSource` trait of engine, task, replication,
/// failover and queuev2.
#[derive(Clone, Copy, Default)]
pub struct SystemClock;

impl crate::engine::deps::TimeSource for SystemClock {
    fn now(&self) -> i64 {
        unix_nanos()
    }
}
impl crate::task::deps::TimeSource for SystemClock {
    fn now(&self) -> crate::task::deps::Time {
        crate::task::deps::Time(unix_nanos())
    }
}
impl crate::replication::deps::TimeSource for SystemClock {
    fn now_nanos(&self) -> i64 {
        unix_nanos()
    }
}
impl crate::failover::TimeSource for SystemClock {
    fn now_nanos(&self) -> i64 {
        unix_nanos()
    }
}

#[derive(Default)]
struct TimerState {
    deadline: Option<Instant>,
}

/// `clock.Timer` on top of tokio.
pub struct TokioTimer {
    state: Mutex<TimerState>,
    changed: Notify,
}

impl TokioTimer {
    pub fn new(d: Duration) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(TimerState { deadline: Some(Instant::now() + d) }),
            changed: Notify::new(),
        })
    }
}

#[async_trait]
impl q2::Timer for TokioTimer {
    fn stop(&self) -> bool {
        let was = self.state.lock().unwrap().deadline.take().is_some();
        self.changed.notify_one();
        was
    }
    fn reset(&self, d: Duration) -> bool {
        let was = self.state.lock().unwrap().deadline.replace(Instant::now() + d).is_some();
        self.changed.notify_one();
        was
    }
    async fn fired(&self) {
        loop {
            let deadline = self.state.lock().unwrap().deadline;
            match deadline {
                None => self.changed.notified().await,
                Some(dl) => {
                    tokio::select! {
                        _ = tokio::time::sleep_until(dl) => {
                            let mut s = self.state.lock().unwrap();
                            if s.deadline == Some(dl) {
                                s.deadline = None;
                                return;
                            }
                        }
                        _ = self.changed.notified() => {}
                    }
                }
            }
        }
    }
}

/// `clock.TimerGate`: fires at the earliest time passed to `update`.
pub struct TokioTimerGate {
    next: Mutex<Option<q2::Time>>,
    stopped: std::sync::atomic::AtomicBool,
    changed: Notify,
}

impl Default for TokioTimerGate {
    fn default() -> Self {
        Self {
            next: Mutex::new(None),
            stopped: std::sync::atomic::AtomicBool::new(false),
            changed: Notify::new(),
        }
    }
}

#[async_trait]
impl q2::TimerGate for TokioTimerGate {
    fn update(&self, t: q2::Time) -> bool {
        let mut n = self.next.lock().unwrap();
        match *n {
            Some(cur) if cur <= t => false,
            _ => {
                *n = Some(t);
                drop(n);
                self.changed.notify_one();
                true
            }
        }
    }
    fn stop(&self) {
        self.stopped.store(true, std::sync::atomic::Ordering::SeqCst);
        self.changed.notify_one();
    }
    async fn fired(&self) {
        loop {
            if self.stopped.load(std::sync::atomic::Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            let next = *self.next.lock().unwrap();
            match next {
                None => self.changed.notified().await,
                Some(t) => {
                    let wait = (t - chrono::Utc::now()).to_std().unwrap_or(Duration::ZERO);
                    tokio::select! {
                        _ = tokio::time::sleep(wait) => {
                            let mut n = self.next.lock().unwrap();
                            if *n == Some(t) {
                                *n = None;
                                return;
                            }
                        }
                        _ = self.changed.notified() => {}
                    }
                }
            }
        }
    }
}

impl q2::TimeSource for SystemClock {
    fn now(&self) -> q2::Time {
        chrono::Utc::now()
    }
    fn new_timer(&self, d: Duration) -> Arc<dyn q2::Timer> {
        TokioTimer::new(d)
    }
    fn after_func(&self, d: Duration, f: Box<dyn FnOnce() + Send>) -> Arc<dyn q2::Timer> {
        let timer = TokioTimer::new(d);
        let t2 = Arc::clone(&timer);
        tokio::spawn(async move {
            q2::Timer::fired(&*t2).await;
            f();
        });
        timer
    }
}
