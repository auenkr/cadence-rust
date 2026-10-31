//! Frontend lifecycle state mirrors Go daemon states and health transitions.

use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum State {
    Initialized = 0,
    Started = 1,
    ShuttingDown = 2,
    Stopped = 3,
}

#[derive(Debug, Default)]
pub struct Lifecycle(AtomicU8);

impl Lifecycle {
    pub fn start(&self) -> bool {
        self.0
            .compare_exchange(0, State::Started as u8, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub fn begin_shutdown(&self) -> bool {
        self.0
            .compare_exchange(1, State::ShuttingDown as u8, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub fn stop(&self) -> bool {
        self.0
            .compare_exchange(2, State::Stopped as u8, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub fn state(&self) -> State {
        match self.0.load(Ordering::Acquire) {
            1 => State::Started,
            2 => State::ShuttingDown,
            3 => State::Stopped,
            _ => State::Initialized,
        }
    }
}
