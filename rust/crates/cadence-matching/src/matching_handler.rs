//! Go `service/matching/handler` Start/Stop seam.
//!
//! Go's `handlerImpl.Start` starts the engine once before any RPC is served,
//! and `handlerImpl.Stop` stops it. Rust serves RPCs through tonic, so this
//! type holds the once-only engine calls behind one object that the launcher
//! can drive.

use std::sync::atomic::{AtomicBool, Ordering};

/// The engine operations Go's handler forwards to `Engine.Start` and `Engine.Stop`.
pub trait EngineLifecycle: Send + Sync + 'static {
    fn start(&self);
    fn stop(&self);
}

pub struct Handler<E: EngineLifecycle> {
    engine: E,
    started: AtomicBool,
    stopped: AtomicBool,
}

impl<E: EngineLifecycle> Handler<E> {
    pub fn new(engine: E) -> Self {
        Self {
            engine,
            started: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
        }
    }

    /// Go `handlerImpl.Start`: calls the engine's start once. Returns `false`
    /// without calling it again when the handler has already started.
    pub fn start(&self) -> bool {
        if self.started.swap(true, Ordering::SeqCst) {
            return false;
        }
        self.engine.start();
        true
    }

    /// Go `handlerImpl.Stop`: calls the engine's stop once. Returns `false`
    /// without calling it again when the handler has already stopped.
    pub fn stop(&self) -> bool {
        if self.stopped.swap(true, Ordering::SeqCst) {
            return false;
        }
        self.engine.stop();
        true
    }
}
