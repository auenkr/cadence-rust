//! Async-workflow consumer lifecycle boundary.

pub mod consumer_manager;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum State {
    Disabled,
    Running,
    Stopping,
    Stopped,
}
