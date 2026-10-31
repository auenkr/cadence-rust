//! Async-workflow consumer lifecycle boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum State {
    Disabled,
    Running,
    Stopping,
    Stopped,
}
