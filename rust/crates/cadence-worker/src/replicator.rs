//! Domain replication consumer lifecycle boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum State {
    Created,
    Running,
    Stopping,
    Stopped,
}
