//! Domain replication consumer lifecycle boundary.

pub mod domain_replication_processor;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum State {
    Created,
    Running,
    Stopping,
    Stopped,
}
