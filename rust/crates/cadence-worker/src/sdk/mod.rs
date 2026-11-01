//! Cadence client SDK runtime for system workflows (port of the parts of
//! `go.uber.org/cadence/internal` that the Worker service uses).
//!
//! Design (see memory/worker-core-20261101.md):
//! * `converter`: Go-compatible JSON data converter (values followed by `\n`).
//! * `core`: per-run state, decision list, `WorkflowContext` and the futures
//!   workflow code awaits. Workflows are `async` closures driven by a
//!   deterministic single-threaded executor (one `poll` per history batch with
//!   a no-op waker; futures re-check the state on every poll).
//! * `replay`: splits a history into decision-task batches like Go's history
//!   iterator, applies reply events, runs the workflow, checks generated
//!   decisions against the recorded decision events (non-determinism errors),
//!   and returns the decisions for the final batch plus query answers.
//! * `decision_poller` / `activity`: pollers on the Frontend gRPC WorkerAPI.
//!
//! Not yet supported: child workflows, signal/cancel of external workflows,
//! search attribute upserts, local activities, sessions, sticky execution.

pub mod activity;
pub mod converter;
pub mod core;
pub mod decision_poller;
pub mod registry;
pub mod replay;

#[cfg(test)]
mod tests;
