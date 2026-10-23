//! Blocking boundary for Matching persistence and other synchronous store work.
//!
//! The Cassandra driver is asynchronous, but Matching's durable queue contract
//! is synchronous. Cassandra adapters use a runtime handle from these worker
//! threads; keeping calls behind this helper prevents driver waits and queue
//! locking from occupying Tokio RPC/timer workers.

pub(crate) async fn run<T, F>(work: F) -> Result<T, tokio::task::JoinError>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    tokio::task::spawn_blocking(work).await
}

#[cfg(test)]
mod tests {
    use super::run;
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };

    /// A blocking persistence operation must not stall Tokio timers, and its
    /// completion must happen before the caller proceeds with shutdown.
    #[tokio::test(flavor = "current_thread")]
    async fn blocking_store_wait_keeps_timer_live_and_orders_shutdown() {
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let completed = Arc::new(AtomicBool::new(false));
        let completed_store = completed.clone();
        let operation = tokio::spawn(async move {
            run(move || {
                release_rx.recv().expect("release persistence call");
                completed_store.store(true, Ordering::SeqCst);
            })
            .await
            .expect("blocking worker");
        });

        tokio::time::timeout(
            Duration::from_millis(200),
            tokio::time::sleep(Duration::from_millis(5)),
        )
        .await
        .expect("Tokio timer stalled behind persistence");
        assert!(!completed.load(Ordering::SeqCst));
        release_tx.send(()).unwrap();
        operation.await.unwrap();
        assert!(
            completed.load(Ordering::SeqCst),
            "shutdown must follow store completion"
        );
    }
}
