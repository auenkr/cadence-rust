//! Port of `go/service/history/task/event_logger.go`.

use std::fmt::Debug;
use std::sync::Arc;

use super::deps::{tag, Logger, Time, TimeSource};

/// Go `eventLogger` interface.
pub trait EventLogger: Send + Sync {
    fn add_event(&self, event_msg: &str, details: Vec<String>);
    fn flush_events(&self, msg: &str) -> usize;
}

#[derive(Clone, Default)]
struct Event {
    timestamp: Time,
    message: String,
    details: Vec<String>,
}

struct EventLoggerState {
    events: Vec<Event>,
    next_event_idx: usize,
}

/// Go `eventLoggerImpl`.
pub struct EventLoggerImpl {
    logger: Logger,
    time_source: Arc<dyn TimeSource>,
    state: std::sync::Mutex<EventLoggerState>,
    max_size: usize,
}

pub fn new_event_logger(
    logger: Logger,
    time_source: Arc<dyn TimeSource>,
    max_size: usize,
) -> Arc<dyn EventLogger> {
    Arc::new(EventLoggerImpl {
        logger,
        time_source,
        state: std::sync::Mutex::new(EventLoggerState {
            events: vec![Event::default(); max_size],
            next_event_idx: 0,
        }),
        max_size,
    })
}

impl EventLogger for EventLoggerImpl {
    fn add_event(&self, event_msg: &str, details: Vec<String>) {
        let mut s = self.state.lock().unwrap();
        let idx = s.next_event_idx;
        s.events[idx] = Event {
            timestamp: self.time_source.now(),
            message: event_msg.to_string(),
            details,
        };
        s.next_event_idx = (idx + 1) % self.max_size;
    }

    fn flush_events(&self, msg: &str) -> usize {
        let mut s = self.state.lock().unwrap();
        let mut builder = String::new();
        let mut event_idx = s.next_event_idx as isize;
        let mut events_flushed = 0;

        for _ in 0..s.events.len() {
            event_idx -= 1;
            if event_idx < 0 {
                event_idx = s.events.len() as isize - 1;
            }
            let event = &s.events[event_idx as usize];
            if event.timestamp.is_zero() {
                break;
            }
            events_flushed += 1;
            builder.push_str(&format!(
                "{:?}, {}, {:?}\n",
                event.timestamp, event.message, event.details
            ));
        }

        self.logger
            .info(msg, &[tag::key("events"), tag::value(&builder)]);

        s.events = vec![Event::default(); self.max_size];
        events_flushed
    }
}

/// Helper for `fmt.Sprint`-style details.
pub fn detail<T: Debug>(v: T) -> String {
    format!("{v:?}")
}
