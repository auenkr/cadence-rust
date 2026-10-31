//! Metrics wrapper boundary (Go: wrappers/metered).
pub fn metric_name(operation: &str) -> String {
    format!("frontend.{operation}")
}
