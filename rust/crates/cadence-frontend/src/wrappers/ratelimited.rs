//! Rate-limit wrapper boundary (Go: wrappers/ratelimited).
pub fn is_exempt(_caller: &str) -> bool {
    false
}
