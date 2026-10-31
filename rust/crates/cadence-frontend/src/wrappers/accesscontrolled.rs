//! Access control wrapper boundary (Go: wrappers/accesscontrolled).
pub fn authorize(_operation: &str) -> Result<(), cadence_api::errors::AccessDeniedError> {
    Ok(())
}
