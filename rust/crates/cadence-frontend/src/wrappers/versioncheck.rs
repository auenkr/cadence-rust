//! Client version check wrapper boundary (Go: wrappers/versioncheck).
pub fn check(_client_impl: &str, _feature_version: &str) -> Result<(), String> {
    Ok(())
}
