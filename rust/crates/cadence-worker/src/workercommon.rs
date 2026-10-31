//! Worker-specific utility contracts shared by system workers.

/// Cadence's internal system domain. Go uses this domain for most system workflows.
pub const SYSTEM_DOMAIN: &str = "cadence-system";

/// Go stores JSON encoded system-workflow payloads in Cadence `Payload.data`.
/// This helper keeps serialization centralized so each port can mirror the SDK
/// converter and add byte-for-byte Go fixtures as it is implemented.
pub fn encode_json<T: serde::Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    Ok(serde_json::to_vec(value)?)
}

pub fn decode_json<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> anyhow::Result<T> {
    Ok(serde_json::from_slice(bytes)?)
}

#[cfg(test)]
mod tests {
    #[test]
    fn system_workflow_payloads_use_compact_json() {
        let bytes = super::encode_json(&serde_json::json!({"DomainID":"domain-id"})).unwrap();
        assert_eq!(bytes, br#"{"DomainID":"domain-id"}"#);
        let value: serde_json::Value = super::decode_json(&bytes).unwrap();
        assert_eq!(value["DomainID"], "domain-id");
    }
}
