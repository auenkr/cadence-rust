//! Domain deprecation workflow parameters from Go `domaindeprecation/entities.go`.

use serde::{Deserialize, Serialize};

/// Go `DomainDeprecationParams`. Its JSON keys are the snake_case tags below,
/// and missing keys decode to zero values as they do in Go.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct DomainDeprecationParams {
    pub domain_name: String,
    pub security_token: String,
    pub force: bool,
}

#[cfg(test)]
mod tests {
    use super::DomainDeprecationParams;

    #[test]
    fn encodes_go_snake_case_keys_in_declaration_order() {
        let params = DomainDeprecationParams {
            domain_name: "orders".into(),
            security_token: "token".into(),
            force: true,
        };
        assert_eq!(
            serde_json::to_string(&params).unwrap(),
            r#"{"domain_name":"orders","security_token":"token","force":true}"#
        );
    }

    #[test]
    fn missing_keys_decode_to_zero_values() {
        let params: DomainDeprecationParams =
            serde_json::from_str(r#"{"domain_name":"x"}"#).unwrap();
        assert_eq!(params.domain_name, "x");
        assert_eq!(params.security_token, "");
        assert!(!params.force);
    }
}
