//! Go `common/rpc.authOutboundMiddleware` + cadence-client
//! `NewAdminJwtAuthorizationProvider`: an RS256 admin JWT attached as the
//! `cadence-authorization` header of cross-cluster calls.

use std::time::{SystemTime, UNIX_EPOCH};

use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::Serialize;

/// Go `common.AuthorizationTokenHeaderName`.
pub const AUTHORIZATION_HEADER: &str = "cadence-authorization";
/// cadence-client `internalIssuer`.
const INTERNAL_ISSUER: &str = "internal-jwt";
/// cadence-client `JWTAuthProvider.GetAuthToken`: `ttl := 60 * 10`.
const TTL_SECONDS: i64 = 600;

/// cadence-client `auth.JWTClaims` (Go field names are the JSON keys).
#[derive(Serialize)]
struct Claims {
    iss: &'static str,
    iat: i64,
    exp: i64,
    #[serde(rename = "Sub")]
    sub: &'static str,
    #[serde(rename = "Name")]
    name: &'static str,
    #[serde(rename = "Groups")]
    groups: &'static str,
    #[serde(rename = "Admin")]
    admin: bool,
    #[serde(rename = "TTL")]
    ttl: i64,
}

#[derive(Clone, Debug)]
enum Key {
    Pem(Vec<u8>),
    Invalid(String),
}

/// Signs a fresh admin token per call, like Go's `GetAuthToken`.
#[derive(Clone, Debug)]
pub struct AdminJwtProvider {
    key: Key,
}

impl AdminJwtProvider {
    /// Go `authorization.GetAuthProviderClient(privateKeyPath)`.
    pub fn from_private_key_file(path: &str) -> Result<Self, String> {
        let pem = std::fs::read(path).map_err(|_| format!("invalid private key path {path}"))?;
        Ok(Self::from_pem(pem))
    }

    pub fn from_pem(pem: Vec<u8>) -> Self {
        Self { key: Key::Pem(pem) }
    }

    /// Construction failed (Go aborts startup); every call reports the error.
    pub fn invalid(error: String) -> Self {
        Self {
            key: Key::Invalid(error),
        }
    }

    pub fn token(&self) -> Result<String, String> {
        let pem = match &self.key {
            Key::Pem(pem) => pem,
            Key::Invalid(error) => return Err(error.clone()),
        };
        let key = EncodingKey::from_rsa_pem(pem).map_err(|e| e.to_string())?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs() as i64);
        let claims = Claims {
            iss: INTERNAL_ISSUER,
            iat: now,
            exp: now + TTL_SECONDS,
            sub: "",
            name: "",
            groups: "",
            admin: true,
            ttl: TTL_SECONDS,
        };
        encode(&Header::new(Algorithm::RS256), &claims, &key).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{decode, DecodingKey, Validation};

    const PRIVATE: &str = include_str!("../../cadence-frontend/testdata/test_private.pem");

    #[test]
    fn token_is_rs256_admin_jwt_with_go_claims() {
        let token = AdminJwtProvider::from_pem(PRIVATE.as_bytes().to_vec())
            .token()
            .unwrap();
        let header = jsonwebtoken::decode_header(&token).unwrap();
        assert_eq!(header.alg, Algorithm::RS256);
        let mut validation = Validation::new(Algorithm::RS256);
        validation.insecure_disable_signature_validation();
        validation.set_required_spec_claims(&["exp", "iat"]);
        let claims = decode::<serde_json::Value>(&token, &DecodingKey::from_secret(b""), &validation)
            .unwrap()
            .claims;
        assert_eq!(claims["iss"], "internal-jwt");
        assert_eq!(claims["Admin"], true);
        assert_eq!(claims["TTL"], 600);
        assert_eq!(claims["exp"].as_i64().unwrap() - claims["iat"].as_i64().unwrap(), 600);
    }

    #[test]
    fn missing_key_file_and_bad_pem_are_errors() {
        let error = AdminJwtProvider::from_private_key_file("/nonexistent/key.pem").unwrap_err();
        assert_eq!(error, "invalid private key path /nonexistent/key.pem");
        assert!(AdminJwtProvider::from_pem(b"garbage".to_vec()).token().is_err());
        assert_eq!(
            AdminJwtProvider::invalid("boom".into()).token().unwrap_err(),
            "boom"
        );
    }
}
