//! Go `common/authorization` oauthAuthorizer: RS256 JWT validation with a
//! public key file and domain-group permission checks.
//!
//! Not ported: the external `provider` (JWKS + jmespath attribute paths).
//! `new` rejects it explicitly so a Go config using it fails loudly.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde_json::Value;

use crate::wrappers::accesscontrolled::{Attributes, Authorizer, Decision, Permission};
use cadence_service_core::config::{AuthorizationConfig, OAuthAuthorizerConfig};

const GROUP_SEPARATOR: char = ' ';
const JWT_INTERNAL_ISSUER: &str = "internal-jwt";
const READ_GROUPS: &str = "READ_GROUPS";
const WRITE_GROUPS: &str = "WRITE_GROUPS";
const PROCESS_GROUPS: &str = "PROCESS_GROUPS";

/// Domain data lookup (Go: `domainCache.GetDomain(name).GetInfo().Data`).
pub type DomainDataLookup =
    Arc<dyn Fn(&str) -> Result<HashMap<String, String>, String> + Send + Sync>;

pub struct OAuthAuthorizer {
    max_jwt_ttl: i64,
    public_key: DecodingKey,
    domain_data: DomainDataLookup,
}

/// Go `authorization.NewAuthorizer`: oauth when enabled, otherwise nop.
/// Returns `None` for nop.
pub fn new_authorizer(
    config: &AuthorizationConfig,
    domain_data: DomainDataLookup,
) -> Result<Option<OAuthAuthorizer>, String> {
    let oauth = &config.oauth_authorizer;
    if oauth.enable && config.noop_authorizer.enable {
        return Err("[AuthorizationConfig] More than one authorizer is enabled".into());
    }
    if !oauth.enable {
        return Ok(None);
    }
    OAuthAuthorizer::new(oauth, domain_data).map(Some)
}

impl OAuthAuthorizer {
    pub fn new(config: &OAuthAuthorizerConfig, domain_data: DomainDataLookup) -> Result<Self, String> {
        if config.max_jwt_ttl <= 0 {
            return Err("[OAuthConfig] MaxTTL must be greater than 0".into());
        }
        if config.jwt_credentials.is_none() && config.provider.is_none() {
            return Err("jwtCredentials or provider must be provided".into());
        }
        if let Some(provider) = &config.provider {
            if provider.jwks_url.is_empty() {
                return Err("[OAuthConfig] JWKSURL is not set".into());
            }
            return Err("oauthAuthorizer.provider (JWKS) is not supported by the Rust Frontend".into());
        }
        let creds = config.jwt_credentials.as_ref().expect("checked above");
        if creds.public_key.is_empty() {
            return Err("[OAuthConfig] PublicKey can't be empty".into());
        }
        if creds.algorithm != "RS256" {
            return Err("[OAuthConfig] The only supported Algorithm is RS256".into());
        }
        let pem = std::fs::read(&creds.public_key)
            .map_err(|_| format!("loading RSA public key: invalid public key path {}", creds.public_key))?;
        if !String::from_utf8_lossy(&pem).contains("-----BEGIN PUBLIC KEY-----") {
            return Err("loading RSA public key: failed to parse PEM block containing the public key".into());
        }
        let public_key = DecodingKey::from_rsa_pem(&pem)
            .map_err(|e| format!("loading RSA public key: failed to parse DER encoded public key: {e}"))?;
        Ok(Self {
            max_jwt_ttl: config.max_jwt_ttl,
            public_key,
            domain_data,
        })
    }

    /// Returns the verified claims, or a reason for denial.
    fn verify(&self, token: &str, now: i64) -> Result<Value, String> {
        let header = decode_header(token).map_err(|e| e.to_string())?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.required_spec_claims = HashSet::new();
        validation.leeway = 0;
        validation.validate_exp = true;
        validation.validate_nbf = true;
        let claims = decode::<Value>(token, &self.public_key, &validation)
            .map_err(|e| e.to_string())?
            .claims;
        // Go: external (kid + non-internal issuer) tokens need a JWKS
        // provider, which is not configured here -> no key for verification.
        let internal = header.kid.is_none()
            || claims.get("iss").and_then(Value::as_str) == Some(JWT_INTERNAL_ISSUER);
        if !internal {
            return Err("no public key for verification".into());
        }
        // jwt.WithIssuedAt: iat in the future is invalid.
        if let Some(iat) = claims.get("iat").and_then(Value::as_i64) {
            if iat > now {
                return Err("token used before issued".into());
            }
        }
        Ok(claims)
    }

    fn validate_ttl(&self, claims: &Value, now: i64) -> Result<(), String> {
        let ttl = claims.get("ttl").or_else(|| claims.get("TTL")).and_then(Value::as_i64).unwrap_or(0);
        let mut exp = claims.get("exp").and_then(Value::as_i64);
        if ttl > 0 {
            let iat = claims
                .get("iat")
                .and_then(Value::as_i64)
                .ok_or("ExpiresAt is not set")?;
            exp = Some(iat + ttl);
        }
        let exp = exp.ok_or("ExpiresAt is not set")?;
        let time_left = exp - now;
        if time_left < 0 {
            return Err("token is expired".into());
        }
        if time_left > self.max_jwt_ttl {
            return Err(format!(
                "token TTL: {time_left} is larger than MaxTTL allowed: {}",
                self.max_jwt_ttl
            ));
        }
        Ok(())
    }

    fn decide(&self, attributes: &Attributes, now: i64) -> Result<Decision, String> {
        if attributes.auth_token.is_empty() {
            return Ok(Decision::Deny);
        }
        let claims = match self.verify(&attributes.auth_token, now) {
            Ok(c) => c,
            Err(_) => return Ok(Decision::Deny),
        };
        if self.validate_ttl(&claims, now).is_err() {
            return Ok(Decision::Deny);
        }
        let admin = claims
            .get("admin")
            .or_else(|| claims.get("Admin"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if admin {
            return Ok(Decision::Allow);
        }
        let data = (self.domain_data)(&attributes.domain)?;
        let groups = claims
            .get("groups")
            .or_else(|| claims.get("Groups"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        Ok(if validate_permission(groups, attributes.permission, &data) {
            Decision::Allow
        } else {
            Decision::Deny
        })
    }
}

fn split_groups(value: Option<&String>) -> Vec<&str> {
    value.map_or_else(Vec::new, |v| v.split(GROUP_SEPARATOR).collect())
}

fn validate_permission(jwt_groups: &str, permission: Option<Permission>, data: &HashMap<String, String>) -> bool {
    let Some(permission) = permission else {
        return false;
    };
    let mut allowed: HashSet<&str> = split_groups(data.get(WRITE_GROUPS)).into_iter().collect();
    if permission == Permission::Read {
        allowed.extend(split_groups(data.get(READ_GROUPS)));
    }
    if permission == Permission::Process {
        allowed.extend(split_groups(data.get(PROCESS_GROUPS)));
    }
    // Go splits an empty claim into [""], which matches an empty allowed
    // entry (e.g. a missing group key); keep that quirk.
    let jwt: Vec<&str> = jwt_groups.split(GROUP_SEPARATOR).collect();
    if allowed.is_empty() {
        return false;
    }
    jwt.iter().any(|g| allowed.contains(g))
}

impl Authorizer for OAuthAuthorizer {
    type Error = String;

    fn authorize(&self, attributes: &Attributes) -> Result<Decision, Self::Error> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or_default();
        self.decide(attributes, now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_service_core::config::JwtCredentialsConfig;
    use jsonwebtoken::{encode, EncodingKey, Header};

    const PRIV: &str = include_str!("../testdata/test_private.pem");

    fn pub_path() -> String {
        format!("{}/testdata/test_public.pem", env!("CARGO_MANIFEST_DIR"))
    }

    fn authorizer(data: HashMap<String, String>) -> OAuthAuthorizer {
        let cfg = OAuthAuthorizerConfig {
            enable: true,
            max_jwt_ttl: 3600,
            jwt_credentials: Some(JwtCredentialsConfig {
                algorithm: "RS256".into(),
                public_key: pub_path(),
            }),
            provider: None,
        };
        OAuthAuthorizer::new(&cfg, Arc::new(move |_| Ok(data.clone()))).unwrap()
    }

    fn now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    fn token(claims: Value) -> String {
        encode(
            &Header::new(Algorithm::RS256),
            &claims,
            &EncodingKey::from_rsa_pem(PRIV.as_bytes()).unwrap(),
        )
        .unwrap()
    }

    fn attrs(token: String, permission: Permission) -> Attributes {
        Attributes {
            domain: "d".into(),
            permission: Some(permission),
            auth_token: token,
            ..Default::default()
        }
    }

    fn groups(r: &str, w: &str) -> HashMap<String, String> {
        HashMap::from([(READ_GROUPS.into(), r.into()), (WRITE_GROUPS.into(), w.into())])
    }

    #[test]
    fn missing_or_garbage_token_denied() {
        let a = authorizer(groups("a", "b"));
        assert_eq!(a.authorize(&attrs(String::new(), Permission::Read)).unwrap(), Decision::Deny);
        assert_eq!(a.authorize(&attrs("x.y.z".into(), Permission::Read)).unwrap(), Decision::Deny);
    }

    #[test]
    fn admin_allowed_and_group_permissions() {
        let a = authorizer(groups("readers", "writers"));
        let n = now();
        let admin = token(serde_json::json!({"admin": true, "iat": n, "ttl": 60}));
        assert_eq!(a.authorize(&attrs(admin, Permission::Admin)).unwrap(), Decision::Allow);
        let reader = token(serde_json::json!({"groups": "x readers", "iat": n, "ttl": 60}));
        assert_eq!(a.authorize(&attrs(reader.clone(), Permission::Read)).unwrap(), Decision::Allow);
        assert_eq!(a.authorize(&attrs(reader, Permission::Write)).unwrap(), Decision::Deny);
        let writer = token(serde_json::json!({"groups": "writers", "iat": n, "exp": n + 60}));
        assert_eq!(a.authorize(&attrs(writer.clone(), Permission::Read)).unwrap(), Decision::Allow);
        assert_eq!(a.authorize(&attrs(writer, Permission::Write)).unwrap(), Decision::Allow);
    }

    #[test]
    fn expired_too_long_and_wrong_signature_denied() {
        let a = authorizer(groups("g", "g"));
        let n = now();
        let expired = token(serde_json::json!({"admin": true, "iat": n - 100, "exp": n - 10}));
        assert_eq!(a.authorize(&attrs(expired, Permission::Read)).unwrap(), Decision::Deny);
        let long = token(serde_json::json!({"admin": true, "iat": n, "ttl": 7200}));
        assert_eq!(a.authorize(&attrs(long, Permission::Read)).unwrap(), Decision::Deny);
        let no_exp = token(serde_json::json!({"admin": true, "iat": n}));
        assert_eq!(a.authorize(&attrs(no_exp, Permission::Read)).unwrap(), Decision::Deny);
        let mut ok = token(serde_json::json!({"admin": true, "iat": n, "ttl": 60}));
        ok.push('A');
        assert_eq!(a.authorize(&attrs(ok, Permission::Read)).unwrap(), Decision::Deny);
    }

    #[test]
    fn domain_lookup_error_is_returned() {
        let cfg = OAuthAuthorizerConfig {
            enable: true,
            max_jwt_ttl: 3600,
            jwt_credentials: Some(JwtCredentialsConfig { algorithm: "RS256".into(), public_key: pub_path() }),
            provider: None,
        };
        let a = OAuthAuthorizer::new(&cfg, Arc::new(|_| Err("nope".into()))).unwrap();
        let t = token(serde_json::json!({"groups": "g", "iat": now(), "ttl": 60}));
        assert_eq!(a.authorize(&attrs(t, Permission::Read)).unwrap_err(), "nope");
    }

    #[test]
    fn config_validation_matches_go() {
        let lookup: DomainDataLookup = Arc::new(|_| Ok(HashMap::new()));
        let mut cfg = OAuthAuthorizerConfig { enable: true, ..Default::default() };
        assert!(OAuthAuthorizer::new(&cfg, lookup.clone()).unwrap_err().contains("MaxTTL"));
        cfg.max_jwt_ttl = 10;
        assert!(OAuthAuthorizer::new(&cfg, lookup.clone()).unwrap_err().contains("jwtCredentials or provider"));
        cfg.jwt_credentials = Some(JwtCredentialsConfig { algorithm: "HS256".into(), public_key: pub_path() });
        assert!(OAuthAuthorizer::new(&cfg, lookup.clone()).unwrap_err().contains("RS256"));
        let both = AuthorizationConfig {
            oauth_authorizer: OAuthAuthorizerConfig { enable: true, ..Default::default() },
            noop_authorizer: cadence_service_core::config::NoopAuthorizerConfig { enable: true },
        };
        assert!(new_authorizer(&both, lookup.clone()).is_err());
        assert!(new_authorizer(&AuthorizationConfig::default(), lookup).unwrap().is_none());
    }
}
