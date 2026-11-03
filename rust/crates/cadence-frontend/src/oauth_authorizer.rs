//! Go `common/authorization` oauthAuthorizer: RS256 JWT validation with a
//! public key file and domain-group permission checks.
//!
//! The external `provider` mode fetches the JWKS once at startup (Go
//! keyfunc default options do not refresh) and applies the jmespath
//! groups/admin attribute paths to external tokens.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde_json::Value;

use crate::wrappers::accesscontrolled::{Attributes, Authorizer, Decision, Permission};
use cadence_service_core::config::{
    AuthorizationConfig, OAuthAuthorizerConfig, OAuthProviderConfig,
};

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
    public_key: Option<DecodingKey>,
    /// kid -> verification key (Go keyfunc JWKS, fetched once at startup).
    jwks: Option<HashMap<String, (DecodingKey, Algorithm)>>,
    provider: Option<OAuthProviderConfig>,
    domain_data: DomainDataLookup,
}

/// Claims of a JWT without verifying the signature (Go `ParseUnverified`).
fn unverified_claims(token: &str) -> Result<Value, String> {
    use base64::Engine;
    let payload = token.split('.').nth(1).ok_or("token is malformed")?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .map_err(|e| e.to_string())?;
    serde_json::from_slice(&bytes).map_err(|e| e.to_string())
}

fn jmespath_search(
    expression: &str,
    data: &Value,
) -> Result<std::rc::Rc<jmespath::Variable>, String> {
    jmespath::compile(expression)
        .map_err(|e| e.to_string())?
        .search(data)
        .map_err(|e| e.to_string())
}

/// Fetch and parse a JWKS document (RFC 7517) over HTTP(S). Runs on a
/// dedicated thread because the blocking client cannot run inside tokio.
fn fetch_jwks(url: &str) -> Result<HashMap<String, (DecodingKey, Algorithm)>, String> {
    let url = url.to_owned();
    let body: Value = std::thread::spawn(move || {
        reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .and_then(|c| c.get(&url).send())
            .and_then(|r| r.error_for_status())
            .and_then(|r| r.json::<Value>())
            .map_err(|e| e.to_string())
    })
    .join()
    .map_err(|_| "JWKS fetch panicked".to_owned())??;
    parse_jwks(&body)
}

fn parse_jwks(body: &Value) -> Result<HashMap<String, (DecodingKey, Algorithm)>, String> {
    let keys = body
        .get("keys")
        .and_then(Value::as_array)
        .ok_or("JWKS has no keys")?;
    let mut out = HashMap::new();
    for key in keys {
        let text = |f: &str| key.get(f).and_then(Value::as_str);
        if text("kty") != Some("RSA") {
            continue;
        }
        let (Some(kid), Some(n), Some(e)) = (text("kid"), text("n"), text("e")) else {
            continue;
        };
        let alg = match text("alg") {
            Some("RS384") => Algorithm::RS384,
            Some("RS512") => Algorithm::RS512,
            Some("PS256") => Algorithm::PS256,
            Some("PS384") => Algorithm::PS384,
            Some("PS512") => Algorithm::PS512,
            _ => Algorithm::RS256,
        };
        let decoding = DecodingKey::from_rsa_components(n, e).map_err(|e| e.to_string())?;
        out.insert(kid.to_owned(), (decoding, alg));
    }
    Ok(out)
}

impl std::fmt::Debug for OAuthAuthorizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthAuthorizer")
            .field("max_jwt_ttl", &self.max_jwt_ttl)
            .finish_non_exhaustive()
    }
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
    pub fn new(
        config: &OAuthAuthorizerConfig,
        domain_data: DomainDataLookup,
    ) -> Result<Self, String> {
        if config.max_jwt_ttl <= 0 {
            return Err("[OAuthConfig] MaxTTL must be greater than 0".into());
        }
        if config.jwt_credentials.is_none() && config.provider.is_none() {
            return Err("jwtCredentials or provider must be provided".into());
        }
        let mut jwks = None;
        if let Some(provider) = &config.provider {
            if provider.jwks_url.is_empty() {
                return Err("[OAuthConfig] JWKSURL is not set".into());
            }
            jwks = Some(fetch_jwks(&provider.jwks_url).map_err(|e| {
                format!(
                    "creating JWKS from resource: {} error: {e}",
                    provider.jwks_url
                )
            })?);
        }
        let mut public_key = None;
        if let Some(creds) = &config.jwt_credentials {
            if creds.public_key.is_empty() {
                return Err("[OAuthConfig] PublicKey can't be empty".into());
            }
            if creds.algorithm != "RS256" {
                return Err("[OAuthConfig] The only supported Algorithm is RS256".into());
            }
            let pem = std::fs::read(&creds.public_key).map_err(|_| {
                format!(
                    "loading RSA public key: invalid public key path {}",
                    creds.public_key
                )
            })?;
            if !String::from_utf8_lossy(&pem).contains("-----BEGIN PUBLIC KEY-----") {
                return Err(
                    "loading RSA public key: failed to parse PEM block containing the public key"
                        .into(),
                );
            }
            public_key = Some(DecodingKey::from_rsa_pem(&pem).map_err(|e| {
                format!("loading RSA public key: failed to parse DER encoded public key: {e}")
            })?);
        }
        Ok(Self {
            max_jwt_ttl: config.max_jwt_ttl,
            public_key,
            jwks,
            provider: config.provider.clone(),
            domain_data,
        })
    }

    /// Returns the verified claims (plus whether the token is internal), or
    /// a reason for denial.
    fn verify(&self, token: &str, now: i64) -> Result<(Value, bool), String> {
        let header = decode_header(token).map_err(|e| e.to_string())?;
        let unverified = unverified_claims(token)?;
        // Go isTokenInternal: no kid header, or the internal issuer.
        let internal = header.kid.is_none()
            || unverified.get("iss").and_then(Value::as_str) == Some(JWT_INTERNAL_ISSUER);
        let (key, algorithm) = match (&self.public_key, &self.jwks) {
            (Some(key), _) if internal => (key, Algorithm::RS256),
            (_, Some(jwks)) => {
                let kid = header.kid.as_deref().ok_or("token has no kid header")?;
                let (key, alg) = jwks.get(kid).ok_or("no JWKS key for kid")?;
                (key, *alg)
            }
            _ => return Err("no public key for verification".into()),
        };
        let mut validation = Validation::new(algorithm);
        validation.required_spec_claims = HashSet::new();
        validation.leeway = 0;
        validation.validate_exp = true;
        validation.validate_nbf = true;
        let claims = decode::<Value>(token, key, &validation)
            .map_err(|e| e.to_string())?
            .claims;
        // jwt.WithIssuedAt: iat in the future is invalid.
        if let Some(iat) = claims.get("iat").and_then(Value::as_i64) {
            if iat > now {
                return Err("token used before issued".into());
            }
        }
        Ok((claims, internal))
    }

    /// Go `parseExternal`: override groups/admin via jmespath attribute paths.
    fn parse_external(&self, raw: &Value, claims: &mut Value) -> Result<(), String> {
        let Some(provider) = &self.provider else {
            return Ok(());
        };
        let Some(obj) = claims.as_object_mut() else {
            return Ok(());
        };
        if !provider.groups_attribute_path.is_empty() {
            let found = jmespath_search(&provider.groups_attribute_path, raw)
                .map_err(|e| format!("extracting JWT Groups claim: {e}"))?;
            let groups = found.as_string().ok_or("cannot convert groups to string")?;
            obj.insert("groups".into(), Value::String(groups.clone()));
            obj.remove("Groups");
        }
        if !provider.admin_attribute_path.is_empty() {
            let found = jmespath_search(&provider.admin_attribute_path, raw)
                .map_err(|e| format!("extracting JWT Admin claim: {e}"))?;
            let admin = found.as_boolean().ok_or("cannot convert isAdmin to bool")?;
            obj.insert("admin".into(), Value::Bool(admin));
            obj.remove("Admin");
        }
        Ok(())
    }

    fn validate_ttl(&self, claims: &Value, now: i64) -> Result<(), String> {
        let ttl = claims
            .get("ttl")
            .or_else(|| claims.get("TTL"))
            .and_then(Value::as_i64)
            .unwrap_or(0);
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
            Ok((c, true)) => c,
            Ok((mut c, false)) => {
                let raw = c.clone();
                if self.parse_external(&raw, &mut c).is_err() {
                    return Ok(Decision::Deny);
                }
                c
            }
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
        Ok(
            if validate_permission(groups, attributes.permission, &data) {
                Decision::Allow
            } else {
                Decision::Deny
            },
        )
    }
}

fn split_groups(value: Option<&String>) -> Vec<&str> {
    value.map_or_else(Vec::new, |v| v.split(GROUP_SEPARATOR).collect())
}

fn validate_permission(
    jwt_groups: &str,
    permission: Option<Permission>,
    data: &HashMap<String, String>,
) -> bool {
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
        HashMap::from([
            (READ_GROUPS.into(), r.into()),
            (WRITE_GROUPS.into(), w.into()),
        ])
    }

    #[test]
    fn missing_or_garbage_token_denied() {
        let a = authorizer(groups("a", "b"));
        assert_eq!(
            a.authorize(&attrs(String::new(), Permission::Read))
                .unwrap(),
            Decision::Deny
        );
        assert_eq!(
            a.authorize(&attrs("x.y.z".into(), Permission::Read))
                .unwrap(),
            Decision::Deny
        );
    }

    #[test]
    fn admin_allowed_and_group_permissions() {
        let a = authorizer(groups("readers", "writers"));
        let n = now();
        let admin = token(serde_json::json!({"admin": true, "iat": n, "ttl": 60}));
        assert_eq!(
            a.authorize(&attrs(admin, Permission::Admin)).unwrap(),
            Decision::Allow
        );
        let reader = token(serde_json::json!({"groups": "x readers", "iat": n, "ttl": 60}));
        assert_eq!(
            a.authorize(&attrs(reader.clone(), Permission::Read))
                .unwrap(),
            Decision::Allow
        );
        assert_eq!(
            a.authorize(&attrs(reader, Permission::Write)).unwrap(),
            Decision::Deny
        );
        let writer = token(serde_json::json!({"groups": "writers", "iat": n, "exp": n + 60}));
        assert_eq!(
            a.authorize(&attrs(writer.clone(), Permission::Read))
                .unwrap(),
            Decision::Allow
        );
        assert_eq!(
            a.authorize(&attrs(writer, Permission::Write)).unwrap(),
            Decision::Allow
        );
    }

    #[test]
    fn expired_too_long_and_wrong_signature_denied() {
        let a = authorizer(groups("g", "g"));
        let n = now();
        let expired = token(serde_json::json!({"admin": true, "iat": n - 100, "exp": n - 10}));
        assert_eq!(
            a.authorize(&attrs(expired, Permission::Read)).unwrap(),
            Decision::Deny
        );
        let long = token(serde_json::json!({"admin": true, "iat": n, "ttl": 7200}));
        assert_eq!(
            a.authorize(&attrs(long, Permission::Read)).unwrap(),
            Decision::Deny
        );
        let no_exp = token(serde_json::json!({"admin": true, "iat": n}));
        assert_eq!(
            a.authorize(&attrs(no_exp, Permission::Read)).unwrap(),
            Decision::Deny
        );
        let mut ok = token(serde_json::json!({"admin": true, "iat": n, "ttl": 60}));
        ok.push('A');
        assert_eq!(
            a.authorize(&attrs(ok, Permission::Read)).unwrap(),
            Decision::Deny
        );
    }

    #[test]
    fn domain_lookup_error_is_returned() {
        let cfg = OAuthAuthorizerConfig {
            enable: true,
            max_jwt_ttl: 3600,
            jwt_credentials: Some(JwtCredentialsConfig {
                algorithm: "RS256".into(),
                public_key: pub_path(),
            }),
            provider: None,
        };
        let a = OAuthAuthorizer::new(&cfg, Arc::new(|_| Err("nope".into()))).unwrap();
        let t = token(serde_json::json!({"groups": "g", "iat": now(), "ttl": 60}));
        assert_eq!(
            a.authorize(&attrs(t, Permission::Read)).unwrap_err(),
            "nope"
        );
    }

    #[test]
    fn config_validation_matches_go() {
        let lookup: DomainDataLookup = Arc::new(|_| Ok(HashMap::new()));
        let mut cfg = OAuthAuthorizerConfig {
            enable: true,
            ..Default::default()
        };
        assert!(OAuthAuthorizer::new(&cfg, lookup.clone())
            .unwrap_err()
            .contains("MaxTTL"));
        cfg.max_jwt_ttl = 10;
        assert!(OAuthAuthorizer::new(&cfg, lookup.clone())
            .unwrap_err()
            .contains("jwtCredentials or provider"));
        cfg.jwt_credentials = Some(JwtCredentialsConfig {
            algorithm: "HS256".into(),
            public_key: pub_path(),
        });
        assert!(OAuthAuthorizer::new(&cfg, lookup.clone())
            .unwrap_err()
            .contains("RS256"));
        let both = AuthorizationConfig {
            oauth_authorizer: OAuthAuthorizerConfig {
                enable: true,
                ..Default::default()
            },
            noop_authorizer: cadence_service_core::config::NoopAuthorizerConfig { enable: true },
        };
        assert!(new_authorizer(&both, lookup.clone()).is_err());
        assert!(new_authorizer(&AuthorizationConfig::default(), lookup)
            .unwrap()
            .is_none());
    }
}
