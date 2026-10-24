//! OpenFeature provider contract and plugin registry, mirroring Go
//! `common/dynamicconfig/openfeatureprovider` (`provider.go`).
//!
//! Go's `openfeature.FeatureProvider` has one typed evaluation per flag type.
//! Here the type of the evaluation is carried by the [`FlagValue`] default
//! passed to [`FeatureProvider::resolve`], so one method serves all five.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use serde_json::Value;

/// Attributes handed to a provider, keyed by Cadence filter name
/// (`domainName`, `taskListName`, `taskType`, `clusterName`). Go's
/// `FlattenedContext`.
pub type FlattenedContext = BTreeMap<String, Value>;

/// A flag value of one OpenFeature type. Go passes `any` for object flags.
#[derive(Debug, Clone, PartialEq)]
pub enum FlagValue {
    Bool(bool),
    String(String),
    Int(i64),
    Float(f64),
    Object(Value),
}

/// OpenFeature error codes (`go-sdk/openfeature` `ErrorCode`), with the same
/// wire strings the SDK prints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    ProviderNotReady,
    FlagNotFound,
    ParseError,
    TypeMismatch,
    TargetingKeyMissing,
    InvalidContext,
    ProviderFatal,
    General,
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ProviderNotReady => "PROVIDER_NOT_READY",
            Self::FlagNotFound => "FLAG_NOT_FOUND",
            Self::ParseError => "PARSE_ERROR",
            Self::TypeMismatch => "TYPE_MISMATCH",
            Self::TargetingKeyMissing => "TARGETING_KEY_MISSING",
            Self::InvalidContext => "INVALID_CONTEXT",
            Self::ProviderFatal => "PROVIDER_FATAL",
            Self::General => "GENERAL",
        })
    }
}

/// Go `openfeature.ResolutionError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolutionError {
    pub code: ErrorCode,
    pub message: String,
}

impl fmt::Display for ResolutionError {
    /// Go `ResolutionError.Error`: `CODE: message`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

/// Go `openfeature.InterfaceResolutionDetail` reduced to what the dynamic
/// config client reads: the value and the resolution error, if any. The
/// variant, reason and metadata fields are not consumed by the client.
#[derive(Debug, Clone, PartialEq)]
pub struct Resolution {
    pub value: FlagValue,
    pub error: Option<ResolutionError>,
}

/// Go `openfeature.FeatureProvider`.
pub trait FeatureProvider: Send + Sync {
    /// Go `Init`, run once by `RegisterProvider` before the provider serves
    /// evaluations. An error fails registration.
    fn init(&self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Go `Shutdown`, run when the last attached service deregisters.
    fn shutdown(&self) {}

    /// Evaluate `flag`. The flag type is the variant of `default`.
    fn resolve(&self, flag: &str, default: FlagValue, ctx: &FlattenedContext) -> Resolution;
}

impl fmt::Debug for dyn FeatureProvider {
    /// Needed so `Result<Arc<dyn FeatureProvider>, _>` supports `unwrap_err`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FeatureProvider")
    }
}

/// Go `openfeatureprovider.Constructor`. The config is the provider's own
/// `provider:` block; `None` is Go's nil `*yaml.Node`.
pub type Constructor = Arc<
    dyn Fn(Option<&serde_yaml::Value>) -> anyhow::Result<Arc<dyn FeatureProvider>> + Send + Sync,
>;

static CONSTRUCTORS: Mutex<BTreeMap<String, Constructor>> = Mutex::new(BTreeMap::new());

/// Go `openfeatureprovider.Register`: a provider's constructor is registered
/// once per name.
pub fn register(name: &str, constructor: Constructor) -> anyhow::Result<()> {
    let mut constructors = CONSTRUCTORS.lock().unwrap_or_else(|e| e.into_inner());
    if constructors.contains_key(name) {
        anyhow::bail!("openfeature provider {name:?} already registered");
    }
    constructors.insert(name.to_string(), constructor);
    Ok(())
}

/// Go `openfeatureprovider.Get`.
pub fn get(name: &str) -> Option<Constructor> {
    CONSTRUCTORS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(name)
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoopProvider;

    impl FeatureProvider for NoopProvider {
        fn resolve(&self, _flag: &str, default: FlagValue, _ctx: &FlattenedContext) -> Resolution {
            Resolution { value: default, error: None }
        }
    }

    fn constructor() -> Constructor {
        Arc::new(
            |_: Option<&serde_yaml::Value>| -> anyhow::Result<Arc<dyn FeatureProvider>> {
                Ok(Arc::new(NoopProvider) as Arc<dyn FeatureProvider>)
            },
        )
    }

    // Mirrors provider_test.go TestRegisterAndGet.
    #[test]
    fn register_and_get() {
        let name = "test-provider-register-and-get";
        assert!(register(name, constructor()).is_ok());
        assert!(get(name).is_some());
    }

    #[test]
    fn duplicate_registration_fails() {
        let name = "test-provider-duplicate";
        assert!(register(name, constructor()).is_ok());
        let err = register(name, constructor()).unwrap_err();
        assert!(err.to_string().contains("already registered"));
    }

    // Mirrors provider_test.go TestGet_Unknown.
    #[test]
    fn get_unknown_is_none() {
        assert!(get("does-not-exist-provider").is_none());
    }

    #[test]
    fn resolution_error_display_matches_go() {
        let err = ResolutionError {
            code: ErrorCode::General,
            message: "flag is disabled".into(),
        };
        assert_eq!(err.to_string(), "GENERAL: flag is disabled");
    }
}
