//! OpenFeature-backed dynamic config client, mirroring Go
//! `common/dynamicconfig/openfeatureclient` (`client.go`).
//!
//! Go's client evaluates against OpenFeature's process-wide default provider
//! at call time. The same registration state lives here: one provider per
//! process, shared by every service attached to it, shut down only when the
//! last attached service deregisters.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use cadence_common::duration::parse_nanos;
use serde_json::{Map, Value};

use super::provider::{self, ErrorCode, FeatureProvider, FlagValue, FlattenedContext, ResolutionError};

/// Go `dynamicconfig.NotFoundError` message (`types.EntityNotExistsError`).
pub const NOT_FOUND_MESSAGE: &str = "unable to find key";

/// Error vocabulary of the OpenFeature client. Go returns `error` values;
/// only `NotFound` is special-cased by callers (it logs at debug, not warn).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    /// Go `dynamicconfig.NotFoundError`, returned when the provider reports
    /// `FLAG_NOT_FOUND`.
    NotFound,
    Message(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => f.write_str(NOT_FOUND_MESSAGE),
            Self::Message(message) => f.write_str(message),
        }
    }
}

/// Go `Registration` state guarded by `registerOnce`, `attachedMu` and
/// `attachedCount`.
struct Registration {
    /// True once the first `register_provider` call has run (Go `registerOnce`).
    initialized: bool,
    /// Sticky result of that first call: a failure is returned to every later
    /// caller, as Go's `registerErr` is.
    init_error: Option<String>,
    provider_name: String,
    /// Installed evaluation provider. `None` is OpenFeature's no-op default,
    /// which returns the caller's default without error.
    provider: Option<Arc<dyn FeatureProvider>>,
    /// Services currently attached. Signed to match Go's `attachedCount--`.
    attached: i64,
}

impl Registration {
    const EMPTY: Self = Self {
        initialized: false,
        init_error: None,
        provider_name: String::new(),
        provider: None,
        attached: 0,
    };
}

static REGISTRATION: Mutex<Registration> = Mutex::new(Registration::EMPTY);

fn registration() -> MutexGuard<'static, Registration> {
    REGISTRATION.lock().unwrap_or_else(|e| e.into_inner())
}

/// Go `RegisterProvider`. Registers `provider_name` as the process-wide
/// provider at most once. Every later call must name the same provider and
/// attaches to it. `config` is the provider's own `provider:` block.
pub fn register_provider(
    provider_name: &str,
    config: Option<&serde_yaml::Value>,
) -> anyhow::Result<()> {
    let mut registration = registration();
    if !registration.initialized {
        registration.initialized = true;
        match construct_and_init(provider_name, config) {
            Ok(provider) => {
                registration.provider_name = provider_name.to_string();
                registration.provider = Some(provider);
            }
            Err(error) => registration.init_error = Some(format!("{error:#}")),
        }
    }
    if let Some(error) = &registration.init_error {
        anyhow::bail!("{error}");
    }
    if registration.provider_name != provider_name {
        anyhow::bail!(
            "provider {:?} already registered in this process; cannot also use {provider_name:?}",
            registration.provider_name
        );
    }
    registration.attached += 1;
    Ok(())
}

fn construct_and_init(
    provider_name: &str,
    config: Option<&serde_yaml::Value>,
) -> anyhow::Result<Arc<dyn FeatureProvider>> {
    let constructor = provider::get(provider_name)
        .ok_or_else(|| anyhow::anyhow!("unknown openfeature provider: {provider_name:?}"))?;
    let provider = constructor(config).map_err(|error| {
        anyhow::anyhow!("failed to construct openfeature provider {provider_name:?}: {error:#}")
    })?;
    provider.init().map_err(|error| {
        anyhow::anyhow!("failed to set openfeature provider {provider_name:?}: {error:#}")
    })?;
    Ok(provider)
}

/// Go `DeregisterProvider`. Marks one attached service as stopped and shuts
/// the provider down once none remain. Only called after a successful
/// `register_provider`.
pub fn deregister_provider() {
    let mut registration = registration();
    registration.attached -= 1;
    if registration.attached == 0 {
        if let Some(provider) = registration.provider.take() {
            provider.shutdown();
        }
    }
}

fn current_provider() -> Option<Arc<dyn FeatureProvider>> {
    registration().provider.clone()
}

/// Go `translateErr`: only `FLAG_NOT_FOUND` becomes `NotFound`. Other codes
/// keep their message, wrapped as Go's `error code: %w`.
fn translate_err(error: &ResolutionError) -> ClientError {
    if error.code == ErrorCode::FlagNotFound {
        ClientError::NotFound
    } else {
        ClientError::Message(format!("error code: {error}"))
    }
}

/// Go `Client.evaluate` plus the typed `*ValueDetails` contract: on a
/// resolution error the caller's default is returned with the error, and with
/// no installed provider the default is returned with no error.
fn evaluate(
    flag: &str,
    default: FlagValue,
    filters: &FlattenedContext,
) -> (FlagValue, Option<ClientError>) {
    let Some(provider) = current_provider() else {
        return (default, None);
    };
    let resolution = provider.resolve(flag, default.clone(), filters);
    match resolution.error {
        None => (resolution.value, None),
        Some(error) => (default, Some(translate_err(&error))),
    }
}

/// Go `openFeatureClient`. Holds no state of its own: evaluations go through
/// the process-wide registration.
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenFeatureClient;

impl OpenFeatureClient {
    pub fn new() -> Self {
        Self
    }

    /// Go `GetValue`: an untyped object flag with no filters.
    pub fn get_value(&self, name: &str, default: Value) -> (Value, Option<ClientError>) {
        self.get_value_with_filters(name, default, &FlattenedContext::new())
    }

    /// Go `GetValueWithFilters`. Filters become evaluation-context attributes
    /// keyed by filter name; Go's `toEvalContext` attaches no targeting key.
    pub fn get_value_with_filters(
        &self,
        name: &str,
        default: Value,
        filters: &FlattenedContext,
    ) -> (Value, Option<ClientError>) {
        match evaluate(name, FlagValue::Object(default.clone()), filters) {
            (FlagValue::Object(value), None) => (value, None),
            (_, Some(error)) => (default, Some(error)),
            (other, None) => (default, Some(type_error("value", &other))),
        }
    }

    /// Go `GetIntValue`.
    pub fn get_int_value(
        &self,
        name: &str,
        default: i64,
        filters: &FlattenedContext,
    ) -> (i64, Option<ClientError>) {
        match evaluate(name, FlagValue::Int(default), filters) {
            (FlagValue::Int(value), None) => (value, None),
            (_, Some(error)) => (default, Some(error)),
            (other, None) => (default, Some(type_error("int", &other))),
        }
    }

    /// Go `GetFloatValue`.
    pub fn get_float_value(
        &self,
        name: &str,
        default: f64,
        filters: &FlattenedContext,
    ) -> (f64, Option<ClientError>) {
        match evaluate(name, FlagValue::Float(default), filters) {
            (FlagValue::Float(value), None) => (value, None),
            (_, Some(error)) => (default, Some(error)),
            (other, None) => (default, Some(type_error("float64", &other))),
        }
    }

    /// Go `GetBoolValue`.
    pub fn get_bool_value(
        &self,
        name: &str,
        default: bool,
        filters: &FlattenedContext,
    ) -> (bool, Option<ClientError>) {
        match evaluate(name, FlagValue::Bool(default), filters) {
            (FlagValue::Bool(value), None) => (value, None),
            (_, Some(error)) => (default, Some(error)),
            (other, None) => (default, Some(type_error("bool", &other))),
        }
    }

    /// Go `GetStringValue`.
    pub fn get_string_value(
        &self,
        name: &str,
        default: &str,
        filters: &FlattenedContext,
    ) -> (String, Option<ClientError>) {
        match evaluate(name, FlagValue::String(default.to_string()), filters) {
            (FlagValue::String(value), None) => (value, None),
            (_, Some(error)) => (default.to_string(), Some(error)),
            (other, None) => (default.to_string(), Some(type_error("string", &other))),
        }
    }

    /// Go `GetMapValue`: an object flag whose value must be a JSON object.
    pub fn get_map_value(
        &self,
        name: &str,
        default: Map<String, Value>,
        filters: &FlattenedContext,
    ) -> (Map<String, Value>, Option<ClientError>) {
        match evaluate(name, FlagValue::Object(Value::Object(default.clone())), filters) {
            (FlagValue::Object(Value::Object(value)), None) => (value, None),
            (FlagValue::Object(other), None) => (
                default,
                Some(ClientError::Message(format!(
                    "value type is not map but is: {}",
                    go_type_name(&other)
                ))),
            ),
            (_, Some(error)) => (default, Some(error)),
            (other, None) => (default, Some(type_error("map", &other))),
        }
    }

    /// Go `GetDurationValue`. Durations are stored as `time.ParseDuration`
    /// strings, the same convention as the file-based client. Returns nanos.
    pub fn get_duration_value(
        &self,
        name: &str,
        default_nanos: i64,
        filters: &FlattenedContext,
    ) -> (i64, Option<ClientError>) {
        let default_text = go_duration_string(default_nanos);
        match evaluate(name, FlagValue::String(default_text), filters) {
            (FlagValue::String(text), None) => match parse_nanos(&text) {
                Some(nanos) => (nanos, None),
                None => (
                    default_nanos,
                    Some(ClientError::Message(format!(
                        "failed to parse duration: time: invalid duration {text:?}"
                    ))),
                ),
            },
            (_, Some(error)) => (default_nanos, Some(error)),
            (other, None) => (default_nanos, Some(type_error("string", &other))),
        }
    }

    /// Go `GetListValue`: an object flag whose value must be a JSON array.
    pub fn get_list_value(
        &self,
        name: &str,
        default: Vec<Value>,
        filters: &FlattenedContext,
    ) -> (Vec<Value>, Option<ClientError>) {
        match evaluate(name, FlagValue::Object(Value::Array(default.clone())), filters) {
            (FlagValue::Object(Value::Array(value)), None) => (value, None),
            (FlagValue::Object(other), None) => (
                default,
                Some(ClientError::Message(format!(
                    "value type is not list but is: {}",
                    go_type_name(&other)
                ))),
            ),
            (_, Some(error)) => (default, Some(error)),
            (other, None) => (default, Some(type_error("list", &other))),
        }
    }

    /// Go `UpdateValue`: flags are managed in the provider's control plane.
    pub fn update_value(&self, _name: &str, _value: &Value) -> Result<(), ClientError> {
        Err(ClientError::Message(
            "not supported for openfeature client: manage flags via the configured provider"
                .to_string(),
        ))
    }

    /// Go `RestoreValue`.
    pub fn restore_value(
        &self,
        _name: &str,
        _filters: &FlattenedContext,
    ) -> Result<(), ClientError> {
        Err(ClientError::Message(
            "not supported for openfeature client".to_string(),
        ))
    }

    /// Go `ListValue`, which returns `[]*types.DynamicConfigEntry`. The
    /// OpenFeature client never lists, so it always errors and no entries
    /// are constructed.
    pub fn list_value(&self, _name: &str) -> Result<Vec<Value>, ClientError> {
        Err(ClientError::Message(
            "not supported for openfeature client".to_string(),
        ))
    }
}

/// Defensive only: a conforming provider returns the variant of the default
/// it was given, so this is unreachable for the providers in this crate.
fn type_error(expected: &str, got: &FlagValue) -> ClientError {
    ClientError::Message(format!("openfeature provider returned {got:?} for a {expected} flag"))
}

/// Go `%T` names for the JSON values a provider can return. Go decodes JSON
/// numbers as float64 and typed int64 values as int64.
fn go_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "<nil>",
        Value::Bool(_) => "bool",
        Value::Number(number) if number.is_i64() => "int64",
        Value::Number(_) => "float64",
        Value::String(_) => "string",
        Value::Array(_) => "[]interface {}",
        Value::Object(_) => "map[string]interface {}",
    }
}

/// Go `time.Duration.String`, so the default a provider receives matches
/// Go's string exactly.
pub fn go_duration_string(nanos: i64) -> String {
    if nanos == 0 {
        return "0s".to_string();
    }
    let body = if nanos.unsigned_abs() < 1_000_000_000 {
        let magnitude = nanos.unsigned_abs();
        let (unit, precision) = if magnitude < 1_000 {
            ("ns", 0)
        } else if magnitude < 1_000_000 {
            ("\u{b5}s", 3)
        } else {
            ("ms", 6)
        };
        let (fraction, whole) = fmt_fraction(magnitude, precision);
        format!("{whole}{fraction}{unit}")
    } else {
        let (fraction, seconds) = fmt_fraction(nanos.unsigned_abs(), 9);
        let mut text = format!("{}{fraction}s", seconds % 60);
        let minutes = seconds / 60;
        if minutes > 0 {
            text = format!("{}m{text}", minutes % 60);
            let hours = minutes / 60;
            if hours > 0 {
                text = format!("{hours}h{text}");
            }
        }
        text
    };
    if nanos < 0 {
        format!("-{body}")
    } else {
        body
    }
}

/// Go `time`'s `fmtFrac`: the lower `precision` digits as `.digits` with
/// trailing zeros removed (none when they are all zero), plus the whole part.
fn fmt_fraction(value: u64, precision: u32) -> (String, u64) {
    let scale = 10u64.pow(precision);
    let whole = value / scale;
    let remainder = value % scale;
    if remainder == 0 {
        return (String::new(), whole);
    }
    let digits = format!("{remainder:0width$}", width = precision as usize);
    (format!(".{}", digits.trim_end_matches('0')), whole)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::openfeature::provider::{Constructor, Resolution};

    /// Mirrors the `memprovider.InMemoryFlag` fields the Go tests use.
    type ContextEvaluator = Arc<dyn Fn(&FlattenedContext) -> Option<Value> + Send + Sync>;

    #[derive(Clone)]
    struct TestFlag {
        disabled: bool,
        /// Go `DefaultVariant` value.
        variant: Option<Value>,
        evaluator: Option<ContextEvaluator>,
    }

    fn static_variant(value: Value) -> TestFlag {
        TestFlag { disabled: false, variant: Some(value), evaluator: None }
    }

    /// Mirrors `memprovider.InMemoryProvider` resolution: missing flags are
    /// `FLAG_NOT_FOUND`, disabled flags are `GENERAL`, and a value of the
    /// wrong type is `TYPE_MISMATCH`, each returning the default.
    struct InMemoryProvider {
        flags: BTreeMap<String, TestFlag>,
        shutdowns: Arc<AtomicUsize>,
    }

    impl FeatureProvider for InMemoryProvider {
        fn shutdown(&self) {
            self.shutdowns.fetch_add(1, Ordering::SeqCst);
        }

        fn resolve(&self, flag: &str, default: FlagValue, ctx: &FlattenedContext) -> Resolution {
            let Some(definition) = self.flags.get(flag) else {
                return Resolution {
                    value: default,
                    error: Some(ResolutionError {
                        code: ErrorCode::FlagNotFound,
                        message: format!("flag for key {flag} not found"),
                    }),
                };
            };
            if definition.disabled {
                return Resolution {
                    value: default,
                    error: Some(ResolutionError {
                        code: ErrorCode::General,
                        message: "flag is disabled".to_string(),
                    }),
                };
            }
            let raw = match &definition.evaluator {
                Some(evaluator) => evaluator(ctx),
                None => definition.variant.clone(),
            };
            match raw.and_then(|raw| typed(&default, raw)) {
                Some(value) => Resolution { value, error: None },
                None => Resolution {
                    value: default,
                    error: Some(ResolutionError {
                        code: ErrorCode::TypeMismatch,
                        message: "incorrect type association".to_string(),
                    }),
                },
            }
        }
    }

    fn typed(default: &FlagValue, raw: Value) -> Option<FlagValue> {
        match default {
            FlagValue::Bool(_) => raw.as_bool().map(FlagValue::Bool),
            FlagValue::String(_) => raw.as_str().map(|s| FlagValue::String(s.to_string())),
            FlagValue::Int(_) => raw.as_i64().map(FlagValue::Int),
            FlagValue::Float(_) => raw.as_f64().map(FlagValue::Float),
            FlagValue::Object(_) => Some(FlagValue::Object(raw)),
        }
    }

    /// Serializes tests: registration is process-wide, as in Go, and Rust's
    /// test harness runs tests on parallel threads.
    fn test_lock() -> MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Go `resetOpenFeatureRegistration`.
    fn reset_registration() {
        *registration() = Registration::EMPTY;
    }

    /// Go `newTestClient`: register `name` with `flags` and attach a client.
    fn new_test_client(
        name: &str,
        flags: BTreeMap<String, TestFlag>,
    ) -> (OpenFeatureClient, Arc<AtomicUsize>) {
        reset_registration();
        let shutdowns = Arc::new(AtomicUsize::new(0));
        let shutdowns_for_provider = shutdowns.clone();
        let constructor: Constructor = Arc::new(
            move |_: Option<&serde_yaml::Value>| -> anyhow::Result<Arc<dyn FeatureProvider>> {
                Ok(Arc::new(InMemoryProvider {
                    flags: flags.clone(),
                    shutdowns: shutdowns_for_provider.clone(),
                }) as Arc<dyn FeatureProvider>)
            },
        );
        provider::register(name, constructor).expect("register test provider");
        register_provider(name, None).expect("attach test provider");
        (OpenFeatureClient::new(), shutdowns)
    }

    fn one_flag(key: &str, flag: TestFlag) -> BTreeMap<String, TestFlag> {
        BTreeMap::from([(key.to_string(), flag)])
    }

    fn filters(pairs: &[(&str, Value)]) -> FlattenedContext {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    fn no_filters() -> FlattenedContext {
        FlattenedContext::new()
    }

    fn is_message(error: &Option<ClientError>, needle: &str) -> bool {
        matches!(error, Some(ClientError::Message(message)) if message.contains(needle))
    }

    // Mirrors client_test.go TestRegisterProvider_UnknownProvider.
    #[test]
    fn register_unknown_provider_errors() {
        let _guard = test_lock();
        reset_registration();
        let err = register_provider("does-not-exist", None).unwrap_err();
        assert!(err.to_string().contains("unknown openfeature provider"));
    }

    // Mirrors TestRegisterProvider_ConstructorError.
    #[test]
    fn register_constructor_error_is_sticky() {
        let _guard = test_lock();
        reset_registration();
        let name = "test-constructor-error";
        let constructor: Constructor = Arc::new(
            |_: Option<&serde_yaml::Value>| -> anyhow::Result<Arc<dyn FeatureProvider>> {
                anyhow::bail!("boom")
            },
        );
        provider::register(name, constructor).unwrap();
        let err = register_provider(name, None).unwrap_err();
        assert!(err.to_string().contains("failed to construct openfeature provider"));
        // Go's registerOnce keeps the first outcome, so a retry sees it too.
        let again = register_provider(name, None).unwrap_err();
        assert_eq!(err.to_string(), again.to_string());
    }

    // Mirrors TestNewOpenFeatureClient_EndToEnd.
    #[test]
    fn end_to_end_bool() {
        let _guard = test_lock();
        let (client, _) = new_test_client(
            "test-end-to-end",
            one_flag("testGetBoolPropertyKey", static_variant(Value::Bool(true))),
        );
        let (value, err) = client.get_bool_value("testGetBoolPropertyKey", false, &no_filters());
        assert!(err.is_none());
        assert!(value);
        deregister_provider();
    }

    // Mirrors TestGetValue_TranslatesFlagNotFoundToNotFoundError.
    #[test]
    fn flag_not_found_translates_to_not_found() {
        let _guard = test_lock();
        let (client, _) = new_test_client("test-not-found-translate", BTreeMap::new());
        let (value, err) = client.get_bool_value("testGetBoolPropertyKey", true, &no_filters());
        assert_eq!(err, Some(ClientError::NotFound));
        assert!(value);
        deregister_provider();
    }

    // Mirrors TestRegisterProvider_SingletonAcrossCalls.
    #[test]
    fn provider_constructed_once_across_attachments() {
        let _guard = test_lock();
        reset_registration();
        let name = "test-singleton";
        let constructed = Arc::new(AtomicUsize::new(0));
        let constructed_for_constructor = constructed.clone();
        let constructor: Constructor = Arc::new(
            move |_: Option<&serde_yaml::Value>| -> anyhow::Result<Arc<dyn FeatureProvider>> {
                constructed_for_constructor.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(InMemoryProvider {
                    flags: one_flag("testGetBoolPropertyKey", static_variant(Value::Bool(true))),
                    shutdowns: Arc::new(AtomicUsize::new(0)),
                }) as Arc<dyn FeatureProvider>)
            },
        );
        provider::register(name, constructor).unwrap();
        register_provider(name, None).unwrap();
        register_provider(name, None).unwrap();
        assert_eq!(constructed.load(Ordering::SeqCst), 1);
        let (value, err) = OpenFeatureClient::new()
            .get_bool_value("testGetBoolPropertyKey", false, &no_filters());
        assert!(err.is_none());
        assert!(value);
        deregister_provider();
        deregister_provider();
    }

    // Mirrors TestRegisterProvider_RejectsMismatchedProvider.
    #[test]
    fn mismatched_provider_is_rejected() {
        let _guard = test_lock();
        reset_registration();
        let first = "test-mismatch-first";
        let second = "test-mismatch-second";
        let empty: BTreeMap<String, TestFlag> = BTreeMap::new();
        for name in [first, second] {
            let flags = empty.clone();
            let constructor: Constructor = Arc::new(
                move |_: Option<&serde_yaml::Value>| -> anyhow::Result<Arc<dyn FeatureProvider>> {
                    Ok(Arc::new(InMemoryProvider {
                        flags: flags.clone(),
                        shutdowns: Arc::new(AtomicUsize::new(0)),
                    }) as Arc<dyn FeatureProvider>)
                },
            );
            provider::register(name, constructor).unwrap();
        }
        register_provider(first, None).unwrap();
        let err = register_provider(second, None).unwrap_err();
        assert!(err.to_string().contains("already registered in this process"));
        deregister_provider();
    }

    // Mirrors TestDeregisterProvider_ShutdownOnlyAfterLastServiceStops, and
    // additionally asserts the provider's shutdown runs exactly once.
    #[test]
    fn shutdown_only_after_last_service_stops() {
        let _guard = test_lock();
        let (client, shutdowns) = new_test_client(
            "test-refcount",
            one_flag("testGetBoolPropertyKey", static_variant(Value::Bool(true))),
        );
        register_provider("test-refcount", None).unwrap();

        deregister_provider();
        assert_eq!(shutdowns.load(Ordering::SeqCst), 0);
        let (value, err) = client.get_bool_value("testGetBoolPropertyKey", false, &no_filters());
        assert!(err.is_none());
        assert!(value, "second service still attached: evaluation must still work");

        deregister_provider();
        assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
    }

    // OpenFeature's no-op default: after shutdown, evaluation returns the
    // caller's default and no error, and a re-attach does not reinstall the
    // provider (Go's registerOnce has already run).
    #[test]
    fn evaluation_after_shutdown_is_default_without_error() {
        let _guard = test_lock();
        let (client, _) = new_test_client(
            "test-after-shutdown",
            one_flag("testGetBoolPropertyKey", static_variant(Value::Bool(true))),
        );
        deregister_provider();
        let (value, err) = client.get_bool_value("testGetBoolPropertyKey", false, &no_filters());
        assert!(err.is_none());
        assert!(!value);
        register_provider("test-after-shutdown", None).unwrap();
        let (value, err) = client.get_bool_value("testGetBoolPropertyKey", false, &no_filters());
        assert!(err.is_none());
        assert!(!value);
        deregister_provider();
    }

    // Mirrors TestGetValue.
    #[test]
    fn get_value_object() {
        let _guard = test_lock();
        let (client, _) = new_test_client(
            "test-get-value",
            one_flag("testGetIntPropertyKey", static_variant(Value::from(42))),
        );
        let (value, err) = client.get_value("testGetIntPropertyKey", Value::from(0));
        assert!(err.is_none());
        assert_eq!(value, Value::from(42));
        deregister_provider();
    }

    // Mirrors TestGetValueWithFilters_NotFound_ReturnsDefault.
    #[test]
    fn get_value_with_filters_not_found_returns_default() {
        let _guard = test_lock();
        let (client, _) = new_test_client("test-value-filters-not-found", BTreeMap::new());
        let (value, err) = client.get_value_with_filters(
            "testGetIntPropertyKey",
            Value::from(9),
            &filters(&[("domainName", Value::from("some-domain"))]),
        );
        assert_eq!(err, Some(ClientError::NotFound));
        assert_eq!(value, Value::from(9));
        deregister_provider();
    }

    // Mirrors TestGetValueWithFilters_PropagatesFiltersAsContextAttributes:
    // the domain filter must reach the provider as a context attribute.
    #[test]
    fn filters_propagate_as_context_attributes() {
        let _guard = test_lock();
        let evaluator: ContextEvaluator = Arc::new(|ctx: &FlattenedContext| {
            Some(ctx.get("domainName").cloned().unwrap_or(Value::Null))
        });
        let flag = TestFlag { disabled: false, variant: None, evaluator: Some(evaluator) };
        let (client, _) = new_test_client(
            "test-filters-propagate",
            one_flag("testGetStringPropertyKey", flag),
        );
        let (value, err) = client.get_string_value(
            "testGetStringPropertyKey",
            "",
            &filters(&[("domainName", Value::from("my-domain"))]),
        );
        assert!(err.is_none());
        assert_eq!(value, "my-domain");
        deregister_provider();
    }

    // Mirrors TestGetIntValue and TestGetIntValue_NotFound_ReturnsDefault.
    #[test]
    fn get_int_value_and_not_found_default() {
        let _guard = test_lock();
        let (client, _) = new_test_client(
            "test-int",
            one_flag("testGetIntPropertyKey", static_variant(Value::from(7))),
        );
        let (value, err) = client.get_int_value("testGetIntPropertyKey", 0, &no_filters());
        assert!(err.is_none());
        assert_eq!(value, 7);
        deregister_provider();

        let (client, _) = new_test_client("test-int-missing", BTreeMap::new());
        let (value, err) = client.get_int_value("testGetIntPropertyKey", 5, &no_filters());
        assert_eq!(err, Some(ClientError::NotFound));
        assert_eq!(value, 5);
        deregister_provider();
    }

    // Mirrors TestGetFloatValue.
    #[test]
    fn get_float_value() {
        let _guard = test_lock();
        let (client, _) = new_test_client(
            "test-float",
            one_flag("testGetFloat64PropertyKey", static_variant(Value::from(4.25))),
        );
        let (value, err) = client.get_float_value("testGetFloat64PropertyKey", 0.0, &no_filters());
        assert!(err.is_none());
        assert_eq!(value, 4.25);
        deregister_provider();
    }

    // Mirrors TestGetStringValue.
    #[test]
    fn get_string_value() {
        let _guard = test_lock();
        let (client, _) = new_test_client(
            "test-string",
            one_flag("testGetStringPropertyKey", static_variant(Value::from("hello"))),
        );
        let (value, err) = client.get_string_value("testGetStringPropertyKey", "", &no_filters());
        assert!(err.is_none());
        assert_eq!(value, "hello");
        deregister_provider();
    }

    // Mirrors TestGetMapValue.
    #[test]
    fn get_map_value() {
        let _guard = test_lock();
        let want = serde_json::json!({"a": "b"});
        let (client, _) = new_test_client(
            "test-map",
            one_flag("testGetMapPropertyKey", static_variant(want.clone())),
        );
        let (value, err) = client.get_map_value("testGetMapPropertyKey", Map::new(), &no_filters());
        assert!(err.is_none());
        assert_eq!(Value::Object(value), want);
        deregister_provider();
    }

    // Mirrors TestGetMapValue_TypeMismatch.
    #[test]
    fn get_map_value_type_mismatch() {
        let _guard = test_lock();
        let (client, _) = new_test_client(
            "test-map-mismatch",
            one_flag("testGetMapPropertyKey", static_variant(Value::from("not-a-map"))),
        );
        let (value, err) = client.get_map_value("testGetMapPropertyKey", Map::new(), &no_filters());
        assert!(is_message(&err, "value type is not map but is"));
        assert_eq!(value, Map::new());
        deregister_provider();
    }

    // Mirrors TestGetDurationValue_StoredAsParseableString.
    #[test]
    fn get_duration_value_parseable_string() {
        let _guard = test_lock();
        let (client, _) = new_test_client(
            "test-duration",
            one_flag("testGetDurationPropertyKey", static_variant(Value::from("5s"))),
        );
        let (value, err) = client.get_duration_value("testGetDurationPropertyKey", 0, &no_filters());
        assert!(err.is_none());
        assert_eq!(value, 5_000_000_000);
        deregister_provider();
    }

    // Mirrors TestGetDurationValue_UnparseableString.
    #[test]
    fn get_duration_value_unparseable_string() {
        let _guard = test_lock();
        let (client, _) = new_test_client(
            "test-duration-bad",
            one_flag("testGetDurationPropertyKey", static_variant(Value::from("not-a-duration"))),
        );
        let (value, err) =
            client.get_duration_value("testGetDurationPropertyKey", 3_000_000_000, &no_filters());
        assert!(is_message(&err, "failed to parse duration"));
        assert_eq!(value, 3_000_000_000);
        deregister_provider();
    }

    // Mirrors TestGetListValue.
    #[test]
    fn get_list_value() {
        let _guard = test_lock();
        let want = vec![Value::from("a"), Value::from("b")];
        let (client, _) = new_test_client(
            "test-list",
            one_flag("testGetListPropertyKey", static_variant(Value::Array(want.clone()))),
        );
        let (value, err) = client.get_list_value("testGetListPropertyKey", vec![], &no_filters());
        assert!(err.is_none());
        assert_eq!(value, want);
        deregister_provider();
    }

    // Mirrors TestGetListValue_TypeMismatch.
    #[test]
    fn get_list_value_type_mismatch() {
        let _guard = test_lock();
        let (client, _) = new_test_client(
            "test-list-mismatch",
            one_flag("testGetListPropertyKey", static_variant(Value::from("not-a-list"))),
        );
        let (value, err) = client.get_list_value("testGetListPropertyKey", vec![], &no_filters());
        assert!(is_message(&err, "value type is not list but is"));
        assert!(value.is_empty());
        deregister_provider();
    }

    // Mirrors TestUnsupportedWriteOperations.
    #[test]
    fn unsupported_write_operations() {
        let _guard = test_lock();
        let (client, _) = new_test_client("test-writes", BTreeMap::new());
        let err = client.update_value("testGetIntPropertyKey", &Value::from(1)).unwrap_err();
        assert!(err.to_string().contains("not supported for openfeature client"));
        let err = client.restore_value("testGetIntPropertyKey", &no_filters()).unwrap_err();
        assert!(err.to_string().contains("not supported for openfeature client"));
        let err = client.list_value("testGetIntPropertyKey").unwrap_err();
        assert!(err.to_string().contains("not supported for openfeature client"));
        deregister_provider();
    }

    // A disabled flag is a GENERAL resolution error in memprovider, so Go
    // returns the default with an error (not NotFound).
    #[test]
    fn disabled_flag_returns_default_with_general_error() {
        let _guard = test_lock();
        let flag = TestFlag {
            disabled: true,
            variant: Some(Value::from(1)),
            evaluator: None,
        };
        let (client, _) = new_test_client("test-disabled", one_flag("testGetIntPropertyKey", flag));
        let (value, err) = client.get_int_value("testGetIntPropertyKey", 4, &no_filters());
        assert!(is_message(&err, "error code: GENERAL: flag is disabled"));
        assert_eq!(value, 4);
        deregister_provider();
    }

    #[test]
    fn go_duration_string_matches_go() {
        assert_eq!(go_duration_string(0), "0s");
        assert_eq!(go_duration_string(5_000_000_000), "5s");
        assert_eq!(go_duration_string(1_500_000_000), "1.5s");
        assert_eq!(go_duration_string(90_000_000_000), "1m30s");
        assert_eq!(go_duration_string(3_600_000_000_000), "1h0m0s");
        assert_eq!(go_duration_string(1_500_000), "1.5ms");
        assert_eq!(go_duration_string(1_500), "1.5\u{b5}s");
        assert_eq!(go_duration_string(500), "500ns");
        assert_eq!(go_duration_string(-2_000_000_000), "-2s");
    }

    #[test]
    fn matching_property_reads_are_live_typed_filtered_and_lifecycle_managed() {
        use cadence_persistence::dynamic_config;
        use std::sync::atomic::{AtomicBool, AtomicI64};

        struct RecordingProvider {
            value: AtomicI64,
            fail: AtomicBool,
            calls: Mutex<Vec<(String, FlagValue, FlattenedContext)>>,
            shutdowns: Arc<AtomicUsize>,
        }
        impl FeatureProvider for RecordingProvider {
            fn shutdown(&self) { self.shutdowns.fetch_add(1, Ordering::SeqCst); }
            fn resolve(&self, key: &str, default: FlagValue, ctx: &FlattenedContext) -> Resolution {
                self.calls.lock().unwrap().push((key.to_owned(), default.clone(), ctx.clone()));
                if self.fail.load(Ordering::SeqCst) {
                    return Resolution { value: default, error: Some(ResolutionError { code: ErrorCode::General, message: "offline".into() }) };
                }
                if key == "matching.contextValue" {
                    let domain = ctx.get("domainName").and_then(Value::as_str).unwrap_or_default();
                    let task_list = ctx.get("taskListName").and_then(Value::as_str).unwrap_or_default();
                    let task_type = ctx.get("taskType").and_then(Value::as_i64).unwrap_or_default();
                    let scoped = match (domain, task_list, task_type) {
                        ("domain-a", "queue-a", 0) => 41,
                        ("domain-b", "queue-b", 1) => 43,
                        _ => 0,
                    };
                    return Resolution { value: FlagValue::Int(scoped), error: None };
                }
                let value = self.value.load(Ordering::SeqCst);
                Resolution { value: FlagValue::Int(value), error: None }
            }
        }

        let _guard = test_lock();
        reset_registration();
        let shutdowns = Arc::new(AtomicUsize::new(0));
        let fake = Arc::new(RecordingProvider {
            value: AtomicI64::new(11), fail: AtomicBool::new(false),
            calls: Mutex::new(Vec::new()), shutdowns: shutdowns.clone(),
        });
        let provider_for_constructor = fake.clone();
        let name = "matching-runtime-reader-test";
        let constructor: Constructor = Arc::new(move |_| {
            Ok(provider_for_constructor.clone() as Arc<dyn FeatureProvider>)
        });
        provider::register(name, constructor).unwrap();
        register_provider(name, None).unwrap();

        let runtime = super::super::runtime::OpenFeatureRuntimeProvider::with_cluster_name("cluster-a");
        let mut overlay = crate::admin::DynamicConfigOverlay::default();
        overlay.set_runtime_provider(Some(runtime));
        let first = dynamic_config::task_list_integer(
            overlay.property_values("matching.maxTaskBatchSize"), Some("domain-a"), "queue-a", 0, 7,
        );
        assert_eq!(first, 11);
        fake.value.store(19, Ordering::SeqCst);
        let changed = dynamic_config::task_list_integer(
            overlay.property_values("matching.maxTaskBatchSize"), Some("domain-b"), "queue-b", 1, 7,
        );
        assert_eq!(changed, 19);
        assert_eq!(dynamic_config::task_list_integer(
            overlay.property_values("matching.contextValue"), Some("domain-a"), "queue-a", 0, 0,
        ), 41);
        assert_eq!(dynamic_config::task_list_integer(
            overlay.property_values("matching.contextValue"), Some("domain-b"), "queue-b", 1, 0,
        ), 43);

        let calls = fake.calls.lock().unwrap();
        assert_eq!(calls[0].0, "matching.maxTaskBatchSize");
        assert_eq!(calls[0].1, FlagValue::Int(7));
        assert_eq!(calls[0].2.get("domainName"), Some(&Value::String("domain-a".into())));
        assert_eq!(calls[0].2.get("taskListName"), Some(&Value::String("queue-a".into())));
        assert_eq!(calls[0].2.get("taskType"), Some(&Value::from(0)));
        assert_eq!(calls[0].2.get("clusterName"), Some(&Value::String("cluster-a".into())));
        assert_eq!(calls[1].2.get("domainName"), Some(&Value::String("domain-b".into())));
        assert_eq!(calls[1].2.get("taskType"), Some(&Value::from(1)));
        drop(calls);

        fake.fail.store(true, Ordering::SeqCst);
        assert_eq!(dynamic_config::integer(overlay.property_values("matching.rps"), None, 23), 23);
        fake.fail.store(false, Ordering::SeqCst);
        assert_eq!(dynamic_config::boolean(overlay.property_values("matching.enabled"), None, true), true);
        deregister_provider();
        assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
    }
}
