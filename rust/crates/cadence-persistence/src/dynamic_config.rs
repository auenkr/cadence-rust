//! Runtime typed reads from Go configstore/config_store_client.go. Admin
//! listing/getting entries and typed runtime properties are distinct APIs.
use cadence_api::{admin::DynamicConfigValue, enums::EncodingType, types::DataBlob};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{collections::BTreeMap, sync::Arc};

/// Runtime evaluation boundary for named dynamic properties. Keeping the flag
/// name, JSON typed default and filter attributes together prevents a caller
/// from accidentally turning a filter-aware read into a snapshot lookup.
/// Implementations return an error for provider failures and type mismatches;
/// callers then use the supplied default, as Go's typed collection does.
pub trait RuntimePropertyProvider: Send + Sync + std::fmt::Debug {
    fn evaluate(
        &self,
        key: &str,
        default: Value,
        filters: &BTreeMap<String, Value>,
    ) -> Result<Value, String>;
}

/// An explicit key-preserving runtime read. File and config-store values stay
/// in `DynamicConfigOverlay`; this small handle is only used when a Matching
/// consumer performs a typed property read.
#[derive(Clone)]
pub struct RuntimePropertyRead {
    key: String,
    provider: Arc<dyn RuntimePropertyProvider>,
}

impl std::fmt::Debug for RuntimePropertyRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimePropertyRead").field("key", &self.key).finish()
    }
}

impl RuntimePropertyRead {
    pub fn new(key: impl Into<String>, provider: Arc<dyn RuntimePropertyProvider>) -> Self {
        Self { key: key.into(), provider }
    }

    pub fn key(&self) -> &str { &self.key }

    pub fn evaluate(&self, default: Value, filters: &BTreeMap<String, Value>) -> Result<Value, String> {
        self.provider.evaluate(&self.key, default, filters)
    }
}

/// Snapshot values paired with their property identity and optional live
/// evaluator. The Deref implementation keeps read-only inspection helpers
/// source compatible; typed property functions use `evaluate` below.
#[derive(Clone, Debug)]
pub struct PropertyValues<'a> {
    values: &'a [DynamicConfigValue],
    runtime: Option<RuntimePropertyRead>,
}

impl<'a> PropertyValues<'a> {
    pub fn snapshot(_key: impl Into<String>, values: &'a [DynamicConfigValue]) -> Self {
        Self { values, runtime: None }
    }
    pub fn with_runtime(mut self, runtime: Option<RuntimePropertyRead>) -> Self {
        self.runtime = runtime;
        self
    }
    pub fn evaluate(&self, default: Value, filters: &BTreeMap<String, Value>) -> Option<Result<Value, String>> {
        self.runtime.as_ref().map(|runtime| runtime.evaluate(default, filters))
    }
}

impl std::ops::Deref for PropertyValues<'_> {
    type Target = [DynamicConfigValue];
    fn deref(&self) -> &Self::Target { self.values }
}

pub trait PropertyValueSource {
    fn snapshot_values(&self) -> &[DynamicConfigValue];
    fn runtime_value(&self, default: Value, filters: &BTreeMap<String, Value>) -> Option<Result<Value, String>>;
}

impl PropertyValueSource for PropertyValues<'_> {
    fn snapshot_values(&self) -> &[DynamicConfigValue] { self.values }
    fn runtime_value(&self, default: Value, filters: &BTreeMap<String, Value>) -> Option<Result<Value, String>> { self.evaluate(default, filters) }
}
impl PropertyValueSource for &PropertyValues<'_> {
    fn snapshot_values(&self) -> &[DynamicConfigValue] { self.values }
    fn runtime_value(&self, default: Value, filters: &BTreeMap<String, Value>) -> Option<Result<Value, String>> { self.evaluate(default, filters) }
}

impl PropertyValueSource for &[DynamicConfigValue] {
    fn snapshot_values(&self) -> &[DynamicConfigValue] { self }
    fn runtime_value(&self, _: Value, _: &BTreeMap<String, Value>) -> Option<Result<Value, String>> { None }
}
impl PropertyValueSource for &Vec<DynamicConfigValue> {
    fn snapshot_values(&self) -> &[DynamicConfigValue] { self.as_slice() }
    fn runtime_value(&self, _: Value, _: &BTreeMap<String, Value>) -> Option<Result<Value, String>> { None }
}
impl<const N: usize> PropertyValueSource for &[DynamicConfigValue; N] {
    fn snapshot_values(&self) -> &[DynamicConfigValue] { self.as_slice() }
    fn runtime_value(&self, _: Value, _: &BTreeMap<String, Value>) -> Option<Result<Value, String>> { None }
}

fn property_filters(domain: Option<&str>, task_list: Option<(&str, i32)>) -> BTreeMap<String, Value> {
    let mut filters = BTreeMap::new();
    if let Some(domain) = domain { filters.insert("domainName".into(), Value::String(domain.into())); }
    if let Some((task_list, task_type)) = task_list {
        filters.insert("taskListName".into(), Value::String(task_list.into()));
        filters.insert("taskType".into(), Value::from(task_type));
    }
    filters
}

fn runtime_typed<T>(result: Result<Value, String>, _default: T, property_type: &str) -> Option<Value> {
    match result {
        Ok(value) => Some(value),
        Err(error) => { report_read_error(property_type, &error); None }
    }
}

fn duration_default(nanos: i64) -> String {
    if nanos == 0 { return "0s".into(); }
    if nanos % 1_000_000_000 == 0 { return format!("{}s", nanos / 1_000_000_000); }
    if nanos % 1_000_000 == 0 { return format!("{}ms", nanos / 1_000_000); }
    if nanos % 1_000 == 0 { return format!("{}µs", nanos / 1_000); }
    format!("{nanos}ns")
}

static READ_ERROR_COUNT: AtomicU64 = AtomicU64::new(0);

fn decode(blob: Option<&DataBlob>) -> Option<Value> {
    let blob = blob?;
    if blob.encoding_type != Some(EncodingType::JSON) {
        return None;
    }
    serde_json::from_slice(&blob.data).ok()
}

pub fn filters_match(value: &DynamicConfigValue, domain: Option<&str>) -> bool {
    filters_match_context(value, domain, None)
}

fn filters_match_context(
    value: &DynamicConfigValue,
    domain: Option<&str>,
    task_list: Option<(&str, i32)>,
) -> bool {
    if value.filters.len() > usize::from(domain.is_some()) + 2 * usize::from(task_list.is_some()) {
        return false;
    }
    value.filters.iter().all(|filter| {
        let expected = match filter.name.as_str() {
            "domainName" => domain.map(|name| Value::String(name.into())),
            "taskListName" => task_list.map(|(name, _)| Value::String(name.into())),
            "taskType" => task_list.map(|(_, kind)| Value::from(kind)),
            _ => None,
        };
        expected.is_some() && decode(filter.value.as_ref()) == expected
    })
}

fn selected(values: &[DynamicConfigValue], domain: Option<&str>) -> Option<Value> {
    selected_context(values, domain, None)
}

// Entries are ordered, not ranked by specificity: the first matching filtered
// entry wins; the last decodable unfiltered entry is the fallback.
fn selected_context(
    values: &[DynamicConfigValue],
    domain: Option<&str>,
    task_list: Option<(&str, i32)>,
) -> Option<Value> {
    let mut fallback = None;
    for value in values {
        if value.filters.is_empty() {
            if let Some(value) = decode(value.value.as_ref()) {
                fallback = Some(value);
            }
        } else if filters_match_context(value, domain, task_list) {
            return decode(value.value.as_ref());
        }
    }
    fallback
}

fn report_read_error(property_type: &str, reason: &str) {
    // Go's dynamic-config Collection logs typed client errors before returning
    // the property's default. This module has no injected server logger, so
    // preserve that diagnostic on the process error stream.
    if READ_ERROR_COUNT
        .fetch_add(1, Ordering::Relaxed)
        .is_multiple_of(1_000)
    {
        eprintln!("dynamic config {property_type} read failed ({reason}); using default");
    }
}

fn integer_value(value: Value) -> Option<i64> {
    value.as_i64()
}

/// Matching properties scoped by domain name, physical task-list name and the
/// persistence task type (0 decision, 1 activity), not the public API enum.
pub fn task_list_integer(
    values: impl PropertyValueSource,
    domain: Option<&str>,
    task_list: &str,
    task_type: i32,
    default: i64,
) -> i64 {
    let filters = property_filters(domain, Some((task_list, task_type)));
    if let Some(result) = values.runtime_value(Value::from(default), &filters) {
        return runtime_typed(result, default, "integer").and_then(integer_value).unwrap_or(default);
    }
    match selected_context(values.snapshot_values(), domain, Some((task_list, task_type))) {
        Some(value) => integer_value(value).unwrap_or_else(|| {
            report_read_error("integer", "selected value has the wrong type");
            default
        }),
        None => {
            report_read_error("integer", "value was not found");
            default
        }
    }
}

pub fn task_list_boolean(
    values: impl PropertyValueSource,
    domain: Option<&str>,
    task_list: &str,
    task_type: i32,
    default: bool,
) -> bool {
    let filters = property_filters(domain, Some((task_list, task_type)));
    if let Some(result) = values.runtime_value(Value::Bool(default), &filters) {
        return runtime_typed(result, default, "boolean").and_then(|value| value.as_bool()).unwrap_or(default);
    }
    match selected_context(values.snapshot_values(), domain, Some((task_list, task_type)))
        .and_then(|value| value.as_bool())
    {
        Some(value) => value,
        None => {
            report_read_error("boolean", "value was missing or had the wrong type");
            default
        }
    }
}

/// Go `GetFloat64PropertyFilteredByTaskListInfo` reads, e.g. the scaler's
/// downscale hysteresis factor. Non-finite values fall back to the default.
pub fn task_list_float(
    values: impl PropertyValueSource,
    domain: Option<&str>,
    task_list: &str,
    task_type: i32,
    default: f64,
) -> f64 {
    let filters = property_filters(domain, Some((task_list, task_type)));
    if let Some(result) = values.runtime_value(serde_json::json!(default), &filters) {
        return runtime_typed(result, default, "float").and_then(|value| value.as_f64()).filter(|v| v.is_finite()).unwrap_or(default);
    }
    match selected_context(values.snapshot_values(), domain, Some((task_list, task_type)))
        .and_then(|value| value.as_f64())
        .filter(|value| value.is_finite())
    {
        Some(value) => value,
        None => {
            report_read_error("float", "value was missing or invalid");
            default
        }
    }
}

pub fn task_list_duration(
    values: impl PropertyValueSource,
    domain: Option<&str>,
    task_list: &str,
    task_type: i32,
    default: i64,
) -> i64 {
    let filters = property_filters(domain, Some((task_list, task_type)));
    if let Some(result) = values.runtime_value(Value::String(duration_default(default)), &filters) {
        return runtime_typed(result, default, "duration").and_then(|value| value.as_str().and_then(cadence_common::duration::parse_nanos)).unwrap_or(default);
    }
    match selected_context(values.snapshot_values(), domain, Some((task_list, task_type))).and_then(|value| {
        value
            .as_str()
            .and_then(cadence_common::duration::parse_nanos)
    }) {
        Some(value) => value,
        None => {
            report_read_error(
                "duration",
                "value was missing, had the wrong type, or was invalid",
            );
            default
        }
    }
}

pub fn integer(values: impl PropertyValueSource, domain: Option<&str>, default: i64) -> i64 {
    let filters = domain.map(|v| BTreeMap::from([("domainName".into(), Value::String(v.into()))])).unwrap_or_default();
    if let Some(result) = values.runtime_value(Value::from(default), &filters) {
        return runtime_typed(result, default, "integer").and_then(integer_value).unwrap_or(default);
    }
    match selected(values.snapshot_values(), domain).and_then(integer_value) {
        Some(value) => value,
        None => {
            report_read_error("integer", "value was missing or had the wrong type");
            default
        }
    }
}

pub fn boolean(values: impl PropertyValueSource, domain: Option<&str>, default: bool) -> bool {
    let filters = domain.map(|v| BTreeMap::from([("domainName".into(), Value::String(v.into()))])).unwrap_or_default();
    if let Some(result) = values.runtime_value(Value::Bool(default), &filters) {
        return runtime_typed(result, default, "boolean").and_then(|v| v.as_bool()).unwrap_or(default);
    }
    match selected(values.snapshot_values(), domain).and_then(|value| value.as_bool()) {
        Some(value) => value,
        None => {
            report_read_error("boolean", "value was missing or had the wrong type");
            default
        }
    }
}

pub fn duration(values: impl PropertyValueSource, domain: Option<&str>, default: i64) -> i64 {
    let filters = domain.map(|v| BTreeMap::from([("domainName".into(), Value::String(v.into()))])).unwrap_or_default();
    if let Some(result) = values.runtime_value(Value::String(duration_default(default)), &filters) {
        return runtime_typed(result, default, "duration").and_then(|v| v.as_str().and_then(cadence_common::duration::parse_nanos)).unwrap_or(default);
    }
    match selected(values.snapshot_values(), domain).and_then(|value| {
        value
            .as_str()
            .and_then(cadence_common::duration::parse_nanos)
    }) {
        Some(value) => value,
        None => {
            report_read_error(
                "duration",
                "value was missing, had the wrong type, or was invalid",
            );
            default
        }
    }
}

/// Go `GetFloat64Property` without filters. Non-finite or non-numeric values
/// fall back to the default.
pub fn float(values: impl PropertyValueSource, default: f64) -> f64 {
    if let Some(result) = values.runtime_value(serde_json::json!(default), &BTreeMap::new()) {
        return runtime_typed(result, default, "float").and_then(|v| v.as_f64()).filter(|v| v.is_finite()).unwrap_or(default);
    }
    match selected(values.snapshot_values(), None)
        .and_then(|value| value.as_f64())
        .filter(|value| value.is_finite())
    {
        Some(value) => value,
        None => {
            report_read_error("float", "value was missing or invalid");
            default
        }
    }
}

/// Go `GetBoolPropertyFilteredByDomainID` (filter `domainID`). Like the other
/// readers, the first matching filtered entry wins and the last decodable
/// unfiltered entry is the fallback.
pub fn boolean_domain_id(
    values: impl PropertyValueSource,
    domain_id: &str,
    default: bool,
) -> bool {
    let filters = BTreeMap::from([("domainID".into(), Value::String(domain_id.into()))]);
    if let Some(result) = values.runtime_value(Value::Bool(default), &filters) {
        return runtime_typed(result, default, "boolean").and_then(|v| v.as_bool()).unwrap_or(default);
    }
    let values = values.snapshot_values();
    let mut fallback = None;
    for value in values {
        if value.filters.is_empty() {
            if let Some(value) = decode(value.value.as_ref()) {
                fallback = Some(value);
            }
        } else if value.filters.len() == 1
            && value.filters[0].name == "domainID"
            && decode(value.filters[0].value.as_ref()) == Some(Value::String(domain_id.into()))
        {
            fallback = decode(value.value.as_ref());
            break;
        }
    }
    match fallback.and_then(|value| value.as_bool()) {
        Some(value) => value,
        None => {
            report_read_error("boolean", "value was missing or had the wrong type");
            default
        }
    }
}

/// Go `GetStringProperty` without filters. An unset key is not an error.
pub fn string(values: impl PropertyValueSource, default: &str) -> String {
    let filters = BTreeMap::new();
    if let Some(result) = values.runtime_value(Value::String(default.into()), &filters) {
        return runtime_typed(result, default, "string").and_then(|v| v.as_str().map(str::to_owned)).unwrap_or_else(|| default.into());
    }
    let values = values.snapshot_values();
    match selected(values, None).and_then(|value| value.as_str().map(str::to_owned)) {
        Some(value) => value,
        None => {
            if !values.is_empty() {
                report_read_error("string", "value was missing or had the wrong type");
            }
            default.to_owned()
        }
    }
}

/// Go `MapAllIsolationGroupsResponse`: one non-string rejects the whole list.
pub fn string_list(values: impl PropertyValueSource) -> Vec<String> {
    if let Some(result) = values.runtime_value(Value::Array(Vec::new()), &BTreeMap::new()) {
        return runtime_typed(result, (), "string list").and_then(|v| v.as_array().and_then(|items| items.iter().map(|v| v.as_str().map(str::to_owned)).collect())).unwrap_or_default();
    }
    match selected(values.snapshot_values(), None)
        .and_then(|value| {
            value.as_array().map(|items| {
                items
                    .iter()
                    .map(|item| item.as_str().map(str::to_owned))
                    .collect::<Option<Vec<_>>>()
            })
        })
        .flatten()
    {
        Some(value) => value,
        None => {
            report_read_error(
                "string list",
                "value was missing or contained non-string entries",
            );
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::admin::DynamicConfigFilter;
    use serde_json::json;

    fn entry(value: Value, filters: &[(&str, Value)]) -> DynamicConfigValue {
        let blob = |value: &Value| {
            Some(DataBlob::new(
                EncodingType::JSON,
                serde_json::to_vec(value).unwrap(),
            ))
        };
        DynamicConfigValue {
            value: blob(&value),
            filters: filters
                .iter()
                .map(|(name, value)| DynamicConfigFilter {
                    name: (*name).into(),
                    value: blob(value),
                })
                .collect(),
        }
    }

    #[test]
    fn domain_id_filtered_boolean_and_plain_float_readers() {
        let values = vec![
            entry(json!(false), &[]),
            entry(json!(true), &[("domainID", json!("d1"))]),
            entry(json!(false), &[("domainName", json!("d2"))]),
        ];
        assert!(boolean_domain_id(&values, "d1", false));
        assert!(!boolean_domain_id(&values, "d2", false));
        assert!(boolean_domain_id(&[], "d1", true));
        assert_eq!(float(&[entry(json!(0.25), &[])], 0.0), 0.25);
        assert_eq!(float(&[entry(json!("x"), &[])], 0.5), 0.5);
        assert_eq!(float(&[], 0.0), 0.0);
    }

    #[test]
    fn task_list_filters_require_every_constraint_and_preserve_entry_order() {
        let values = vec![
            entry(json!(2), &[]),
            entry(
                json!(7),
                &[
                    ("domainName", json!("domain")),
                    ("taskListName", json!("queue")),
                    ("taskType", json!(0)),
                ],
            ),
            entry(json!(5), &[("taskListName", json!("queue"))]),
            entry(json!(3), &[]),
        ];
        for (domain, list, kind, expected) in [
            (Some("domain"), "queue", 0, 7),
            (Some("domain"), "queue", 1, 5),
            (Some("other"), "queue", 0, 5),
            (None, "queue", 0, 5),
            (Some("domain"), "other", 0, 3),
            (Some("domain"), "/__cadence_sys/queue/1", 0, 3),
        ] {
            assert_eq!(task_list_integer(&values, domain, list, kind, 1), expected);
        }
        assert_eq!(
            integer(&values, Some("domain"), 1),
            3,
            "domain-only callers must not match task-list filters"
        );
        let broad_first = vec![values[2].clone(), values[1].clone()];
        assert_eq!(
            task_list_integer(&broad_first, Some("domain"), "queue", 0, 1),
            5
        );
    }

    #[test]
    fn scoped_duration_and_invalid_values_fall_back_without_skipping_matches() {
        let values = vec![
            entry(json!("2s"), &[]),
            entry(json!("50ms"), &[("taskType", json!(1))]),
        ];
        assert_eq!(
            task_list_duration(&values, None, "queue", 1, 10),
            50_000_000
        );
        assert_eq!(
            task_list_duration(&values, None, "queue", 0, 10),
            2_000_000_000
        );
        for filters in [vec![("taskType", json!("1"))], vec![("unknown", json!(1))]] {
            assert_eq!(
                task_list_integer(&[entry(json!(8), &filters)], None, "queue", 1, 4),
                4
            );
        }
        let values = vec![
            entry(json!(9), &[]),
            entry(json!(1.5), &[("taskType", json!(0))]),
            entry(json!(6), &[("taskType", json!(0))]),
        ];
        assert_eq!(task_list_integer(&values, None, "queue", 0, 4), 4);
        assert_eq!(
            integer(&[entry(json!(5.0), &[])], None, 9),
            9,
            "Go GetIntValue rejects YAML float values even when mathematically integral"
        );
        assert!(!boolean(&[entry(json!("true"), &[])], None, false));
        assert_eq!(
            integer(&[], None, 9),
            9,
            "missing keys retain their default"
        );
    }

    #[test]
    fn isolation_group_list_rejects_partial_decoding_and_filtered_entries() {
        assert_eq!(
            string_list(&[entry(json!(["a", "b", "a"]), &[])]),
            vec!["a", "b", "a"]
        );
        for value in [json!(["a", 1]), json!({"a": true}), json!(null)] {
            assert!(string_list(&[entry(value, &[])]).is_empty());
        }
        assert!(string_list(&[entry(json!(["a"]), &[("domainName", json!("domain"))])]).is_empty());
    }
}
