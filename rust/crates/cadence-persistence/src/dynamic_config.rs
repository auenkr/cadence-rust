//! Runtime typed reads from Go configstore/config_store_client.go. Admin
//! listing/getting entries and typed runtime properties are distinct APIs.
use cadence_api::{admin::DynamicConfigValue, enums::EncodingType, types::DataBlob};
use serde_json::Value;

fn decode(blob: Option<&DataBlob>) -> Option<Value> {
    let blob = blob?;
    if blob.encoding_type != Some(EncodingType::JSON) {
        return None;
    }
    serde_json::from_slice(&blob.data).ok()
}

pub fn filters_match(value: &DynamicConfigValue, domain: Option<&str>) -> bool {
    if value.filters.len() > usize::from(domain.is_some()) {
        return false;
    }
    value.filters.iter().all(|filter| {
        filter.name == "domainName"
            && domain.is_some_and(|domain| {
                decode(filter.value.as_ref()) == Some(Value::String(domain.into()))
            })
    })
}

fn selected(values: &[DynamicConfigValue], domain: Option<&str>) -> Option<Value> {
    let mut fallback = None;
    for value in values {
        if value.filters.is_empty() {
            if let Some(value) = decode(value.value.as_ref()) {
                fallback = Some(value);
            }
        } else if filters_match(value, domain) {
            return decode(value.value.as_ref());
        }
    }
    fallback
}

pub fn integer(values: &[DynamicConfigValue], domain: Option<&str>, default: i64) -> i64 {
    selected(values, domain)
        .and_then(|value| value.as_f64())
        .filter(|value| value.is_finite() && value.trunc() == *value)
        .map(|value| value as i64)
        .unwrap_or(default)
}

pub fn duration(values: &[DynamicConfigValue], domain: Option<&str>, default: i64) -> i64 {
    selected(values, domain)
        .and_then(|value| {
            value
                .as_str()
                .and_then(cadence_common::duration::parse_nanos)
        })
        .unwrap_or(default)
}
