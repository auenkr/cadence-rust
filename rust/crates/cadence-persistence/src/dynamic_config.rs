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

/// Matching properties scoped by domain name, physical task-list name and the
/// persistence task type (0 decision, 1 activity), not the public API enum.
pub fn task_list_integer(
    values: &[DynamicConfigValue],
    domain: Option<&str>,
    task_list: &str,
    task_type: i32,
    default: i64,
) -> i64 {
    selected_context(values, domain, Some((task_list, task_type)))
        .and_then(|value| value.as_f64())
        .filter(|value| value.is_finite() && value.trunc() == *value)
        .map(|value| value as i64)
        .unwrap_or(default)
}

pub fn task_list_boolean(
    values: &[DynamicConfigValue],
    domain: Option<&str>,
    task_list: &str,
    task_type: i32,
    default: bool,
) -> bool {
    selected_context(values, domain, Some((task_list, task_type)))
        .and_then(|value| value.as_bool())
        .unwrap_or(default)
}

/// Go `GetFloat64PropertyFilteredByTaskListInfo` reads, e.g. the scaler's
/// downscale hysteresis factor. Non-finite values fall back to the default.
pub fn task_list_float(
    values: &[DynamicConfigValue],
    domain: Option<&str>,
    task_list: &str,
    task_type: i32,
    default: f64,
) -> f64 {
    selected_context(values, domain, Some((task_list, task_type)))
        .and_then(|value| value.as_f64())
        .filter(|value| value.is_finite())
        .unwrap_or(default)
}

pub fn task_list_duration(
    values: &[DynamicConfigValue],
    domain: Option<&str>,
    task_list: &str,
    task_type: i32,
    default: i64,
) -> i64 {
    selected_context(values, domain, Some((task_list, task_type)))
        .and_then(|value| {
            value
                .as_str()
                .and_then(cadence_common::duration::parse_nanos)
        })
        .unwrap_or(default)
}

pub fn integer(values: &[DynamicConfigValue], domain: Option<&str>, default: i64) -> i64 {
    selected(values, domain)
        .and_then(|value| value.as_f64())
        .filter(|value| value.is_finite() && value.trunc() == *value)
        .map(|value| value as i64)
        .unwrap_or(default)
}

pub fn boolean(values: &[DynamicConfigValue], domain: Option<&str>, default: bool) -> bool {
    selected(values, domain).and_then(|value| value.as_bool()).unwrap_or(default)
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

/// Go `MapAllIsolationGroupsResponse`: one non-string rejects the whole list.
pub fn string_list(values: &[DynamicConfigValue]) -> Vec<String> {
    selected(values, None)
        .and_then(|value| {
            value.as_array().map(|items| {
                items
                    .iter()
                    .map(|item| item.as_str().map(str::to_owned))
                    .collect::<Option<Vec<_>>>()
            })
        })
        .flatten()
        .unwrap_or_default()
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
