//! Port of Go `common/elasticsearch/validator.SearchAttributesValidator`
//! (StartWorkflow / SignalWithStartWorkflow search-attribute checks).

use std::collections::HashMap;

use cadence_api::enums::IndexedValueType;
use cadence_api::types::SearchAttributes;
use serde_json::Value;

/// Limits resolved per domain (`frontend.searchAttributes*Limit`).
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub number_of_keys: i64,
    pub size_of_value: i64,
    pub total_size: i64,
}

impl Default for Limits {
    fn default() -> Self {
        Self { number_of_keys: 100, size_of_value: 2048, total_size: 40 * 1024 }
    }
}

fn scalar_ok(v: &Value, t: IndexedValueType) -> bool {
    match t {
        IndexedValueType::String | IndexedValueType::Keyword => v.is_string(),
        IndexedValueType::Int => v.is_i64() || v.is_u64(),
        IndexedValueType::Double => v.is_number(),
        IndexedValueType::Bool => v.is_boolean(),
        IndexedValueType::Datetime => v
            .as_str()
            .is_some_and(|s| chrono::DateTime::parse_from_rfc3339(s).is_ok()),
    }
}

/// Go `DeserializeSearchAttributeValue`: a JSON scalar or list of the type.
pub fn value_is_valid(raw: &[u8], t: IndexedValueType) -> bool {
    match serde_json::from_slice::<Value>(raw) {
        Ok(Value::Array(a)) => a.iter().all(|x| scalar_ok(x, t)),
        Ok(v) => scalar_ok(&v, t),
        Err(_) => false,
    }
}

/// Returns the BadRequest message on failure.
pub fn validate(
    input: Option<&SearchAttributes>,
    limits: Limits,
    validate_attrs: bool,
    valid: &HashMap<String, IndexedValueType>,
) -> Result<(), String> {
    let Some(fields) = input.and_then(|s| s.indexed_fields.as_ref()) else {
        return Ok(());
    };
    if fields.len() as i64 > limits.number_of_keys {
        return Err(format!("number of keys {} exceed limit", fields.len()));
    }
    let mut total = 0usize;
    // Go iterates a map (random order); sort for deterministic errors.
    let mut keys: Vec<&String> = fields.keys().collect();
    keys.sort();
    for key in keys {
        let val = &fields[key];
        if validate_attrs {
            let Some(t) = valid.get(key) else {
                return Err(format!("{key} is not a valid search attribute key"));
            };
            if !value_is_valid(val, *t) {
                return Err(format!(
                    "{} is not a valid search attribute value for key {key}",
                    String::from_utf8_lossy(val)
                ));
            }
        }
        if cadence_api::search_attributes::is_system_indexed_key(key) {
            return Err(format!("{key} is read-only Cadence reserved attribute"));
        }
        if val.len() as i64 > limits.size_of_value {
            return Err(format!("size limit exceed for key {key}"));
        }
        total += key.len() + val.len();
    }
    if total as i64 > limits.total_size {
        return Err(format!("total size {total} exceed limit"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sa(k: &str, v: &str) -> SearchAttributes {
        SearchAttributes { indexed_fields: Some([(k.to_string(), v.as_bytes().to_vec())].into()) }
    }
    fn valid() -> HashMap<String, IndexedValueType> {
        [
            ("K".to_string(), IndexedValueType::Keyword),
            ("I".to_string(), IndexedValueType::Int),
            ("D".to_string(), IndexedValueType::Datetime),
        ]
        .into()
    }

    #[test]
    fn accepts_valid_and_lists() {
        let l = Limits::default();
        assert!(validate(Some(&sa("K", "\"x\"")), l, true, &valid()).is_ok());
        assert!(validate(Some(&sa("K", "[\"x\",\"y\"]")), l, true, &valid()).is_ok());
        assert!(validate(Some(&sa("I", "5")), l, true, &valid()).is_ok());
        assert!(validate(Some(&sa("D", "\"2021-01-01T00:00:00Z\"")), l, true, &valid()).is_ok());
        assert!(validate(None, l, true, &valid()).is_ok());
    }

    #[test]
    fn rejects_like_go() {
        let l = Limits::default();
        assert_eq!(
            validate(Some(&sa("Nope", "1")), l, true, &valid()),
            Err("Nope is not a valid search attribute key".into())
        );
        assert_eq!(
            validate(Some(&sa("I", "\"s\"")), l, true, &valid()),
            Err("\"s\" is not a valid search attribute value for key I".into())
        );
        let mut v = valid();
        v.insert("WorkflowID".into(), IndexedValueType::Keyword);
        assert_eq!(
            validate(Some(&sa("WorkflowID", "\"x\"")), l, true, &v),
            Err("WorkflowID is read-only Cadence reserved attribute".into())
        );
        let tiny = Limits { number_of_keys: 0, ..l };
        assert_eq!(
            validate(Some(&sa("K", "\"x\"")), tiny, true, &valid()),
            Err("number of keys 1 exceed limit".into())
        );
        let small = Limits { size_of_value: 2, ..l };
        assert_eq!(
            validate(Some(&sa("K", "\"xyz\"")), small, true, &valid()),
            Err("size limit exceed for key K".into())
        );
        let total = Limits { total_size: 3, ..l };
        assert_eq!(
            validate(Some(&sa("K", "\"x\"")), total, true, &valid()),
            Err("total size 4 exceed limit".into())
        );
    }

    #[test]
    fn disabled_validation_skips_key_checks() {
        assert!(validate(Some(&sa("Nope", "1")), Limits::default(), false, &valid()).is_ok());
    }
}
