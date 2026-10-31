//! Document field rules from Go `indexer/indexer.go` (`isValidFieldToES` and
//! `fulfillDoc`).

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

/// Go `definition.DomainID`.
pub const DOMAIN_ID: &str = "DomainID";
/// Go `definition.WorkflowID`.
pub const WORKFLOW_ID: &str = "WorkflowID";
/// Go `definition.RunID`.
pub const RUN_ID: &str = "RunID";
/// Go `definition.KafkaKey`.
pub const KAFKA_KEY: &str = "KafkaKey";
/// Go `definition.Memo`.
pub const MEMO: &str = "Memo";
/// Go `definition.Encoding`.
pub const ENCODING: &str = "Encoding";
/// Go `es.VisibilityOperation`.
pub const VISIBILITY_OPERATION: &str = "VisibilityOperation";

/// Go `isValidFieldToES`. With query-attribute validation off, every field is
/// accepted. Otherwise a field must be a valid search attribute or one of the
/// reserved document fields.
pub fn is_valid_field_to_es(
    validation_enabled: bool,
    valid_search_attributes: &BTreeSet<String>,
    field: &str,
) -> bool {
    if !validation_enabled {
        return true;
    }
    if valid_search_attributes.contains(field) {
        return true;
    }
    matches!(
        field,
        MEMO | KAFKA_KEY | ENCODING | VISIBILITY_OPERATION
    )
}

/// Go `fulfillDoc`: writes the four identifying fields into the document,
/// replacing any value already there.
pub fn fulfill_doc(
    doc: &mut BTreeMap<String, Value>,
    domain_id: &str,
    workflow_id: &str,
    run_id: &str,
    kafka_key: &str,
) {
    doc.insert(DOMAIN_ID.to_owned(), Value::from(domain_id));
    doc.insert(WORKFLOW_ID.to_owned(), Value::from(workflow_id));
    doc.insert(RUN_ID.to_owned(), Value::from(run_id));
    doc.insert(KAFKA_KEY.to_owned(), Value::from(kafka_key));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attributes(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    #[test]
    fn validation_off_accepts_any_field() {
        assert!(is_valid_field_to_es(false, &attributes(&[]), "someField"));
    }

    #[test]
    fn validation_on_accepts_valid_attributes_and_reserved_fields_only() {
        let valid = attributes(&["someField"]);
        assert!(is_valid_field_to_es(true, &valid, "someField"));
        assert!(is_valid_field_to_es(true, &attributes(&[]), MEMO));
        assert!(is_valid_field_to_es(true, &attributes(&[]), KAFKA_KEY));
        assert!(is_valid_field_to_es(true, &attributes(&[]), ENCODING));
        assert!(is_valid_field_to_es(true, &attributes(&[]), VISIBILITY_OPERATION));
        assert!(!is_valid_field_to_es(true, &attributes(&[]), "stuff"));
    }

    #[test]
    fn fulfill_doc_writes_identifiers_and_kafka_key() {
        let mut doc = BTreeMap::new();
        doc.insert(DOMAIN_ID.to_owned(), Value::from("stale"));
        fulfill_doc(&mut doc, "domain1", "workflow1", "run1", "kafka-key-1");
        assert_eq!(doc[DOMAIN_ID], "domain1");
        assert_eq!(doc[WORKFLOW_ID], "workflow1");
        assert_eq!(doc[RUN_ID], "run1");
        assert_eq!(doc[KAFKA_KEY], "kafka-key-1");
        assert_eq!(doc.len(), 4);
    }
}
