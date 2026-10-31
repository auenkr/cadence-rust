//! Query templates from Go `esanalyzer` (`workflow.go` and
//! `domainWorkflowTypeCountWorkflow.go`).
//!
//! The templates are copied verbatim from Go, including its whitespace, so the
//! ES and Pinot text sent to the stores is unchanged. Domain IDs and table names
//! are substituted into the `{...}` placeholders. Go writes them into the text
//! without escaping, and this port does the same.
//!
//! The Go `ORDER BY count` in the domain workflow-type Pinot query has no
//! `DESC`, unlike the other Pinot queries. It is kept as written in Go.

/// Go `readMode`: which store serves the metric query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadMode {
    Pinot,
    Es,
}

impl ReadMode {
    /// Go `readMode` string values.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pinot => "pinot",
            Self::Es => "es",
        }
    }
}

/// Go getWorkflowVersionQuery: versions of open executions by workflow type (ES).
const WORKFLOW_VERSION_ES_QUERY: &str = r#"
{
    "aggs" : {
        "wftypes" : {
            "terms" : { "field" : "WorkflowType"},
            "aggs": {
                "versions": {
                    "terms" : { "field" : "Attr.CadenceChangeVersion"}
                }
            }
        }
    },
    "query": {
        "bool": {
            "must_not": {
                "exists": {
                    "field": "CloseTime"
                }
            },
            "must": [
                {
                    "match" : {
                        "DomainID" : "{DOMAIN_ID}"
                    }
                }
            ]
        }
    },
    "size": 0
}
    "#;

/// Go getWorkflowTypePinotQuery: top open workflow types (Pinot).
const WORKFLOW_TYPE_PINOT_QUERY: &str = r#"
SELECT WorkflowType, COUNT(*) AS count
FROM {TABLE}
WHERE DomainID = '{DOMAIN_ID}'
  AND CloseStatus = -1
  AND StartTime > 0
GROUP BY WorkflowType
ORDER BY count DESC
LIMIT 10
    "#;

/// Go getWorkflowVersionPinotQuery: versions of one open workflow type (Pinot).
const WORKFLOW_VERSION_PINOT_QUERY: &str = r#"
SELECT JSON_EXTRACT_SCALAR(Attr, '$.CadenceChangeVersion', 'STRING_ARRAY') AS CadenceChangeVersion, COUNT(*) AS count
FROM {TABLE}
WHERE DomainID = '{DOMAIN_ID}'
  AND CloseStatus = -1
  AND StartTime > 0
  AND WorkflowType = '{WORKFLOW_TYPE}'
GROUP BY JSON_EXTRACT_SCALAR(Attr, '$.CadenceChangeVersion', 'STRING_ARRAY')
ORDER BY count DESC
LIMIT 10
    "#;

/// Go getDomainWorkflowTypeCountQuery: open executions by workflow type (ES).
const DOMAIN_WORKFLOW_TYPE_COUNT_ES_QUERY: &str = r#"
{
    "aggs" : {
        "wftypes" : {
            "terms" : { "field" : "WorkflowType"}
        }
    },
    "query": {
        "bool": {
            "must_not": {
                "exists": {
                    "field": "CloseTime"
                }
            },
            "must": [
                {
                    "match" : {
                        "DomainID" : "{DOMAIN_ID}"
                    }
                },
				{
					"exists": {
						"field": "StartTime"
					}
				}
            ]
        }
    },
    "size": 0
}
    "#;

/// Go getDomainWorkflowTypeCountPinotQuery: open executions by workflow type (Pinot).
const DOMAIN_WORKFLOW_TYPE_COUNT_PINOT_QUERY: &str = r#"
SELECT WorkflowType, COUNT(*) AS count
FROM {TABLE}
WHERE DomainID = '{DOMAIN_ID}'
AND CloseStatus = -1
AND StartTime > 0
GROUP BY WorkflowType
ORDER BY count
LIMIT 10
    
===="#;

/// Go `getWorkflowVersionQuery`.
pub fn workflow_version_es_query(domain_id: &str) -> String {
    WORKFLOW_VERSION_ES_QUERY.replace("{DOMAIN_ID}", domain_id)
}

/// Go `getWorkflowTypePinotQuery`.
pub fn workflow_type_pinot_query(table: &str, domain_id: &str) -> String {
    WORKFLOW_TYPE_PINOT_QUERY
        .replace("{TABLE}", table)
        .replace("{DOMAIN_ID}", domain_id)
}

/// Go `getWorkflowVersionPinotQuery`.
pub fn workflow_version_pinot_query(table: &str, domain_id: &str, workflow_type: &str) -> String {
    WORKFLOW_VERSION_PINOT_QUERY
        .replace("{TABLE}", table)
        .replace("{DOMAIN_ID}", domain_id)
        .replace("{WORKFLOW_TYPE}", workflow_type)
}

/// Go `getDomainWorkflowTypeCountQuery`.
pub fn domain_workflow_type_count_es_query(domain_id: &str) -> String {
    DOMAIN_WORKFLOW_TYPE_COUNT_ES_QUERY.replace("{DOMAIN_ID}", domain_id)
}

/// Go `getDomainWorkflowTypeCountPinotQuery`.
pub fn domain_workflow_type_count_pinot_query(table: &str, domain_id: &str) -> String {
    DOMAIN_WORKFLOW_TYPE_COUNT_PINOT_QUERY
        .replace("{TABLE}", table)
        .replace("{DOMAIN_ID}", domain_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_mode_values_match_go() {
        assert_eq!(ReadMode::Pinot.as_str(), "pinot");
        assert_eq!(ReadMode::Es.as_str(), "es");
    }

    #[test]
    fn es_queries_bind_the_domain_id_and_leave_no_placeholders() {
        let version = workflow_version_es_query("dom-1");
        assert!(version.contains(r#""DomainID" : "dom-1""#));
        assert!(version.contains(r#""field" : "Attr.CadenceChangeVersion""#));
        assert!(version.contains(r#""size": 0"#));
        assert!(!version.contains("{DOMAIN_ID}"));

        let count = domain_workflow_type_count_es_query("dom-1");
        assert!(count.contains(r#""DomainID" : "dom-1""#));
        assert!(count.contains(r#""field": "StartTime""#));
        assert!(!count.contains("{DOMAIN_ID}"));
    }

    #[test]
    fn pinot_queries_bind_table_domain_and_type() {
        let types = workflow_type_pinot_query("tbl", "dom-1");
        assert!(types.contains("FROM tbl\nWHERE DomainID = 'dom-1'"));
        assert!(types.contains("LIMIT 10"));

        let versions = workflow_version_pinot_query("tbl", "dom-1", "MyWf");
        assert!(versions.contains("WorkflowType = 'MyWf'"));
        assert!(versions.contains("ORDER BY count DESC\nLIMIT 10"));
        assert!(!versions.contains("{WORKFLOW_TYPE}"));

        let counts = domain_workflow_type_count_pinot_query("tbl", "dom-1");
        assert!(counts.contains("ORDER BY count\nLIMIT 10"));
        assert!(!counts.contains("ORDER BY count DESC"));
    }
}
