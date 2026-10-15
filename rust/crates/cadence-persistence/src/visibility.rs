//! Visibility query evaluation for the in-memory store. The query syntax and
//! builtin field values follow Go's Elasticsearch visibility adapter. Durable
//! backend execution, aggregations, and text analyzer behavior remain separate
//! ports; unsupported expressions fail instead of silently dropping filters.
use cadence_api::{errors::BadRequestError, service_types::WorkflowExecutionInfo};
use serde_json::Value;
use sqlparser::{
    ast::{BinaryOperator as Op, Expr, SetExpr, Statement, UnaryOperator, Value as SqlValue},
    dialect::MySqlDialect,
    parser::Parser,
};
use std::{cmp::Ordering, collections::HashSet};

type Result<T> = std::result::Result<T, BadRequestError>;
fn bad(message: impl Into<String>) -> BadRequestError {
    BadRequestError::new(message)
}
fn name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(v) => Some(v.value.clone()),
        Expr::CompoundIdentifier(v) => Some(
            v.iter()
                .map(|i| i.value.clone())
                .collect::<Vec<_>>()
                .join("."),
        ),
        _ => None,
    }
}
fn time_name(key: &str) -> bool {
    matches!(
        key,
        "StartTime"
            | "CloseTime"
            | "ExecutionTime"
            | "UpdateTime"
            | "ScheduledExecutionTime"
            | "CustomDatetimeField"
    )
}
fn time_value(ms: Option<i64>) -> Value {
    ms.map(|n| Value::from(n.saturating_mul(1_000_000)))
        .unwrap_or(Value::Null)
}
fn field(row: &WorkflowExecutionInfo, key: &str, known: &HashSet<String>) -> Result<Value> {
    let key = key.strip_prefix("Attr.").unwrap_or(key);
    if key.eq_ignore_ascii_case("missing") {
        return Ok(Value::Null);
    }
    let value = match key {
        "missing" => Value::Null,
        "WorkflowID" => row
            .execution
            .as_ref()
            .map(|e| Value::from(e.workflow_id.clone()))
            .unwrap_or(Value::Null),
        "RunID" => row
            .execution
            .as_ref()
            .map(|e| Value::from(e.run_id.clone()))
            .unwrap_or(Value::Null),
        "WorkflowType" => row
            .workflow_type
            .as_ref()
            .map(|e| Value::from(e.name.clone()))
            .unwrap_or(Value::Null),
        "TaskList" => row
            .task_list
            .as_ref()
            .map(|e| Value::from(e.name.clone()))
            .unwrap_or(Value::Null),
        "CloseStatus" => row
            .close_status
            .map(|s| Value::from(s as i32))
            .unwrap_or(Value::Null),
        "ExecutionStatus" => row
            .execution_status
            .map(|s| Value::from(s as i32))
            .unwrap_or(Value::Null),
        "HistoryLength" => Value::from(row.history_length),
        "CronSchedule" => row
            .cron_schedule
            .as_ref()
            .map(|s| Value::from(s.clone()))
            .unwrap_or(Value::Null),
        "DomainID" | "NumClusters" | "ClusterAttributeScope" | "ClusterAttributeName" => {
            return Err(bad("This cluster visibility field has not been ported."))
        }
        "IsCron" => Value::from(row.is_cron),
        "StartTime" => time_value(row.start_time),
        "CloseTime" => time_value(row.close_time),
        "ExecutionTime" => time_value(row.execution_time),
        "UpdateTime" => time_value(row.update_time),
        "ScheduledExecutionTime" => time_value(row.scheduled_execution_time),
        "CustomStringField" | "CustomDomain" | "Operator" => {
            return Err(bad("Full text visibility queries have not been ported."))
        }
        _ if known.contains(key) => row
            .search_attributes
            .as_ref()
            .and_then(|s| s.indexed_fields.as_ref())
            .and_then(|f| f.get(key))
            .map(|bytes| {
                serde_json::from_slice(bytes)
                    .map_err(|_| bad(format!("Invalid search attribute payload for {key}.")))
            })
            .transpose()?
            .unwrap_or(Value::Null),
        _ => return Err(bad(format!("Invalid search attribute: {key}"))),
    };
    normalize(Some(key), value)
}
fn literal(v: &SqlValue) -> Result<Value> {
    match v {
        SqlValue::Number(n, _) => {
            serde_json::from_str(n).map_err(|_| bad("Invalid number in visibility query."))
        }
        SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
            Ok(Value::from(s.clone()))
        }
        SqlValue::Boolean(v) => Ok(Value::from(*v)),
        SqlValue::Null => Ok(Value::Null),
        _ => Err(bad("Unsupported visibility literal.")),
    }
}
fn normalize(key: Option<&str>, value: Value) -> Result<Value> {
    let Some(key) = key else { return Ok(value) };
    let key = key.strip_prefix("Attr.").unwrap_or(key);
    let Value::String(text) = &value else {
        return Ok(value);
    };
    if time_name(key) {
        if let Ok(v) = text.parse::<i64>() {
            return Ok(Value::from(v));
        }
        let date = chrono::DateTime::parse_from_rfc3339(text)
            .map_err(|e| bad(format!("Invalid visibility timestamp: {e}")))?;
        return date
            .timestamp_nanos_opt()
            .map(Value::from)
            .ok_or_else(|| bad("Visibility timestamp is outside the nanosecond range."));
    }
    if matches!(key, "CustomIntField" | "CustomDoubleField") {
        return serde_json::from_str(text)
            .map_err(|_| bad("Invalid numeric search attribute value."));
    }
    if key == "CustomBoolField" {
        return text
            .parse::<bool>()
            .map(Value::from)
            .map_err(|_| bad("Invalid boolean search attribute value."));
    }
    if key == "CloseStatus" {
        let status = cadence_api::enums::WorkflowExecutionCloseStatus::parse(text)
            .ok_or_else(|| bad("Invalid CloseStatus."))?;
        return Ok(Value::from(status as i32));
    }
    Ok(value)
}
fn compare(a: &Value, b: &Value) -> Result<Ordering> {
    match (a, b) {
        (Value::Null, Value::Null) => Ok(Ordering::Equal),
        (Value::Null, _) => Ok(Ordering::Greater),
        (_, Value::Null) => Ok(Ordering::Less),
        (Value::String(a), Value::String(b)) => Ok(a.cmp(b)),
        (Value::Bool(a), Value::Bool(b)) => Ok(a.cmp(b)),
        (Value::Number(a), Value::Number(b)) => {
            if let (Some(a), Some(b)) = (a.as_i64(), b.as_i64()) {
                return Ok(a.cmp(&b));
            }
            a.as_f64()
                .and_then(|a| b.as_f64().and_then(|b| a.partial_cmp(&b)))
                .ok_or_else(|| bad("Invalid numeric comparison."))
        }
        _ => Err(bad("Visibility comparison has incompatible value types.")),
    }
}
fn truth(v: Value) -> Result<bool> {
    v.as_bool()
        .ok_or_else(|| bad("Visibility predicate must return a boolean."))
}
fn wildcard(pattern: &str, value: &str) -> bool {
    let value: Vec<_> = value.to_lowercase().chars().collect();
    let pattern: Vec<_> = pattern
        .to_lowercase()
        .replace('%', "*")
        .replace('_', "?")
        .chars()
        .chain(std::iter::once('*'))
        .collect();
    let mut previous = vec![false; value.len() + 1];
    previous[0] = true;
    for c in pattern {
        let mut current = vec![false; value.len() + 1];
        if c == '*' {
            current[0] = previous[0];
        }
        for i in 1..=value.len() {
            current[i] = if c == '*' {
                previous[i] || current[i - 1]
            } else {
                previous[i - 1] && (c == '?' || c == value[i - 1])
            };
        }
        previous = current;
    }
    previous[value.len()]
}
fn evaluation(expr: &Expr, row: &WorkflowExecutionInfo, known: &HashSet<String>) -> Result<Value> {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            field(row, &name(expr).expect("identifier"), known)
        }
        Expr::Value(v) => literal(v),
        Expr::Nested(e) => evaluation(e, row, known),
        Expr::UnaryOp { op, expr } => match op {
            UnaryOperator::Not => Ok(Value::from(!truth(evaluation(expr, row, known)?)?)),
            UnaryOperator::Minus => evaluation(expr, row, known)?
                .as_i64()
                .and_then(i64::checked_neg)
                .map(Value::from)
                .ok_or_else(|| bad("Invalid negative numeric literal.")),
            _ => Err(bad("Unsupported visibility unary operator.")),
        },
        Expr::BinaryOp { left, op, right } => {
            let a = evaluation(left, row, known)?;
            let b = normalize(name(left).as_deref(), evaluation(right, row, known)?)?;
            let value = match op {
                Op::And => truth(a)? & truth(b)?,
                Op::Or => truth(a)? | truth(b)?,
                Op::Eq | Op::NotEq => {
                    let eq = if let Value::Array(values) = &a {
                        values.iter().any(|v| v == &b)
                    } else {
                        a == b
                    };
                    if *op == Op::Eq {
                        eq
                    } else {
                        !eq
                    }
                }
                Op::Gt | Op::GtEq | Op::Lt | Op::LtEq => {
                    if a.is_null() || b.is_null() {
                        false
                    } else {
                        let order = compare(&a, &b)?;
                        match op {
                            Op::Gt => order == Ordering::Greater,
                            Op::GtEq => order != Ordering::Less,
                            Op::Lt => order == Ordering::Less,
                            _ => order != Ordering::Greater,
                        }
                    }
                }
                _ => return Err(bad("Unsupported visibility binary operator.")),
            };
            Ok(Value::from(value))
        }
        Expr::IsNull(e) => Ok(Value::from(evaluation(e, row, known)?.is_null())),
        Expr::IsNotNull(e) => Ok(Value::from(!evaluation(e, row, known)?.is_null())),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let key = name(expr);
            let a = evaluation(expr, row, known)?;
            let values = list
                .iter()
                .map(|e| normalize(key.as_deref(), evaluation(e, row, known)?))
                .collect::<Result<Vec<_>>>()?;
            let matched = if let Value::Array(items) = a {
                items.iter().any(|v| values.contains(v))
            } else {
                values.contains(&a)
            };
            Ok(Value::from(matched ^ negated))
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let key = name(expr);
            let a = evaluation(expr, row, known)?;
            let low = normalize(key.as_deref(), evaluation(low, row, known)?)?;
            let high = normalize(key.as_deref(), evaluation(high, row, known)?)?;
            let matched = !a.is_null()
                && compare(&a, &low)? != Ordering::Less
                && compare(&a, &high)? != Ordering::Greater;
            Ok(Value::from(matched ^ negated))
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            escape_char,
            ..
        } => {
            if escape_char.is_some() {
                return Err(bad("LIKE ESCAPE has not been ported."));
            }
            let a = evaluation(expr, row, known)?;
            let b = evaluation(pattern, row, known)?;
            let pattern = b
                .as_str()
                .ok_or_else(|| bad("LIKE pattern must be a string."))?;
            let matched = if pattern.trim().is_empty() {
                true
            } else {
                a.as_str().is_some_and(|value| wildcard(pattern, value))
            };
            Ok(Value::from(matched ^ negated))
        }
        _ => Err(bad("This visibility expression has not been ported.")),
    }
}

pub fn apply(rows: &mut Vec<WorkflowExecutionInfo>, query: &str) -> Result<()> {
    let query = query.trim();
    let sql = if query.is_empty() {
        "SELECT * FROM executions".to_string()
    } else if query.to_ascii_uppercase().starts_with("ORDER BY ") {
        format!("SELECT * FROM executions {query}")
    } else {
        format!("SELECT * FROM executions WHERE {query}")
    };
    let statements = Parser::parse_sql(&MySqlDialect {}, &sql)
        .map_err(|e| bad(format!("Invalid visibility query: {e}")))?;
    if statements.len() != 1 {
        return Err(bad("Visibility query must contain one statement."));
    }
    let Statement::Query(q) = &statements[0] else {
        return Err(bad("Invalid visibility query."));
    };
    let SetExpr::Select(select) = q.body.as_ref() else {
        return Err(bad("Visibility set operations have not been ported."));
    };
    if q.limit.is_some()
        || q.offset.is_some()
        || q.fetch.is_some()
        || select.having.is_some()
        || !matches!(&select.group_by,sqlparser::ast::GroupByExpr::Expressions(items,_) if items.is_empty())
    {
        return Err(bad(
            "Visibility aggregation and explicit pagination clauses have not been ported.",
        ));
    }
    let mut known: HashSet<_> = cadence_api::search_attributes::default_indexed_keys()
        .into_keys()
        .collect();
    for row in rows.iter() {
        if let Some(attrs) = row
            .search_attributes
            .as_ref()
            .and_then(|s| s.indexed_fields.as_ref())
        {
            known.extend(attrs.keys().cloned());
        }
    }
    let sample = WorkflowExecutionInfo::default();
    if let Some(predicate) = &select.selection {
        // Validate even when the domain contains no workflows.
        truth(evaluation(predicate, &sample, &known)?)?;
        let mut kept = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            if truth(evaluation(predicate, row, &known)?)? {
                kept.push(row.clone());
            }
        }
        *rows = kept;
    }
    let orders = if let Some(order) = &q.order_by {
        order
            .exprs
            .iter()
            .map(|o| {
                if o.nulls_first.is_some() {
                    return Err(bad(
                        "Explicit visibility NULLS ordering has not been ported.",
                    ));
                }
                let key = name(&o.expr)
                    .ok_or_else(|| bad("Visibility ORDER BY requires field names."))?;
                field(&sample, &key, &known)?;
                Ok((key, o.asc.unwrap_or(true)))
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        vec![("StartTime".to_string(), false)]
    };
    let mut decorated = rows
        .iter()
        .cloned()
        .map(|row| {
            let keys = orders
                .iter()
                .map(|(key, _)| field(&row, key, &known))
                .collect::<Result<Vec<_>>>()?;
            Ok((row, keys))
        })
        .collect::<Result<Vec<_>>>()?;
    for column in 0..orders.len() {
        let first = decorated
            .iter()
            .map(|(_, keys)| &keys[column])
            .find(|v| !v.is_null());
        for (_, keys) in &decorated {
            if let Some(first) = first {
                compare(first, &keys[column])?;
            }
        }
    }
    decorated.sort_by(|(a, ak), (b, bk)| {
        for ((av, bv), (_, asc)) in ak.iter().zip(bk).zip(&orders) {
            let cmp = compare(av, bv).expect("validated visibility sort values");
            let cmp = if av.is_null() || bv.is_null() || *asc {
                cmp
            } else {
                cmp.reverse()
            };
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        a.execution
            .as_ref()
            .map(|e| &e.run_id)
            .cmp(&b.execution.as_ref().map(|e| &e.run_id))
    });
    *rows = decorated.into_iter().map(|(row, _)| row).collect();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::{
        enums::WorkflowExecutionCloseStatus as Close,
        types::{SearchAttributes, WorkflowExecution, WorkflowType},
    };
    fn records() -> Vec<WorkflowExecutionInfo> {
        [
            (
                "workflow-one",
                "TypeA",
                1_700_000_000_000,
                Some(Close::Completed),
            ),
            ("workflow-two", "TypeB", 1_700_000_001_000, None),
            (
                "another-three",
                "TypeB",
                1_700_000_002_000,
                Some(Close::Failed),
            ),
        ]
        .into_iter()
        .map(|(id, typ, time, close)| WorkflowExecutionInfo {
            execution: Some(WorkflowExecution::new(id, id)),
            workflow_type: Some(WorkflowType { name: typ.into() }),
            start_time: Some(time),
            close_time: close.map(|_| time + 500),
            close_status: close,
            search_attributes: Some(SearchAttributes {
                indexed_fields: Some(
                    [
                        (
                            "CustomKeywordField".into(),
                            serde_json::to_vec(&"keyword").unwrap(),
                        ),
                        ("CustomIntField".into(), serde_json::to_vec(&42).unwrap()),
                    ]
                    .into_iter()
                    .collect(),
                ),
            }),
            ..Default::default()
        })
        .collect()
    }
    fn ids(rows: &[WorkflowExecutionInfo]) -> Vec<&str> {
        rows.iter()
            .map(|r| r.execution.as_ref().unwrap().workflow_id.as_str())
            .collect()
    }
    #[test]
    fn boolean_status_and_missing_filters_match_cadence_queries() {
        let mut rows = records();
        apply(
            &mut rows,
            "(WorkflowID = \"workflow-one\" OR WorkflowType = \"TypeB\") AND CloseStatus IN (0, 1)",
        )
        .unwrap();
        assert_eq!(ids(&rows), vec!["another-three", "workflow-one"]);
        let mut rows = records();
        apply(&mut rows, "CloseTime = missing").unwrap();
        assert_eq!(ids(&rows), vec!["workflow-two"]);
        let mut rows = records();
        apply(&mut rows, "CloseStatus = 'COMPLETED'").unwrap();
        assert_eq!(ids(&rows), vec!["workflow-one"]);
    }
    #[test]
    fn time_literals_use_go_nanoseconds_and_sort_nulls_last() {
        let mut rows = records();
        apply(
            &mut rows,
            "StartTime > '2023-11-14T22:13:20Z' ORDER BY StartTime ASC",
        )
        .unwrap();
        assert_eq!(ids(&rows), vec!["workflow-two", "another-three"]);
        let mut numeric = records();
        apply(
            &mut numeric,
            "StartTime > 1700000000000000000 ORDER BY StartTime ASC",
        )
        .unwrap();
        assert_eq!(rows, numeric);
        let mut rows = records();
        apply(&mut rows, "ORDER BY CloseTime DESC").unwrap();
        assert_eq!(
            ids(&rows),
            vec!["another-three", "workflow-one", "workflow-two"]
        );
    }
    #[test]
    fn like_is_case_insensitive_prefix_matching_in_go_visibility() {
        let mut rows = records();
        apply(&mut rows,"WorkflowID LIKE 'WORKFLOW-' AND CustomKeywordField = 'keyword' AND CustomIntField BETWEEN 40 AND 50").unwrap();
        assert_eq!(ids(&rows), vec!["workflow-two", "workflow-one"]);
    }
    #[test]
    fn unsupported_or_invalid_queries_fail_on_empty_domains_too() {
        for q in [
            "UnknownField = 1",
            "StartTime > 'not-a-date'",
            "CloseStatus =",
            "1=1; SELECT * FROM other",
            "CloseStatus=0 GROUP BY WorkflowType",
            "CustomStringField = 'requires analyzer'",
        ] {
            assert!(apply(&mut Vec::new(), q).is_err(), "accepted {q}");
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Cursor {
    run_id: String,
    start_time: Option<i64>,
}
/// Resume after the last execution instead of a mutable array offset.
pub fn page(
    rows: Vec<WorkflowExecutionInfo>,
    size: i32,
    token: &[u8],
) -> Result<cadence_api::service_types::ListWorkflowExecutionsResponse> {
    let size = if size <= 0 { 1000 } else { size as usize };
    if size > 10000 {
        return Err(bad("Pagesize is larger than allow 10000"));
    }
    let offset = if token.is_empty() {
        0
    } else {
        let cursor: Cursor =
            serde_json::from_slice(token).map_err(|_| bad("Invalid NextPageToken."))?;
        rows.iter()
            .position(|e| {
                e.start_time == cursor.start_time
                    && e.execution
                        .as_ref()
                        .is_some_and(|e| e.run_id == cursor.run_id)
            })
            .map(|i| i + 1)
            .ok_or_else(|| bad("Invalid NextPageToken."))?
    };
    let end = offset.saturating_add(size).min(rows.len());
    let token = if end < rows.len() {
        let last = &rows[end - 1];
        let run_id = last
            .execution
            .as_ref()
            .ok_or_else(|| bad("Visibility execution is missing."))?
            .run_id
            .clone();
        serde_json::to_vec(&Cursor {
            run_id,
            start_time: last.start_time,
        })
        .map_err(|_| bad("Invalid NextPageToken."))?
    } else {
        Vec::new()
    };
    Ok(cadence_api::service_types::ListWorkflowExecutionsResponse {
        executions: rows.into_iter().skip(offset).take(end - offset).collect(),
        next_page_token: token,
    })
}

/// Cadence legacy list filters use Unix nanoseconds, including sub-millisecond
/// boundaries. Do not truncate client bounds before comparing stored times.
pub fn filter_start_time(
    rows: &mut Vec<WorkflowExecutionInfo>,
    filter: Option<&cadence_api::service_types::StartTimeFilter>,
) -> Result<()> {
    let filter = filter.ok_or_else(|| bad("StartTimeFilter is required"))?;
    let first = filter
        .earliest_time
        .ok_or_else(|| bad("EarliestTime in StartTimeFilter is required"))?;
    let last = filter
        .latest_time
        .ok_or_else(|| bad("LatestTime in StartTimeFilter is required"))?;
    if first > last {
        return Err(bad(
            "EarliestTime in StartTimeFilter should not be larger than LatestTime",
        ));
    }
    rows.retain(|e| {
        e.start_time.is_some_and(|t| {
            let t = t.saturating_mul(1_000_000);
            t >= first && t <= last
        })
    });
    Ok(())
}

#[cfg(test)]
mod page_tests {
    use super::*;
    use cadence_api::types::WorkflowExecution;
    #[test]
    fn cursor_does_not_repeat_a_page_when_a_new_execution_is_inserted() {
        let item = |id, time| WorkflowExecutionInfo {
            execution: Some(WorkflowExecution::new(id, id)),
            start_time: Some(time),
            ..Default::default()
        };
        let first = page(vec![item("bravo", 2), item("delta", 1)], 1, &[]).unwrap();
        let second = page(
            vec![item("alpha", 3), item("bravo", 2), item("delta", 1)],
            1,
            &first.next_page_token,
        )
        .unwrap();
        assert_eq!(
            second.executions[0].execution.as_ref().unwrap().run_id,
            "delta"
        );
        assert!(second.next_page_token.is_empty());
    }
    #[test]
    fn legacy_time_filter_keeps_sub_millisecond_boundaries() {
        let mut rows = vec![WorkflowExecutionInfo {
            start_time: Some(1000),
            ..Default::default()
        }];
        filter_start_time(
            &mut rows,
            Some(&cadence_api::service_types::StartTimeFilter {
                earliest_time: Some(1_000_000_001),
                latest_time: Some(2_000_000_000),
            }),
        )
        .unwrap();
        assert!(rows.is_empty());
    }
}
