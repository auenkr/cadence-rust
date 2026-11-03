//! Advanced-visibility read path (Go `esVisibilityStore`): List/Scan/Count and
//! ListOpen/ListClosed against Elasticsearch/OpenSearch.

use std::sync::Arc;

use base64::Engine;
use cadence_api::service_types as st;
use cadence_api::types as ty;
use cadence_persistence::visibility_reader::decode_memo;
use cadence_service_core::es_mapping::HttpEsClient;
use serde_json::{json, Value};

use crate::es_query::{self, PageToken, QueryError};

/// Transport seam (real impl: `HttpEsClient`; tests use a fake).
pub trait EsSearch: Send + Sync {
    fn call(&self, method: &str, path: &str, body: Option<String>) -> Result<Value, String>;
}

impl EsSearch for HttpEsClient {
    fn call(&self, method: &str, path: &str, body: Option<String>) -> Result<Value, String> {
        self.raw(method, path, body)
    }
}

pub struct EsVisibility {
    client: Arc<dyn EsSearch>,
    index: String,
    max_result_window: i64,
}

impl std::fmt::Debug for EsVisibility {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EsVisibility")
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}

pub type ListResult = (Vec<st::WorkflowExecutionInfo>, Vec<u8>);

struct Hits {
    total: i64,
    sources: Vec<(Value, Vec<Value>)>,
    scroll_id: String,
}

fn parse_hits(v: &Value) -> Hits {
    let total = match &v["hits"]["total"] {
        Value::Number(n) => n.as_i64().unwrap_or(0),
        o => o["value"].as_i64().unwrap_or(0),
    };
    let sources = v["hits"]["hits"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|h| {
                    (
                        h["_source"].clone(),
                        h["sort"].as_array().cloned().unwrap_or_default(),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    Hits {
        total,
        sources,
        scroll_id: v["_scroll_id"].as_str().unwrap_or("").to_string(),
    }
}

fn i64_of(v: &Value, k: &str) -> i64 {
    v.get(k)
        .and_then(|x| x.as_i64().or_else(|| x.as_f64().map(|f| f as i64)))
        .unwrap_or(0)
}

/// Go `convertSearchResultToVisibilityRecord` + `convertVisibilityWorkflowExecutionInfo`.
pub fn source_to_info(src: &Value) -> st::WorkflowExecutionInfo {
    let start = i64_of(src, "StartTime");
    let exec = i64_of(src, "ExecutionTime");
    let memo = src
        .get("Memo")
        .and_then(Value::as_str)
        .and_then(|s| base64::engine::general_purpose::STANDARD.decode(s).ok())
        .and_then(|b| {
            decode_memo(
                &b,
                src.get("Encoding").and_then(Value::as_str).unwrap_or(""),
            )
            .ok()
            .flatten()
        });
    let search_attributes = src
        .get("Attr")
        .and_then(Value::as_object)
        .filter(|m| !m.is_empty())
        .map(|m| ty::SearchAttributes {
            indexed_fields: Some(
                m.iter()
                    .map(|(k, v)| (k.clone(), serde_json::to_vec(v).unwrap_or_default()))
                    .collect(),
            ),
        });
    fn enum_of<T: serde::de::DeserializeOwned>(src: &Value, k: &str) -> Option<T> {
        serde_json::from_value(Value::from(i64_of(src, k))).ok()
    }
    let mut info = st::WorkflowExecutionInfo {
        execution: Some(ty::WorkflowExecution {
            workflow_id: src["WorkflowID"].as_str().unwrap_or("").to_string(),
            run_id: src["RunID"].as_str().unwrap_or("").to_string(),
        }),
        workflow_type: Some(ty::WorkflowType {
            name: src["WorkflowType"].as_str().unwrap_or("").to_string(),
        }),
        start_time: Some(start),
        execution_time: Some(if exec == 0 { start } else { exec }),
        memo,
        search_attributes,
        task_list: Some(ty::TaskList {
            name: src["TaskList"].as_str().unwrap_or("").to_string(),
            ..Default::default()
        }),
        is_cron: src["IsCron"].as_bool().unwrap_or(false),
        cron_schedule: Some(src["CronSchedule"].as_str().unwrap_or("").to_string()),
        update_time: Some(i64_of(src, "UpdateTime")),
        execution_status: enum_of(src, "ExecutionStatus"),
        scheduled_execution_time: Some(i64_of(src, "ScheduledExecutionTime")),
        ..Default::default()
    };
    let close = i64_of(src, "CloseTime");
    if close != 0 {
        info.close_time = Some(close);
        info.close_status = enum_of(src, "CloseStatus");
        info.history_length = i64_of(src, "HistoryLength");
    }
    info
}

fn internal(prefix: &str, e: impl std::fmt::Display) -> QueryError {
    QueryError::Internal(format!("{prefix}{e}"))
}

impl EsVisibility {
    pub fn new(client: Arc<dyn EsSearch>, index: String, max_result_window: i64) -> Self {
        Self {
            client,
            index,
            max_result_window,
        }
    }

    fn page_size(n: i64) -> i64 {
        if n <= 0 {
            1000
        } else {
            n
        }
    }

    /// Go `GetNextPageToken` after a search.
    fn next_token(&self, hits: &Hits, page_size: i64, token: &PageToken) -> Vec<u8> {
        let n = hits.sources.len() as i64;
        if n == 0 || n != page_size {
            return Vec::new();
        }
        if hits.total <= self.max_result_window - page_size {
            return PageToken {
                from: token.from + n,
                ..Default::default()
            }
            .encode();
        }
        let sort = &hits.sources[hits.sources.len() - 1].1;
        PageToken {
            sort_value: sort.first().cloned().unwrap_or(Value::Null),
            tie_breaker: sort
                .get(1)
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            ..Default::default()
        }
        .encode()
    }

    fn search(&self, body: String) -> Result<Value, String> {
        self.client
            .call("POST", &format!("/{}/_search", self.index), Some(body))
    }

    pub fn list(
        &self,
        domain_id: &str,
        query: &str,
        page_size: i64,
        next_page_token: &[u8],
        attr_types: &dyn Fn(&str) -> Option<i32>,
    ) -> Result<ListResult, QueryError> {
        let page_size = Self::page_size(page_size);
        let token = PageToken::decode(next_page_token)?;
        let body = es_query::dsl_for_list(query, domain_id, page_size, &token, attr_types)?;
        let resp = self
            .search(body)
            .map_err(|e| internal("ListWorkflowExecutions failed, ", e))?;
        let hits = parse_hits(&resp);
        let infos = hits
            .sources
            .iter()
            .map(|(s, _)| source_to_info(s))
            .collect();
        Ok((infos, self.next_token(&hits, page_size, &token)))
    }

    pub fn scan(
        &self,
        domain_id: &str,
        query: &str,
        page_size: i64,
        next_page_token: &[u8],
    ) -> Result<ListResult, QueryError> {
        let page_size = Self::page_size(page_size);
        let token = PageToken::decode(next_page_token)?;
        let resp = if token.scroll_id.is_empty() {
            let body = es_query::dsl_for_scan(query, domain_id, page_size)?;
            self.client.call(
                "POST",
                &format!("/{}/_search?scroll=5m", self.index),
                Some(body),
            )
        } else {
            self.client.call(
                "POST",
                "/_search/scroll",
                Some(json!({"scroll": "5m", "scroll_id": token.scroll_id}).to_string()),
            )
        }
        .map_err(|e| internal("ScanWorkflowExecutions failed, ", e))?;
        let hits = parse_hits(&resp);
        let scroll_id = if hits.scroll_id.is_empty() {
            token.scroll_id.clone()
        } else {
            hits.scroll_id.clone()
        };
        if hits.sources.is_empty() {
            // io.EOF: last page, release the scroll context.
            if !scroll_id.is_empty() {
                let _ = self.client.call(
                    "DELETE",
                    "/_search/scroll",
                    Some(json!({"scroll_id": [scroll_id]}).to_string()),
                );
            }
            return Ok((vec![], vec![]));
        }
        let infos: Vec<_> = hits
            .sources
            .iter()
            .map(|(s, _)| source_to_info(s))
            .collect();
        let next = if hits.sources.len() as i64 == page_size {
            PageToken {
                scroll_id,
                ..Default::default()
            }
            .encode()
        } else {
            let _ = self.client.call(
                "DELETE",
                "/_search/scroll",
                Some(json!({"scroll_id": [scroll_id]}).to_string()),
            );
            Vec::new()
        };
        Ok((infos, next))
    }

    pub fn count(&self, domain_id: &str, query: &str) -> Result<i64, QueryError> {
        let body = es_query::dsl_for_count(query, domain_id)?;
        let resp = self
            .client
            .call("POST", &format!("/{}/_count", self.index), Some(body))
            .map_err(|e| internal("CountWorkflowExecutions failed. Error: ", e))?;
        Ok(resp["count"].as_i64().unwrap_or(0))
    }

    /// `ListOpen/ListClosedWorkflowExecutions(+ByType/ByStatus)`; times are Unix nanos.
    #[allow(clippy::too_many_arguments)]
    pub fn list_open_closed(
        &self,
        op_name: &str,
        domain_id: &str,
        is_open: bool,
        earliest: i64,
        latest: i64,
        page_size: i64,
        next_page_token: &[u8],
        match_query: Option<(String, Value)>,
    ) -> Result<ListResult, QueryError> {
        let page_size = Self::page_size(page_size);
        let token = PageToken::decode(next_page_token)?;
        let body = es_query::open_closed_body(
            domain_id,
            is_open,
            earliest,
            latest,
            page_size,
            &token,
            match_query,
        );
        let resp = self
            .search(body)
            .map_err(|e| internal(&format!("{op_name} failed, "), e))?;
        let hits = parse_hits(&resp);
        let field = if is_open { "StartTime" } else { "CloseTime" };
        let infos = hits
            .sources
            .iter()
            .filter(|(s, _)| {
                let t = i64_of(s, field);
                earliest <= t && t <= latest
            })
            .map(|(s, _)| source_to_info(s))
            .collect();
        Ok((infos, self.next_token(&hits, page_size, &token)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    type Call = (String, String, Option<String>);

    struct Fake {
        calls: Mutex<Vec<Call>>,
        responses: Mutex<Vec<Result<Value, String>>>,
    }
    impl EsSearch for Fake {
        fn call(&self, m: &str, p: &str, b: Option<String>) -> Result<Value, String> {
            self.calls.lock().unwrap().push((m.into(), p.into(), b));
            self.responses.lock().unwrap().remove(0)
        }
    }
    fn fake(r: Vec<Result<Value, String>>) -> (Arc<Fake>, EsVisibility) {
        let f = Arc::new(Fake {
            calls: Mutex::new(vec![]),
            responses: Mutex::new(r),
        });
        (f.clone(), EsVisibility::new(f, "idx".into(), 10000))
    }
    fn hit(id: &str, close: i64) -> Value {
        json!({"_source": {"WorkflowID": id, "RunID": "r", "WorkflowType": "t", "StartTime": 100,
            "CloseTime": close, "CloseStatus": 0, "HistoryLength": 5, "TaskList": "tl",
            "Attr": {"CustomKeywordField": "v"}}, "sort": [100, "r"]})
    }

    #[test]
    fn list_converts_and_pages_by_from() {
        let (f, v) = fake(vec![Ok(
            json!({"hits": {"total": 5, "hits": [hit("a", 0), hit("b", 200)]}}),
        )]);
        let (infos, tok) = v.list("d", "", 2, b"", &|_| None).unwrap();
        assert_eq!(infos.len(), 2);
        assert_eq!(infos[0].close_time, None);
        assert_eq!(infos[1].close_time, Some(200));
        assert_eq!(infos[1].history_length, 5);
        assert_eq!(infos[0].task_list.as_ref().unwrap().name, "tl");
        assert!(infos[0].search_attributes.is_some());
        assert_eq!(PageToken::decode(&tok).unwrap().from, 2);
        let c = f.calls.lock().unwrap();
        assert_eq!(c[0].1, "/idx/_search");
    }

    #[test]
    fn list_switches_to_search_after_over_window() {
        let (_, v) = fake(vec![Ok(
            json!({"hits": {"total": {"value": 20000}, "hits": [hit("a", 0)]}}),
        )]);
        let (_, tok) = v.list("d", "", 1, b"", &|_| None).unwrap();
        let t = PageToken::decode(&tok).unwrap();
        assert_eq!(t.tie_breaker, "r");
        assert_eq!(t.sort_value, json!(100));
    }

    #[test]
    fn list_error_and_bad_token() {
        let (_, v) = fake(vec![Err("boom".into())]);
        assert_eq!(
            v.list("d", "", 1, b"", &|_| None),
            Err(QueryError::Internal(
                "ListWorkflowExecutions failed, boom".into()
            ))
        );
        let (_, v) = fake(vec![]);
        assert!(matches!(
            v.list("d", "", 1, b"{x", &|_| None),
            Err(QueryError::BadRequest(_))
        ));
    }

    #[test]
    fn count_returns_value() {
        let (f, v) = fake(vec![Ok(json!({"count": 7}))]);
        assert_eq!(v.count("d", "WorkflowID = 'x'").unwrap(), 7);
        assert_eq!(f.calls.lock().unwrap()[0].1, "/idx/_count");
    }

    #[test]
    fn scan_scrolls_until_eof() {
        let (f, v) = fake(vec![
            Ok(
                json!({"_scroll_id": "S1", "hits": {"total": 3, "hits": [hit("a", 0), hit("b", 0)]}}),
            ),
            Ok(json!({"_scroll_id": "S1", "hits": {"total": 3, "hits": []}})),
            Ok(Value::Null),
        ]);
        let (infos, tok) = v.scan("d", "", 2, b"").unwrap();
        assert_eq!(infos.len(), 2);
        let t = PageToken::decode(&tok).unwrap();
        assert_eq!(t.scroll_id, "S1");
        let (infos, tok) = v.scan("d", "", 2, &tok).unwrap();
        assert!(infos.is_empty() && tok.is_empty());
        let c = f.calls.lock().unwrap();
        assert!(c[0].1.ends_with("_search?scroll=5m"));
        assert_eq!(c[1].1, "/_search/scroll");
        assert_eq!(c[2].0, "DELETE");
    }

    #[test]
    fn open_closed_filters_by_range() {
        let (_, v) = fake(vec![Ok(
            json!({"hits": {"total": 2, "hits": [hit("a", 150), hit("b", 900)]}}),
        )]);
        let (infos, tok) = v
            .list_open_closed(
                "ListClosedWorkflowExecutions",
                "d",
                false,
                100,
                200,
                10,
                b"",
                None,
            )
            .unwrap();
        assert_eq!(infos.len(), 1);
        assert!(tok.is_empty());
    }
}
