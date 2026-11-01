//! Read-only port of Go's SQL visibility store
//! (`common/persistence/sql/sql_visibility_store.go`, `ListOpen/Closed*`,
//! `GetClosedWorkflowExecution`). Only the operations the Go SQL store
//! supports are present; query based List/Scan/Count are unsupported there too
//! (`ErrVisibilityOperationNotSupported`).
use cadence_api::types::Memo;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VisibilityError {
    EntityNotExists(String),
    BadRequest(String),
    Internal(String),
}

impl std::fmt::Display for VisibilityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EntityNotExists(m) | Self::BadRequest(m) | Self::Internal(m) => f.write_str(m),
        }
    }
}
impl std::error::Error for VisibilityError {}

/// One `executions_visibility` row; timestamps are microseconds since the
/// Unix epoch (the table's DATETIME(6)/TIMESTAMP resolution).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct VisibilityRow {
    pub workflow_id: String,
    pub run_id: String,
    pub start_time_us: i64,
    pub execution_time_us: i64,
    pub workflow_type_name: String,
    pub memo: Vec<u8>,
    pub encoding: String,
    pub is_cron: bool,
    pub update_time_us: i64,
    pub shard_id: i32,
    pub close_time_us: Option<i64>,
    pub close_status: Option<i32>,
    pub history_length: Option<i64>,
}

/// Go `sqlplugin.VisibilityFilter` for the list paths (always time bounded).
#[derive(Debug, Clone, Default)]
pub struct VisibilityFilter {
    pub domain_id: String,
    pub closed: bool,
    /// Pagination read level run id (`visibilityPageToken.RunID`).
    pub run_id: String,
    pub workflow_id: Option<String>,
    pub workflow_type_name: Option<String>,
    pub close_status: Option<i32>,
    pub min_start_time_us: i64,
    pub max_start_time_us: i64,
    pub page_size: i64,
}

/// Row source for one physical visibility store.
pub trait VisibilityStore: Send + Sync + std::fmt::Debug {
    fn select(&self, filter: &VisibilityFilter) -> Result<Vec<VisibilityRow>, VisibilityError>;
    /// `Ok(None)` is Go's `sql.ErrNoRows`.
    fn get_closed(
        &self,
        domain_id: &str,
        run_id: &str,
    ) -> Result<Option<VisibilityRow>, VisibilityError>;
}

/// Go `visibilityPageToken` (JSON `{"Time": RFC3339Nano, "RunID": ...}`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageToken {
    pub time_us: i64,
    pub run_id: String,
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Go `time.Time.MarshalJSON` (RFC3339Nano, UTC) for microsecond values.
pub fn format_rfc3339_us(us: i64) -> String {
    let secs = us.div_euclid(1_000_000);
    let frac = us.rem_euclid(1_000_000);
    let (y, m, d) = civil_from_days(secs.div_euclid(86_400));
    let rem = secs.rem_euclid(86_400);
    let mut out = format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    );
    if frac != 0 {
        let digits = format!("{frac:06}");
        out.push('.');
        out.push_str(digits.trim_end_matches('0'));
    }
    out.push('Z');
    out
}

/// Parses the `Z`-suffixed RFC3339 forms produced by Go for UTC times.
pub fn parse_rfc3339_us(text: &str) -> Option<i64> {
    let text = text.strip_suffix('Z')?;
    let (date, time) = text.split_once('T')?;
    let mut dp = date.splitn(3, '-');
    let y: i64 = dp.next()?.parse().ok()?;
    let m: i64 = dp.next()?.parse().ok()?;
    let d: i64 = dp.next()?.parse().ok()?;
    let (hms, frac) = match time.split_once('.') {
        Some((a, b)) => (a, b),
        None => (time, ""),
    };
    let mut tp = hms.splitn(3, ':');
    let hh: i64 = tp.next()?.parse().ok()?;
    let mm: i64 = tp.next()?.parse().ok()?;
    let ss: i64 = tp.next()?.parse().ok()?;
    if !frac.chars().all(|c| c.is_ascii_digit()) || frac.len() > 9 {
        return None;
    }
    let mut padded = format!("{frac:0<9}");
    padded.truncate(6);
    let micro: i64 = padded.parse().ok()?;
    Some((days_from_civil(y, m, d) * 86_400 + hh * 3600 + mm * 60 + ss) * 1_000_000 + micro)
}

pub fn serialize_page_token(token: &PageToken) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "Time": format_rfc3339_us(token.time_us),
        "RunID": token.run_id,
    }))
    .unwrap_or_default()
}

pub fn deserialize_page_token(data: &[u8]) -> Result<PageToken, VisibilityError> {
    let value: serde_json::Value =
        serde_json::from_slice(data).map_err(|e| VisibilityError::Internal(e.to_string()))?;
    let time = value
        .get("Time")
        .and_then(serde_json::Value::as_str)
        .and_then(parse_rfc3339_us)
        .ok_or_else(|| VisibilityError::Internal("invalid visibility page token".into()))?;
    let run_id = value
        .get("RunID")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    Ok(PageToken {
        time_us: time,
        run_id,
    })
}

/// Decodes the visibility memo blob (Go `DeserializeVisibilityMemo`).
pub fn decode_memo(data: &[u8], encoding: &str) -> Result<Option<Memo>, VisibilityError> {
    if data.is_empty() {
        return Ok(None);
    }
    let bad = |e: String| VisibilityError::Internal(format!("failed to decode memo: {e}"));
    match encoding.to_ascii_lowercase().as_str() {
        "thriftrw" | "" => {
            let mut reader = cadence_thrift::Reader::new(data);
            if reader.read_u8().map_err(|e| bad(e.to_string()))? != 0x59 {
                return Err(bad("invalid ThriftRW version".into()));
            }
            let s =
                cadence_thrift::value::read_struct(&mut reader).map_err(|e| bad(e.to_string()))?;
            let memo = <Memo as cadence_thrift::FromThrift>::from_thrift(&s)
                .map_err(|e| bad(e.to_string()))?;
            Ok(Some(memo))
        }
        "json" => {
            let mut value: serde_json::Value =
                serde_json::from_slice(data).map_err(|e| bad(e.to_string()))?;
            if let Some(fields) = value.get("Fields").cloned() {
                value = serde_json::json!({ "fields": fields });
            }
            let memo: Memo = serde_json::from_value(value).map_err(|e| bad(e.to_string()))?;
            Ok(Some(memo))
        }
        other => Err(bad(format!("unknown encoding {other}"))),
    }
}

/// Result of one list call.
#[derive(Debug, Clone, Default)]
pub struct ListPage {
    pub rows: Vec<VisibilityRow>,
    pub next_page_token: Vec<u8>,
}

/// Go `listWorkflowExecutions` (page token + next-token rule).
pub fn list_executions(
    store: &dyn VisibilityStore,
    mut filter: VisibilityFilter,
    page_token: &[u8],
    earliest_time_us: i64,
    latest_time_us: i64,
) -> Result<ListPage, VisibilityError> {
    let level = if page_token.is_empty() {
        PageToken {
            time_us: latest_time_us,
            run_id: String::new(),
        }
    } else {
        deserialize_page_token(page_token)?
    };
    filter.min_start_time_us = earliest_time_us;
    filter.max_start_time_us = level.time_us;
    filter.run_id = level.run_id;
    let rows = store.select(&filter)?;
    let Some(last) = rows.last() else {
        return Ok(ListPage::default());
    };
    let mut next = Vec::new();
    if last.start_time_us - earliest_time_us > 0 {
        next = serialize_page_token(&PageToken {
            time_us: last.start_time_us,
            run_id: last.run_id.clone(),
        });
    }
    Ok(ListPage {
        rows,
        next_page_token: next,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_round_trips_like_go() {
        let us = 1_700_000_000_123_456;
        let text = format_rfc3339_us(us);
        assert_eq!(text, "2023-11-14T22:13:20.123456Z");
        assert_eq!(parse_rfc3339_us(&text), Some(us));
        assert_eq!(
            format_rfc3339_us(1_700_000_000_000_000),
            "2023-11-14T22:13:20Z"
        );
        assert_eq!(
            parse_rfc3339_us("2023-11-14T22:13:20Z"),
            Some(1_700_000_000_000_000)
        );
    }

    #[test]
    fn page_token_round_trip() {
        let t = PageToken {
            time_us: 42_000_000,
            run_id: "r".into(),
        };
        assert_eq!(
            deserialize_page_token(&serialize_page_token(&t)).unwrap(),
            t
        );
        assert!(deserialize_page_token(b"nope").is_err());
    }

    #[derive(Debug)]
    struct Fake(Vec<VisibilityRow>);
    impl VisibilityStore for Fake {
        fn select(&self, _: &VisibilityFilter) -> Result<Vec<VisibilityRow>, VisibilityError> {
            Ok(self.0.clone())
        }
        fn get_closed(&self, _: &str, _: &str) -> Result<Option<VisibilityRow>, VisibilityError> {
            Ok(None)
        }
    }

    #[test]
    fn next_token_only_when_last_row_after_earliest() {
        let row = |t| VisibilityRow {
            run_id: "r".into(),
            start_time_us: t,
            ..Default::default()
        };
        let page = list_executions(
            &Fake(vec![row(10)]),
            VisibilityFilter::default(),
            &[],
            5,
            100,
        )
        .unwrap();
        assert!(!page.next_page_token.is_empty());
        let page = list_executions(
            &Fake(vec![row(5)]),
            VisibilityFilter::default(),
            &[],
            5,
            100,
        )
        .unwrap();
        assert!(page.next_page_token.is_empty());
        let page =
            list_executions(&Fake(vec![]), VisibilityFilter::default(), &[], 5, 100).unwrap();
        assert!(page.rows.is_empty());
    }
}

/// Bind value for a generated visibility query.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlParam {
    Text(String),
    Int(i64),
    Int32(i32),
    /// Microseconds since the epoch, bound as a datetime expression.
    TimeUs(i64),
}

/// SQL dialect specifics for the visibility templates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    MySql,
    Postgres,
}

impl SqlDialect {
    fn placeholder(self, n: usize) -> String {
        match self {
            Self::MySql => "?".into(),
            Self::Postgres => format!("${n}"),
        }
    }
    fn time_param(self, n: usize) -> String {
        match self {
            Self::MySql => "TIMESTAMPADD(MICROSECOND, ?, '1970-01-01 00:00:00')".into(),
            Self::Postgres => format!(
                "(TIMESTAMP '1970-01-01 00:00:00' + ${n}::bigint * INTERVAL '1 microsecond')"
            ),
        }
    }
    fn time_col(self, col: &str) -> String {
        match self {
            Self::MySql => format!("TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', {col})"),
            Self::Postgres => format!("(EXTRACT(EPOCH FROM {col})::numeric * 1000000)::bigint"),
        }
    }
    /// Column list; indexes 0..=9 open fields, 10..=12 closed fields.
    fn columns(self, closed: bool) -> String {
        let mut cols = format!(
            "workflow_id, run_id, {}, {}, workflow_type_name, memo, encoding, is_cron, {}, shard_id",
            self.time_col("start_time"),
            self.time_col("execution_time"),
            self.time_col("update_time"),
        );
        if closed {
            cols.push_str(&format!(
                ", {}, close_status, history_length",
                self.time_col("close_time")
            ));
        }
        cols
    }
}

/// True when the generated query reads closed rows (the status template is
/// only reachable without a workflow ID/type filter, as in Go's switch).
pub fn is_closed_query(filter: &VisibilityFilter) -> bool {
    filter.closed
        || (filter.workflow_id.is_none()
            && filter.workflow_type_name.is_none()
            && filter.close_status.is_some())
}

/// Go `SelectFromVisibility` templates for the list paths, in Go's case
/// order. Go passes `MinStartTime` (not `MaxStartTime`) as the
/// `start_time < ?` operand of the by-workflow-ID templates; that is kept.
pub fn build_list_query(filter: &VisibilityFilter, dialect: SqlDialect) -> (String, Vec<SqlParam>) {
    let closed = is_closed_query(filter);
    let mut params: Vec<SqlParam> = Vec::new();
    let mut sql = format!(
        "SELECT {} FROM executions_visibility WHERE close_status {} ",
        dialect.columns(closed),
        if closed { "IS NOT NULL" } else { "IS NULL" }
    );
    let push = |params: &mut Vec<SqlParam>, p: SqlParam| -> String {
        params.push(p);
        let n = params.len();
        match params.last() {
            Some(SqlParam::TimeUs(_)) => dialect.time_param(n),
            _ => dialect.placeholder(n),
        }
    };
    // Go orders the placeholder numbering by-value first for Postgres.
    let third_is_min;
    if let Some(id) = &filter.workflow_id {
        let ph = push(&mut params, SqlParam::Text(id.clone()));
        sql.push_str(&format!("AND workflow_id = {ph}"));
        third_is_min = true;
    } else if let Some(name) = &filter.workflow_type_name {
        let ph = push(&mut params, SqlParam::Text(name.clone()));
        sql.push_str(&format!("AND workflow_type_name = {ph}"));
        third_is_min = false;
    } else if let Some(status) = filter.close_status {
        let ph = push(&mut params, SqlParam::Int32(status));
        sql.push_str(&format!("AND close_status = {ph}"));
        third_is_min = false;
    } else {
        third_is_min = false;
    }
    let domain = push(&mut params, SqlParam::Text(filter.domain_id.clone()));
    let min = push(&mut params, SqlParam::TimeUs(filter.min_start_time_us));
    let max = push(&mut params, SqlParam::TimeUs(filter.max_start_time_us));
    let run = push(&mut params, SqlParam::Text(filter.run_id.clone()));
    let third = push(
        &mut params,
        SqlParam::TimeUs(if third_is_min {
            filter.min_start_time_us
        } else {
            filter.max_start_time_us
        }),
    );
    let limit = push(&mut params, SqlParam::Int(filter.page_size));
    sql.push_str(&format!(
        " AND domain_id = {domain} AND start_time >= {min} AND start_time <= {max} AND (run_id > {run} OR start_time < {third}) ORDER BY start_time DESC, run_id LIMIT {limit}"
    ));
    (sql, params)
}

/// Go `templateGetClosedWorkflowExecution` with the shared column order.
pub fn build_get_closed_query(
    domain_id: &str,
    run_id: &str,
    dialect: SqlDialect,
) -> (String, Vec<SqlParam>) {
    (
        format!(
            "SELECT {} FROM executions_visibility WHERE domain_id = {} AND close_status IS NOT NULL AND run_id = {}",
            dialect.columns(true),
            dialect.placeholder(1),
            dialect.placeholder(2),
        ),
        vec![SqlParam::Text(domain_id.into()), SqlParam::Text(run_id.into())],
    )
}

#[cfg(test)]
mod query_tests {
    use super::*;

    fn filter() -> VisibilityFilter {
        VisibilityFilter {
            domain_id: "d".into(),
            run_id: "".into(),
            min_start_time_us: 1,
            max_start_time_us: 9,
            page_size: 10,
            ..Default::default()
        }
    }

    #[test]
    fn open_query_uses_null_close_status_and_six_params() {
        let (sql, params) = build_list_query(&filter(), SqlDialect::MySql);
        assert!(sql.contains("close_status IS NULL"));
        assert_eq!(params.len(), 6);
        assert_eq!(params[2], SqlParam::TimeUs(9));
        assert_eq!(params[4], SqlParam::TimeUs(9));
    }

    #[test]
    fn by_id_uses_min_start_for_pagination_like_go() {
        let mut f = filter();
        f.workflow_id = Some("w".into());
        f.closed = true;
        let (sql, params) = build_list_query(&f, SqlDialect::Postgres);
        assert!(sql.contains("close_status IS NOT NULL"));
        assert!(sql.contains("workflow_id = $1"));
        assert_eq!(params.len(), 7);
        assert_eq!(params[5], SqlParam::TimeUs(1));
    }

    #[test]
    fn status_filter_selects_closed() {
        let mut f = filter();
        f.close_status = Some(3);
        let (sql, _) = build_list_query(&f, SqlDialect::MySql);
        assert!(sql.contains("close_status IS NOT NULL") && sql.contains("AND close_status = ?"));
    }
}
