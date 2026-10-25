//! Backend-neutral persistence boundary for durable Matching task queues.
//!
//! The synchronous methods mirror the existing SQLite queue operations and
//! their result contract. Implementations must preserve Cadence transaction,
//! paging, expiry and lease-fencing semantics at this boundary.

use crate::matching_blob::{TaskData, TaskListData};
use thiserror::Error;
use uuid::Uuid;

/// Driver-independent task-store errors. The SQLite-specific `Sql` and `Blob`
/// variants remain for source compatibility and preserve the old SQLite API;
/// new adapters normalize their driver failures into `Backend`.
#[derive(Debug, Error)]
pub enum MatchingTaskStoreError {
    #[error("unsupported SQLite task-store configuration: {0}")]
    UnsupportedConfiguration(String),
    #[error("incompatible Go SQLite Matching schema: {0}")]
    IncompatibleSchema(String),
    #[error("unsupported Matching task-store backend configuration: {0}")]
    BackendConfiguration(String),
    #[error(transparent)]
    MalformedSqlMatchingRunId(#[from] crate::matching_blob::SqlRunIdError),
    #[error("incompatible Matching task-store backend schema: {0}")]
    BackendSchema(String),
    #[error("{0}")]
    ConditionFailed(String),
    #[error("task list does not exist")]
    NotFound,
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Blob(#[from] cadence_thrift::Error),
    #[error("missing limit parameter")]
    MissingLimit,
    #[error("delete failed: {0} rows affected instead of 1")]
    DeleteCount(usize),
    #[error("task-store backend operation failed ({kind:?}): {message}")]
    Backend {
        kind: BackendErrorKind,
        message: String,
    },
}

/// Go's SQL adapter maps MySQL timeouts, throttling and other driver failures
/// to distinct service errors; all three are persistence-retryable there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendErrorKind {
    Timeout,
    Throttled,
    Internal,
    Permanent,
    /// Go `persistence.DBUnavailableError` (nosql `IsDBUnavailableError`).
    DbUnavailable,
    /// Go `types.BadRequestError`.
    BadRequest,
}

impl MatchingTaskStoreError {
    /// Matches Go persistence retry eligibility for SQLite today and explicit
    /// driver classification for network SQL adapters.
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::Sql(_)
                | Self::Backend {
                    kind: BackendErrorKind::Timeout
                        | BackendErrorKind::Throttled
                        | BackendErrorKind::Internal
                        | BackendErrorKind::DbUnavailable,
                    ..
                }
        )
    }
}

pub type StorageError = MatchingTaskStoreError;
pub type Result<T> = std::result::Result<T, MatchingTaskStoreError>;

/// Go `sqlplugin.GetDBShardIDFromDomainIDAndTasklist` (sqlplugin/dbSharding.go:45):
/// FarmHash32 of `<domainID>_<taskListName>` modulo the configured `nShards`.
/// Go hashes the canonical lowercase hyphenated UUID string.
pub fn go_db_shard_id(domain_id: &Uuid, task_list_name: &str, n_shards: usize) -> i32 {
    let input = format!("{domain_id}_{task_list_name}");
    (farmhash::hash32(input.as_bytes()) % n_shards as u32) as i32
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskListKey {
    pub domain_id: Uuid,
    pub name: String,
    /// Go persistence types: 0 decision, 1 activity (not protobuf enum values).
    pub task_type: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskListRecord {
    pub range_id: i64,
    pub data: TaskListData,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocatedTask {
    pub task_id: i64,
    pub data: TaskData,
}

/// Persistence operations consumed by `DurableTaskQueue`.
///
/// `create_tasks_with_domains` retains Go's cross-domain task row key behavior:
/// rows use each task's source domain while the lease fence applies to `key`.
pub trait MatchingTaskStore: Send + Sync {
    /// Go `ConfigStore.FetchConfig` on this store's datastore (dynamic-config
    /// client `configstore`). `Ok(None)` is Go's not-found.
    fn fetch_latest_config(
        &self,
        _row_type: i32,
    ) -> std::result::Result<Option<crate::config_store::ConfigStoreRow>, String> {
        Err("this task store has no config store".into())
    }

    fn get_task_list(&self, key: &TaskListKey) -> Result<TaskListRecord>;

    fn lease(
        &self,
        key: &TaskListKey,
        kind: i16,
        previous_range: i64,
        now_nanos: i64,
    ) -> Result<TaskListRecord>;

    fn update_task_list(
        &self,
        key: &TaskListKey,
        range: i64,
        data: crate::matching_blob::TaskListData,
        now_nanos: i64,
    ) -> Result<()>;

    fn create_tasks_with_domains(
        &self,
        key: &TaskListKey,
        range: i64,
        now_nanos: i64,
        tasks: &[(Uuid, &AllocatedTask)],
    ) -> Result<()>;

    /// Return tasks in the Go interval `(read_level, max_read_level]`, in
    /// ascending task ID order and bounded by `batch_size`. Expiry is handled
    /// by the queue reader, so implementations return expired rows too.
    fn get_tasks(
        &self,
        key: &TaskListKey,
        read_level: i64,
        max_read_level: i64,
        batch_size: i64,
    ) -> Result<Vec<AllocatedTask>>;

    fn task_list_size(&self, key: &TaskListKey, ack_level: i64) -> Result<i64>;
    fn complete_task(&self, key: &TaskListKey, task_id: i64) -> Result<()>;
    fn complete_tasks_less_than(
        &self,
        key: &TaskListKey,
        inclusive_task_id: i64,
        limit: i64,
    ) -> Result<usize>;
    fn delete_task_list(&self, key: &TaskListKey, range: i64) -> Result<()>;
}

/// Helpers that pin Rust SQL text to the Go source that defines it.
#[cfg(test)]
pub(crate) mod go_sql_pin {
    /// Drop whitespace and Go string-concatenation syntax so a statement can
    /// be located inside a Go source file regardless of formatting.
    pub fn strip(text: &str) -> String {
        text.chars()
            .filter(|c| !c.is_whitespace() && !matches!(c, '`' | '+' | '"'))
            .collect()
    }

    /// Replace `:name` markers with `?` (MySQL) or `$1..$n` (PostgreSQL).
    pub fn positional(template: &str, postgres: bool) -> String {
        let chars: Vec<char> = template.chars().collect();
        let (mut out, mut index, mut number) = (String::new(), 0, 0);
        while index < chars.len() {
            if chars[index] == ':' {
                index += 1;
                while index < chars.len() && (chars[index].is_ascii_lowercase() || chars[index] == '_') {
                    index += 1;
                }
                number += 1;
                if postgres {
                    out.push_str(&format!("${number}"));
                } else {
                    out.push('?');
                }
                continue;
            }
            out.push(chars[index]);
            index += 1;
        }
        out
    }

    /// `statement` must appear verbatim (modulo whitespace) in `go_source`.
    pub fn assert_in_go(go_source: &str, statement: &str) {
        assert!(
            strip(go_source).contains(&strip(statement)),
            "statement not found in Go source: {statement}"
        );
    }

    /// CQL keywords are case-insensitive; compare ignoring case.
    pub fn assert_in_go_ci(go_source: &str, statement: &str) {
        assert!(
            strip(go_source)
                .to_ascii_lowercase()
                .contains(&strip(statement).to_ascii_lowercase()),
            "statement not found in Go source: {statement}"
        );
    }

    /// Named-parameter Go statement `template`: it must exist in the Go source
    /// and `rust` must equal it with the placeholders made positional.
    pub fn assert_named(go_source: &str, template: &str, rust: &str, postgres: bool) {
        assert!(
            strip(go_source).contains(&strip(template)),
            "template not found in Go source: {template}"
        );
        assert_eq!(strip(rust), strip(&positional(template, postgres)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Values produced by dgryski/go-farm `Hash32` (go.mod v0.0.0-20240924180020)
    /// over `<domainID>_<taskListName>`, then `% nShards`, as in
    /// sqlplugin.GetDBShardIDFromDomainIDAndTasklist.
    #[test]
    fn db_shard_id_matches_go_farm_hash32_vectors() {
        let id = Uuid::parse_str("e1b8e2d3-5c1f-4a77-9b0a-6a0a1f0e5d11").unwrap();
        let long = "/__cadence_sys/partition/1/with/a/very/long/name/that/goes/past/sixty/four/bytes/ok";
        for (domain, name, hash) in [
            (id, "my-task-list", 1_266_640_152_u32),
            (id, long, 168_198_957),
            (Uuid::nil(), "x", 1_810_377_098),
            (Uuid::max(), "x", 3_090_509_108),
        ] {
            for shards in [1_usize, 2, 4, 16, 1000] {
                assert_eq!(
                    go_db_shard_id(&domain, name, shards),
                    (hash % shards as u32) as i32,
                    "{domain}_{name} mod {shards}"
                );
            }
        }
    }
}
