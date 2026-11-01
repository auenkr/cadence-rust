//! Read path for `history_node` rows, ported from Go's
//! `common/persistence/history_manager.go` (`readRawHistoryBranch`,
//! `readHistoryBranch`) and `sql_history_store.go` (`ReadHistoryBranch`).
//!
//! Frontend reads workflow history directly from persistence; History only
//! writes it. `HistoryNodeStore` is the SQL row seam (MySQL/PostgreSQL); the
//! branch range walk, paging token and batch validation are shared.

use crate::history_branch;
use cadence_api::service_types::HistoryEvent;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const FIRST_EVENT_ID: i64 = 1;
const EMPTY_VERSION: i64 = -24;
const NOT_STARTED_INDEX: i64 = -1;
const DEFAULT_LAST_NODE_ID: i64 = FIRST_EVENT_ID - 1;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HistoryReadError {
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Inconsistent(String),
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Internal(String),
}

/// One `history_node` row. `txn_id` is already sign-corrected (rows store
/// `txn_id * -1` so that the newest transaction sorts first).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryNodeRow {
    pub node_id: i64,
    pub txn_id: i64,
    pub data: Vec<u8>,
    pub data_encoding: String,
}

/// Row source: Go `SelectFromHistoryNode`.
pub trait HistoryNodeStore: Send + Sync + std::fmt::Debug {
    #[allow(clippy::too_many_arguments)]
    fn select_nodes(
        &self,
        shard_id: i32,
        tree_id: &[u8],
        branch_id: &[u8],
        min_node_id: i64,
        max_node_id: i64,
        page_size: i64,
    ) -> Result<Vec<HistoryNodeRow>, HistoryReadError>;
}

/// Go `historyV2PagingToken` (JSON, Go field names).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct HistoryPagingToken {
    #[serde(rename = "LastEventVersion")]
    pub last_event_version: i64,
    #[serde(rename = "LastEventID")]
    pub last_event_id: i64,
    #[serde(
        rename = "StoreToken",
        serialize_with = "cadence_api::b64::serialize_opt_bytes",
        deserialize_with = "cadence_api::b64::deserialize_opt_bytes"
    )]
    pub store_token: Option<Vec<u8>>,
    #[serde(rename = "CurrentRangeIndex")]
    pub current_range_index: i64,
    #[serde(rename = "FinalRangeIndex")]
    pub final_range_index: i64,
    #[serde(rename = "LastNodeID")]
    pub last_node_id: i64,
    #[serde(rename = "LastTransactionID")]
    pub last_transaction_id: i64,
}

fn deserialize_token(
    data: &[u8],
    default_last_event_id: i64,
) -> Result<HistoryPagingToken, HistoryReadError> {
    if data.is_empty() {
        return Ok(HistoryPagingToken {
            last_event_id: default_last_event_id,
            last_event_version: EMPTY_VERSION,
            current_range_index: NOT_STARTED_INDEX,
            last_node_id: DEFAULT_LAST_NODE_ID,
            last_transaction_id: 0,
            ..Default::default()
        });
    }
    serde_json::from_slice(data).map_err(|e| HistoryReadError::Invalid(e.to_string()))
}

/// Go `serializeToken`.
fn serialize_token(token: &mut HistoryPagingToken) -> Result<Option<Vec<u8>>, HistoryReadError> {
    if token.store_token.as_ref().map_or(true, Vec::is_empty) {
        if token.current_range_index == token.final_range_index {
            return Ok(None);
        }
        token.current_range_index += 1;
    }
    serde_json::to_vec(token)
        .map(Some)
        .map_err(|e| HistoryReadError::Internal(e.to_string()))
}

/// Request of Go `ReadHistoryBranchRequest`.
#[derive(Debug, Clone)]
pub struct ReadHistoryBranchRequest {
    pub branch_token: Vec<u8>,
    pub min_event_id: i64,
    pub max_event_id: i64,
    pub page_size: i64,
    pub next_page_token: Vec<u8>,
    pub shard_id: i32,
}

#[derive(Debug, Clone, Default)]
pub struct ReadHistoryBranchResponse {
    pub batches: Vec<Vec<HistoryEvent>>,
    pub next_page_token: Option<Vec<u8>>,
    pub size: usize,
    pub last_first_event_id: i64,
}

impl ReadHistoryBranchResponse {
    pub fn events(&self) -> Vec<HistoryEvent> {
        self.batches.iter().flatten().cloned().collect()
    }
}

fn uuid_bytes(value: &str) -> Result<Vec<u8>, HistoryReadError> {
    uuid::Uuid::parse_str(value)
        .map(|u| u.as_bytes().to_vec())
        .map_err(|e| HistoryReadError::Invalid(format!("invalid branch uuid {value}: {e}")))
}

fn little_endian_token(offset: i64) -> Vec<u8> {
    offset.to_le_bytes().to_vec()
}

struct StorePage {
    blobs: Vec<(Vec<u8>, String)>,
    next_token: Option<Vec<u8>>,
    last_node_id: i64,
    last_txn_id: i64,
}

/// Go `sqlHistoryStore.ReadHistoryBranch`.
#[allow(clippy::too_many_arguments)]
fn store_read(
    store: &dyn HistoryNodeStore,
    shard_id: i32,
    tree: &[u8],
    branch: &[u8],
    mut min_node_id: i64,
    max_node_id: i64,
    page_size: i64,
    store_token: Option<&[u8]>,
    mut last_node_id: i64,
    mut last_txn_id: i64,
) -> Result<StorePage, HistoryReadError> {
    if let Some(token) = store_token.filter(|t| !t.is_empty()) {
        let bytes: [u8; 8] = token.try_into().map_err(|_| {
            HistoryReadError::Internal(format!("invalid next page token {token:?}"))
        })?;
        min_node_id = i64::from_le_bytes(bytes) + 1;
    }
    let rows = store.select_nodes(shard_id, tree, branch, min_node_id, max_node_id, page_size)?;
    if rows.is_empty() {
        // Go returns an empty response, resetting the node/txn cursor.
        return Ok(StorePage {
            blobs: Vec::new(),
            next_token: None,
            last_node_id: 0,
            last_txn_id: 0,
        });
    }
    let mut blobs = Vec::new();
    for row in &rows {
        if row.txn_id < last_txn_id {
            if row.node_id < last_node_id {
                return Err(HistoryReadError::Inconsistent(
                    "corrupted data, nodeID cannot decrease".into(),
                ));
            } else if row.node_id > last_node_id {
                last_node_id = row.node_id;
            }
            continue;
        }
        if row.node_id < last_node_id {
            return Err(HistoryReadError::Inconsistent(
                "corrupted data, nodeID cannot decrease".into(),
            ));
        }
        if row.node_id == last_node_id {
            return Err(HistoryReadError::Inconsistent(
                "corrupted data, same nodeID must have smaller txnID".into(),
            ));
        }
        last_txn_id = row.txn_id;
        last_node_id = row.node_id;
        blobs.push((row.data.clone(), row.data_encoding.clone()));
    }
    let next_token = if rows.len() as i64 >= page_size {
        Some(little_endian_token(last_node_id))
    } else {
        None
    };
    Ok(StorePage {
        blobs,
        next_token,
        last_node_id,
        last_txn_id,
    })
}

/// Decode one stored batch (event timestamps become internal milliseconds).
pub fn decode_batch(data: &[u8], encoding: &str) -> Result<Vec<HistoryEvent>, HistoryReadError> {
    if !encoding.eq_ignore_ascii_case("thriftrw") {
        return Err(HistoryReadError::Internal(format!(
            "unsupported history encoding {encoding}"
        )));
    }
    cadence_thrift::history_blob::decode(&cadence_api::types::DataBlob {
        encoding_type: Some(cadence_api::enums::EncodingType::ThriftRW),
        data: data.to_vec(),
    })
    .map_err(|e| HistoryReadError::Internal(e.to_string()))
}

#[derive(Clone)]
pub struct HistoryReader {
    store: Arc<dyn HistoryNodeStore>,
}

impl std::fmt::Debug for HistoryReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HistoryReader").finish_non_exhaustive()
    }
}

impl HistoryReader {
    pub fn new(store: Arc<dyn HistoryNodeStore>) -> Self {
        Self { store }
    }

    /// Go `historyV2ManagerImpl.readHistoryBranch` (events, not raw blobs).
    pub fn read_history_branch(
        &self,
        request: &ReadHistoryBranchRequest,
    ) -> Result<ReadHistoryBranchResponse, HistoryReadError> {
        if request.page_size <= 0 || request.min_event_id >= request.max_event_id {
            return Err(HistoryReadError::Invalid(format!(
                "no events can be found for pageSize {}, minEventID {}, maxEventID: {}",
                request.page_size, request.min_event_id, request.max_event_id
            )));
        }
        let default_last_event_id = request.min_event_id - 1;
        let mut token = deserialize_token(&request.next_page_token, default_last_event_id)?;
        let branch = history_branch::decode(&request.branch_token)
            .map_err(|e| HistoryReadError::Invalid(e.message))?;
        let mut ranges: Vec<(String, i64, i64)> = branch
            .ancestors
            .iter()
            .map(|a| (a.branch_id.clone(), a.begin_node_id, a.end_node_id))
            .collect();
        let begin = branch.ancestors.last().map_or(FIRST_EVENT_ID, |a| a.end_node_id);
        ranges.push((branch.branch_id.clone(), begin, request.max_event_id));

        if token.current_range_index == NOT_STARTED_INDEX {
            for (idx, (_, begin, end)) in ranges.iter().enumerate() {
                if request.min_event_id >= *end {
                    continue;
                }
                if request.max_event_id <= *begin {
                    break;
                }
                if token.current_range_index == NOT_STARTED_INDEX {
                    token.current_range_index = idx as i64;
                }
                token.final_range_index = idx as i64;
            }
            if token.current_range_index == NOT_STARTED_INDEX {
                return Err(HistoryReadError::Inconsistent("branchRange is corrupted".into()));
            }
        }
        let (range_branch, _, range_end) = ranges
            .get(token.current_range_index as usize)
            .cloned()
            .ok_or_else(|| HistoryReadError::Invalid("invalid history page token".into()))?;
        let max_node_id = range_end.min(request.max_event_id);
        let page = store_read(
            self.store.as_ref(),
            request.shard_id,
            &uuid_bytes(&branch.tree_id)?,
            &uuid_bytes(&range_branch)?,
            request.min_event_id,
            max_node_id,
            request.page_size,
            token.store_token.as_deref(),
            token.last_node_id,
            token.last_transaction_id,
        )?;
        if page.blobs.is_empty() && request.next_page_token.is_empty() {
            return Err(HistoryReadError::NotFound(
                "Workflow execution history not found.".into(),
            ));
        }
        let size: usize = page.blobs.iter().map(|(d, _)| d.len()).sum();
        token.store_token = page.next_token;
        token.last_node_id = page.last_node_id;
        token.last_transaction_id = page.last_txn_id;

        let mut batches = Vec::new();
        let mut last_first_event_id = -23;
        for (data, encoding) in &page.blobs {
            let events = decode_batch(data, encoding)?;
            let (Some(first), Some(last)) = (events.first(), events.last()) else {
                return Err(HistoryReadError::Inconsistent(
                    "corrupted history event batch, empty events".into(),
                ));
            };
            if first.version != last.version
                || first.event_id + (events.len() as i64 - 1) != last.event_id
            {
                return Err(HistoryReadError::Inconsistent(
                    "corrupted history event batch, wrong version and IDs".into(),
                ));
            }
            if first.version < token.last_event_version || first.event_id <= token.last_event_id {
                continue;
            }
            if first.event_id != token.last_event_id + 1
                && (default_last_event_id == 0 || token.last_event_id != default_last_event_id)
            {
                return Err(HistoryReadError::Inconsistent(
                    "corrupted history event batch, eventID is not continuous".into(),
                ));
            }
            token.last_event_version = first.version;
            token.last_event_id = last.event_id;
            last_first_event_id = first.event_id;
            batches.push(events);
        }
        let next_page_token = serialize_token(&mut token)?;
        Ok(ReadHistoryBranchResponse {
            batches,
            next_page_token,
            size,
            last_first_event_id,
        })
    }
}

// ---------------------------------------------------------------------------
// SQL implementations
// ---------------------------------------------------------------------------

const MYSQL_SELECT: &str = "SELECT node_id, txn_id, data, data_encoding FROM history_node \
WHERE shard_id = ? AND tree_id = ? AND branch_id = ? AND node_id >= ? and node_id < ? \
ORDER BY shard_id, tree_id, branch_id, node_id, txn_id LIMIT ?";
const POSTGRES_SELECT: &str = "SELECT node_id, txn_id, data, data_encoding FROM history_node \
WHERE shard_id = $1 AND tree_id = $2 AND branch_id = $3 AND node_id >= $4 and node_id < $5 \
ORDER BY shard_id, tree_id, branch_id, node_id, txn_id LIMIT $6";

#[derive(Clone)]
pub struct MySqlHistoryNodeStore {
    pool: Arc<crate::matching_mysql::SyncConnectionPool>,
}

impl std::fmt::Debug for MySqlHistoryNodeStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MySqlHistoryNodeStore").finish_non_exhaustive()
    }
}

impl MySqlHistoryNodeStore {
    /// Share the connection pool of the Frontend's domain metadata store.
    pub fn from_domain_store(store: &crate::matching_domain_mysql::MySqlDomainMetadataStore) -> Self {
        Self {
            pool: store.history_pool(),
        }
    }
}

impl HistoryNodeStore for MySqlHistoryNodeStore {
    fn select_nodes(
        &self,
        shard_id: i32,
        tree_id: &[u8],
        branch_id: &[u8],
        min_node_id: i64,
        max_node_id: i64,
        page_size: i64,
    ) -> Result<Vec<HistoryNodeRow>, HistoryReadError> {
        use mysql_async::prelude::Queryable;
        let (tree, branch) = (tree_id.to_vec(), branch_id.to_vec());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        self.pool
            .query_until(deadline, move |connection| {
                Box::pin(async move {
                    let rows: Vec<(i64, i64, Vec<u8>, String)> = connection
                        .exec(
                            MYSQL_SELECT,
                            (shard_id, tree, branch, min_node_id, max_node_id, page_size),
                        )
                        .await?;
                    Ok(rows)
                })
            })
            .map(|rows| {
                rows.into_iter()
                    .map(|(node_id, txn_id, data, data_encoding)| HistoryNodeRow {
                        node_id,
                        txn_id: -txn_id,
                        data,
                        data_encoding,
                    })
                    .collect()
            })
            .map_err(|e| HistoryReadError::Internal(format!("history_node read failed: {e:?}")))
    }
}

#[derive(Clone)]
pub struct PostgresHistoryNodeStore {
    pool: Arc<crate::matching_postgres::PostgresConnectionPool>,
}

impl std::fmt::Debug for PostgresHistoryNodeStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresHistoryNodeStore").finish_non_exhaustive()
    }
}

impl PostgresHistoryNodeStore {
    /// Share the connection pool of the Frontend's domain metadata store.
    pub fn from_domain_store(
        store: &crate::matching_domain_postgres::PostgresDomainMetadataStore,
    ) -> Self {
        Self {
            pool: store.history_pool(),
        }
    }
}

impl HistoryNodeStore for PostgresHistoryNodeStore {
    fn select_nodes(
        &self,
        shard_id: i32,
        tree_id: &[u8],
        branch_id: &[u8],
        min_node_id: i64,
        max_node_id: i64,
        page_size: i64,
    ) -> Result<Vec<HistoryNodeRow>, HistoryReadError> {
        let mut client = self
            .pool
            .checkout_with_timeout(std::time::Duration::from_secs(10))
            .map_err(|e| HistoryReadError::Internal(e.to_string()))?;
        let rows = client
            .query(
                POSTGRES_SELECT,
                &[
                    &shard_id,
                    &tree_id.to_vec(),
                    &branch_id.to_vec(),
                    &min_node_id,
                    &max_node_id,
                    &page_size,
                ],
            )
            .map_err(|e| HistoryReadError::Internal(e.to_string()))?;
        let pg = |e: postgres::Error| HistoryReadError::Internal(e.to_string());
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            out.push(HistoryNodeRow {
                node_id: row.try_get(0).map_err(pg)?,
                txn_id: -row.try_get::<_, i64>(1).map_err(pg)?,
                data: row.try_get(2).map_err(pg)?,
                data_encoding: row.try_get(3).map_err(pg)?,
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Debug, Default)]
    struct FakeStore(Mutex<Vec<HistoryNodeRow>>);

    impl HistoryNodeStore for FakeStore {
        fn select_nodes(
            &self,
            _: i32,
            _: &[u8],
            _: &[u8],
            min: i64,
            max: i64,
            page: i64,
        ) -> Result<Vec<HistoryNodeRow>, HistoryReadError> {
            let mut rows: Vec<_> = self
                .0
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r.node_id >= min && r.node_id < max)
                .cloned()
                .collect();
            // Go orders by node_id then stored (negated) txn_id ascending.
            rows.sort_by_key(|r| (r.node_id, -r.txn_id));
            rows.truncate(page as usize);
            Ok(rows)
        }
    }

    fn row(node: i64, txn: i64) -> HistoryNodeRow {
        HistoryNodeRow {
            node_id: node,
            txn_id: txn,
            data: vec![node as u8],
            data_encoding: "thriftrw".into(),
        }
    }

    #[test]
    fn store_read_skips_stale_txn_and_pages() {
        let store = FakeStore(Mutex::new(vec![row(1, 5), row(1, 3), row(4, 6)]));
        let page = store_read(&store, 0, &[], &[], 1, 100, 3, None, 0, 0).unwrap();
        assert_eq!(page.blobs.len(), 2);
        assert_eq!(page.last_node_id, 4);
        assert_eq!(page.last_txn_id, 6);
        assert_eq!(page.next_token, Some(little_endian_token(4)));
    }

    #[test]
    fn token_roundtrip_matches_go_defaults() {
        let token = deserialize_token(&[], 0).unwrap();
        assert_eq!(token.current_range_index, -1);
        assert_eq!(token.last_event_version, -24);
        let mut finished = HistoryPagingToken {
            current_range_index: 2,
            final_range_index: 2,
            ..Default::default()
        };
        assert_eq!(serialize_token(&mut finished).unwrap(), None);
        let mut more = HistoryPagingToken {
            current_range_index: 0,
            final_range_index: 1,
            ..Default::default()
        };
        let bytes = serialize_token(&mut more).unwrap().unwrap();
        let back = deserialize_token(&bytes, 0).unwrap();
        assert_eq!(back.current_range_index, 1);
    }
}
