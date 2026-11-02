//! Read-only port of Go's `historyV2ManagerImpl.ReadHistoryBranch` and the SQL
//! `sqlHistoryStore.ReadHistoryBranch`. Frontend reads workflow history
//! directly from persistence (`history_node`), as Go does.
#![allow(clippy::too_many_arguments)]
use cadence_api::service_types::HistoryEvent;
use cadence_api::types::DataBlob;
use serde::{Deserialize, Serialize};

const FIRST_EVENT_ID: i64 = 1;
const NOT_STARTED_INDEX: i64 = -1;
const EMPTY_VERSION: i64 = -24;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoryReadError {
    EntityNotExists(String),
    DataInconsistency(String),
    InvalidRequest(String),
    Internal(String),
}

impl std::fmt::Display for HistoryReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EntityNotExists(m)
            | Self::DataInconsistency(m)
            | Self::InvalidRequest(m)
            | Self::Internal(m) => f.write_str(m),
        }
    }
}
impl std::error::Error for HistoryReadError {}

type Result<T> = std::result::Result<T, HistoryReadError>;

/// One `history_node` row.
#[derive(Debug, Clone)]
pub struct HistoryNodeRow {
    pub node_id: i64,
    pub txn_id: i64,
    pub data: Vec<u8>,
    pub encoding: String,
}

/// Row source for one physical store (Go's `SelectFromHistoryNode`).
pub trait HistoryNodeStore: Send + Sync + std::fmt::Debug {
    /// Rows with `min_node <= node_id < max_node`, ordered by
    /// `(node_id ASC, txn_id ASC)`, at most `limit` rows.
    /// SQL defaults to a node cursor; Cassandra overrides this with driver paging.
    fn select_page(
        &self,
        shard_id: i32,
        tree_id: [u8; 16],
        branch_id: [u8; 16],
        min_node: i64,
        max_node: i64,
        limit: i64,
        token: Option<&[u8]>,
    ) -> Result<(Vec<HistoryNodeRow>, Option<Vec<u8>>)> {
        let min_node = if let Some(token) = token.filter(|t| !t.is_empty()) {
            let bytes: [u8; 8] = token.try_into().map_err(|_| {
                HistoryReadError::Internal(format!("invalid next page token {token:?}"))
            })?;
            i64::from_le_bytes(bytes)
                .checked_add(1)
                .ok_or_else(|| HistoryReadError::Internal("history node cursor overflow".into()))?
        } else {
            min_node
        };
        let rows = self.select_nodes(shard_id, tree_id, branch_id, min_node, max_node, limit)?;
        let next = rows
            .last()
            .filter(|_| rows.len() as i64 >= limit)
            .map(|row| row.node_id.to_le_bytes().to_vec());
        Ok((rows, next))
    }

    fn select_nodes(
        &self,
        shard_id: i32,
        tree_id: [u8; 16],
        branch_id: [u8; 16],
        min_node: i64,
        max_node: i64,
        limit: i64,
    ) -> Result<Vec<HistoryNodeRow>>;
}

#[derive(Debug, Clone)]
pub struct ReadHistoryBranchRequest {
    pub branch_token: Vec<u8>,
    pub min_event_id: i64,
    /// Exclusive.
    pub max_event_id: i64,
    pub page_size: i64,
    pub next_page_token: Vec<u8>,
    pub shard_id: i32,
}

#[derive(Debug, Clone, Default)]
pub struct ReadHistoryBranchResponse {
    pub batches: Vec<Vec<HistoryEvent>>,
    pub next_page_token: Vec<u8>,
    pub size: i64,
    pub last_first_event_id: i64,
}

impl ReadHistoryBranchResponse {
    pub fn events(&self) -> Vec<HistoryEvent> {
        self.batches.iter().flatten().cloned().collect()
    }
}

/// Go `historyV2PagingToken` (JSON, Go field names, `[]byte` as base64).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct PagingToken {
    last_event_version: i64,
    #[serde(rename = "LastEventID")]
    last_event_id: i64,
    #[serde(
        default,
        serialize_with = "cadence_api::b64::serialize_opt_bytes",
        deserialize_with = "cadence_api::b64::deserialize_opt_bytes"
    )]
    store_token: Option<Vec<u8>>,
    current_range_index: i64,
    final_range_index: i64,
    #[serde(rename = "LastNodeID")]
    last_node_id: i64,
    #[serde(rename = "LastTransactionID")]
    last_transaction_id: i64,
}

fn parse_uuid(value: &str) -> Result<[u8; 16]> {
    uuid::Uuid::parse_str(value)
        .map(|u| *u.as_bytes())
        .map_err(|e| HistoryReadError::Internal(format!("invalid history uuid {value:?}: {e}")))
}

fn decode_batch(row: &HistoryNodeRowData) -> Result<Vec<HistoryEvent>> {
    let encoding = match row.encoding.to_ascii_lowercase().as_str() {
        "thriftrw" => cadence_api::enums::EncodingType::ThriftRW,
        other => {
            return Err(HistoryReadError::Internal(format!(
                "unsupported history encoding {other:?}"
            )))
        }
    };
    cadence_thrift::history_blob::decode(&DataBlob {
        encoding_type: Some(encoding),
        data: row.data.clone(),
    })
    .map_err(|e| HistoryReadError::Internal(e.to_string()))
}

struct HistoryNodeRowData {
    data: Vec<u8>,
    encoding: String,
}

fn corrupted(message: &str) -> HistoryReadError {
    HistoryReadError::DataInconsistency(message.to_owned())
}

/// SQL store paging (Go `sqlHistoryStore.ReadHistoryBranch`); returns the
/// chosen blobs and `(next store token, last node id, last txn id)`.
#[allow(clippy::type_complexity)]
fn store_read(
    store: &dyn HistoryNodeStore,
    shard_id: i32,
    tree: [u8; 16],
    branch: [u8; 16],
    min_node: i64,
    max_node: i64,
    page_size: i64,
    store_token: &Option<Vec<u8>>,
    mut last_node_id: i64,
    mut last_txn_id: i64,
) -> Result<(Vec<HistoryNodeRowData>, Option<Vec<u8>>, i64, i64)> {
    let (rows, next) = store.select_page(
        shard_id,
        tree,
        branch,
        min_node,
        max_node,
        page_size,
        store_token.as_deref(),
    )?;
    if rows.is_empty() {
        return Ok((Vec::new(), next, last_node_id, last_txn_id));
    }
    let mut history = Vec::new();
    for row in &rows {
        if row.txn_id < last_txn_id {
            if row.node_id < last_node_id {
                return Err(corrupted("corrupted data, nodeID cannot decrease"));
            } else if row.node_id > last_node_id {
                last_node_id = row.node_id;
            }
            continue;
        }
        if row.node_id < last_node_id {
            return Err(corrupted("corrupted data, nodeID cannot decrease"));
        } else if row.node_id == last_node_id {
            return Err(corrupted(
                "corrupted data, same nodeID must have smaller txnID",
            ));
        }
        last_txn_id = row.txn_id;
        last_node_id = row.node_id;
        history.push(HistoryNodeRowData {
            data: row.data.clone(),
            encoding: row.encoding.clone(),
        });
    }
    Ok((history, next, last_node_id, last_txn_id))
}

/// Go `ReadHistoryBranch`: page through the branch's ancestor ranges.
pub fn read_history_branch(
    store: &dyn HistoryNodeStore,
    request: &ReadHistoryBranchRequest,
) -> Result<ReadHistoryBranchResponse> {
    if request.page_size <= 0 || request.min_event_id >= request.max_event_id {
        return Err(HistoryReadError::InvalidRequest(format!(
            "no events can be found for pageSize {}, minEventID {}, maxEventID: {}",
            request.page_size, request.min_event_id, request.max_event_id
        )));
    }
    let default_last_event_id = request.min_event_id - 1;
    let mut token: PagingToken = if request.next_page_token.is_empty() {
        PagingToken {
            last_event_id: default_last_event_id,
            last_event_version: EMPTY_VERSION,
            current_range_index: NOT_STARTED_INDEX,
            final_range_index: 0,
            store_token: None,
            last_node_id: FIRST_EVENT_ID - 1,
            last_transaction_id: 0,
        }
    } else {
        serde_json::from_slice(&request.next_page_token)
            .map_err(|e| HistoryReadError::InvalidRequest(e.to_string()))?
    };
    let branch = crate::history_branch::decode(&request.branch_token)
        .map_err(|e| HistoryReadError::InvalidRequest(e.message))?;
    let mut ranges = branch.ancestors.clone();
    let begin = branch
        .ancestors
        .last()
        .map(|r| r.end_node_id)
        .unwrap_or(FIRST_EVENT_ID);
    ranges.push(cadence_api::types::HistoryBranchRange {
        branch_id: branch.branch_id.clone(),
        begin_node_id: begin,
        end_node_id: request.max_event_id,
    });
    if token.current_range_index == NOT_STARTED_INDEX {
        for (idx, range) in ranges.iter().enumerate() {
            if request.min_event_id >= range.end_node_id {
                continue;
            }
            if request.max_event_id <= range.begin_node_id {
                break;
            }
            if token.current_range_index == NOT_STARTED_INDEX {
                token.current_range_index = idx as i64;
            }
            token.final_range_index = idx as i64;
        }
        if token.current_range_index == NOT_STARTED_INDEX {
            return Err(HistoryReadError::DataInconsistency(
                "branchRange is corrupted".into(),
            ));
        }
    }
    let current = ranges
        .get(token.current_range_index as usize)
        .ok_or_else(|| HistoryReadError::DataInconsistency("branchRange is corrupted".into()))?;
    let max_node = current.end_node_id.min(request.max_event_id);
    let (blobs, next_store, last_node_id, last_txn_id) = store_read(
        store,
        request.shard_id,
        parse_uuid(&branch.tree_id)?,
        parse_uuid(&current.branch_id)?,
        request.min_event_id,
        max_node,
        request.page_size,
        &token.store_token,
        token.last_node_id,
        token.last_transaction_id,
    )?;
    if blobs.is_empty() && request.next_page_token.is_empty() {
        return Err(HistoryReadError::EntityNotExists(
            "Workflow execution history not found.".into(),
        ));
    }
    let size: i64 = blobs.iter().map(|b| b.data.len() as i64).sum();
    token.store_token = next_store;
    token.last_node_id = last_node_id;
    token.last_transaction_id = last_txn_id;

    let mut batches = Vec::new();
    let mut last_first_event_id = -23i64;
    for blob in &blobs {
        let events = decode_batch(blob)?;
        let (Some(first), Some(last)) = (events.first(), events.last()) else {
            return Err(corrupted("corrupted history event batch, empty events"));
        };
        if first.version != last.version
            || first.event_id + events.len() as i64 - 1 != last.event_id
        {
            return Err(corrupted(
                "corrupted history event batch, wrong version and IDs",
            ));
        }
        if first.version < token.last_event_version {
            continue;
        }
        if first.event_id <= token.last_event_id {
            continue;
        }
        if first.event_id != token.last_event_id + 1
            && (default_last_event_id == 0 || token.last_event_id != default_last_event_id)
        {
            return Err(HistoryReadError::DataInconsistency(
                "Corrupted history".into(),
            ));
        }
        token.last_event_version = first.version;
        token.last_event_id = last.event_id;
        last_first_event_id = first.event_id;
        batches.push(events);
    }
    // Go `serializeToken`.
    let next_page_token = if token.store_token.as_ref().is_none_or(|t| t.is_empty()) {
        if token.current_range_index == token.final_range_index {
            Vec::new()
        } else {
            token.current_range_index += 1;
            serde_json::to_vec(&token).map_err(|e| HistoryReadError::Internal(e.to_string()))?
        }
    } else {
        serde_json::to_vec(&token).map_err(|e| HistoryReadError::Internal(e.to_string()))?
    };
    Ok(ReadHistoryBranchResponse {
        batches,
        next_page_token,
        size,
        last_first_event_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cadence_api::service_types::HistoryEvent;

    #[derive(Debug)]
    struct Fixed(Vec<HistoryNodeRow>);
    impl HistoryNodeStore for Fixed {
        fn select_nodes(
            &self,
            _: i32,
            _: [u8; 16],
            _: [u8; 16],
            min: i64,
            max: i64,
            limit: i64,
        ) -> Result<Vec<HistoryNodeRow>> {
            Ok(self
                .0
                .iter()
                .filter(|r| r.node_id >= min && r.node_id < max)
                .take(limit as usize)
                .cloned()
                .collect())
        }
    }

    fn row(first: i64, count: i64, txn: i64) -> HistoryNodeRow {
        let events = (first..first + count)
            .map(|id| HistoryEvent {
                event_id: id,
                version: 0,
                ..Default::default()
            })
            .collect();
        let blob = cadence_thrift::history_blob::encode(events);
        HistoryNodeRow {
            node_id: first,
            txn_id: txn,
            data: blob.data,
            encoding: "thriftrw".into(),
        }
    }

    fn request(token: Vec<u8>) -> ReadHistoryBranchRequest {
        let tree = uuid::Uuid::new_v4().to_string();
        let branch = uuid::Uuid::new_v4().to_string();
        ReadHistoryBranchRequest {
            branch_token: crate::history_branch::new_token(&tree, &branch),
            min_event_id: 1,
            max_event_id: 10,
            page_size: 10,
            next_page_token: token,
            shard_id: 0,
        }
    }

    #[derive(Debug)]
    struct CassandraPages;

    impl HistoryNodeStore for CassandraPages {
        fn select_nodes(&self, _: i32, _: [u8; 16], _: [u8; 16], _: i64, _: i64, _: i64) -> Result<Vec<HistoryNodeRow>> {
            panic!("native pages must not use the SQL node cursor")
        }

        fn select_page(&self, _: i32, _: [u8; 16], _: [u8; 16], min: i64, _: i64, _: i64, token: Option<&[u8]>) -> Result<(Vec<HistoryNodeRow>, Option<Vec<u8>>)> {
            assert_eq!(min, 1);
            match token {
                None => Ok((vec![row(1, 2, 10)], Some(vec![255, 7, 0]))),
                Some([255, 7, 0]) => Ok((vec![row(1, 2, 9), row(3, 2, 11)], None)),
                other => panic!("unexpected opaque token {other:?}"),
            }
        }
    }

    #[test]
    fn native_paging_resumes_within_node_and_skips_older_transaction() {
        let mut req = request(vec![]);
        req.page_size = 1;
        let first = read_history_branch(&CassandraPages, &req).unwrap();
        assert_eq!(first.events().len(), 2);
        assert!(!first.next_page_token.is_empty());
        req.next_page_token = first.next_page_token;
        let second = read_history_branch(&CassandraPages, &req).unwrap();
        assert_eq!(second.events().iter().map(|e| e.event_id).collect::<Vec<_>>(), vec![3, 4]);
        assert!(second.next_page_token.is_empty());
    }

    #[test]
    fn reads_all_batches_and_skips_stale_txn() {
        let store = Fixed(vec![row(1, 2, 1), row(3, 3, 2), row(3, 3, 3), row(6, 1, 4)]);
        let err = read_history_branch(&store, &request(vec![])).unwrap_err();
        // same node twice with increasing txn: Go treats the second as corrupt
        // only when equal node ids both pass the txn filter.
        assert!(matches!(err, HistoryReadError::DataInconsistency(_)));
        let store = Fixed(vec![row(1, 2, 1), row(3, 3, 2), row(6, 1, 4)]);
        let response = read_history_branch(&store, &request(vec![])).unwrap();
        assert_eq!(response.events().len(), 6);
        assert!(response.next_page_token.is_empty());
        assert_eq!(response.last_first_event_id, 6);
    }

    #[test]
    fn empty_history_is_not_found() {
        let err = read_history_branch(&Fixed(vec![]), &request(vec![])).unwrap_err();
        assert!(matches!(err, HistoryReadError::EntityNotExists(_)));
    }

    #[test]
    fn pages_by_row_count() {
        let store = Fixed(vec![row(1, 2, 1), row(3, 3, 2), row(6, 1, 4)]);
        let mut req = request(vec![]);
        req.page_size = 2;
        let first = read_history_branch(&store, &req).unwrap();
        assert_eq!(first.events().len(), 5);
        assert!(!first.next_page_token.is_empty());
        req.next_page_token = first.next_page_token;
        let second = read_history_branch(&store, &req).unwrap();
        assert_eq!(second.events().len(), 1);
        assert!(second.next_page_token.is_empty());
    }
}
