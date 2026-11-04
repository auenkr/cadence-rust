//! Port of Go common/persistence/sql/sql_history_store.go.
use crate::{
    history_contract::*,
    history_sql,
    history_sql_backend::{internal, uuid, uuid_string, Column, SqlBackend, Value},
    history_sql_queries::Statement,
    history_tree_token::{self, HistoryTreeCursor},
};
use async_trait::async_trait;

#[derive(Clone)]
pub struct SqlHistoryV2Store {
    pub backend: SqlBackend,
    pub encoding: crate::sqlblobs::Encoding,
    pub delete_batch_size: i32,
}
impl SqlHistoryV2Store {
    pub fn new(backend: SqlBackend, encoding: crate::sqlblobs::Encoding) -> Self {
        Self {
            backend,
            encoding,
            delete_batch_size: 1000,
        }
    }
    fn tree_blob(
        &self,
        request: &HistoryBranchRequest,
    ) -> Result<(Vec<u8>, String), PersistenceError> {
        history_sql::encode_history_tree_info(
            &HistoryTreeInfo {
                created_time_unix_nanos: request.current_timestamp_unix_nanos,
                ancestors: request.ancestors.clone(),
                info: request.info.clone(),
            },
            self.encoding,
        )
        .map_err(internal)
    }
    fn tree_branch(
        shard_id: i32,
        tree_id: String,
        branch_id: &Value,
        data: &Value,
        encoding: &Value,
    ) -> Result<HistoryTreeBranch, PersistenceError> {
        let info = history_sql::decode_history_tree_info(&data.bytes()?, &encoding.text()?)
            .map_err(internal)?;
        Ok(HistoryTreeBranch {
            shard_id,
            tree_id,
            branch_id: uuid_string(branch_id)?,
            fork_time_unix_nanos: info.created_time_unix_nanos,
            info: info.info,
            ancestors: info.ancestors,
        })
    }
}
#[async_trait]
impl GoHistoryV2Store for SqlHistoryV2Store {
    async fn append_history_nodes(
        &self,
        request: HistoryBranchRequest,
    ) -> Result<(), PersistenceError> {
        if request.node_id < request.ancestors.last().map_or(1, |v| v.end_node_id) {
            return Err(PersistenceError::InvalidPersistenceRequest(
                "cannot append to ancestors' nodes".into(),
            ));
        }
        let tree_blob = if request.is_new_branch {
            Some(self.tree_blob(&request)?)
        } else {
            None
        };
        let tree = uuid(&request.tree_id)?;
        let branch = uuid(&request.branch_id)?;
        // Both Go SQL plugins negate txn_id to emulate descending clustering.
        let node = [
            Value::I32(request.shard_id),
            tree.clone(),
            branch.clone(),
            Value::I64(request.node_id),
            Value::I64(request.transaction_id.wrapping_neg()),
            Value::Bytes(request.data.data),
            Value::Text(request.data.encoding),
        ];
        let dialect = self.backend.dialect();
        self.backend.transaction(|tx| {
            let count = tx.execute(dialect.statement(Statement::InsertHistoryNode), &node)?;
            if tree_blob.is_some() && count != 1 {
                return Err(internal(format!(
                    "expected 1 row to be affected for node table, got {count}"
                )));
            }
            if let Some((data, encoding)) = tree_blob {
                let count = tx.execute(
                    dialect.statement(Statement::InsertHistoryTree),
                    &[
                        Value::I32(request.shard_id),
                        tree,
                        branch,
                        Value::Bytes(data),
                        Value::Text(encoding),
                    ],
                )?;
                if count != 1 {
                    return Err(internal(format!(
                        "expected 1 row to be affected for tree table, got {count}"
                    )));
                }
            }
            Ok(())
        })
    }
    async fn read_history_branch(
        &self,
        request: HistoryBranchRequest,
    ) -> Result<HistoryBranchPage, PersistenceError> {
        let min = if request.next_page_token.is_empty() {
            request.min_node_id
        } else {
            history_sql::decode_history_node_page_token(&request.next_page_token)
                .map_err(internal)?
                .wrapping_add(1)
        };
        let params = [
            Value::I32(request.shard_id),
            uuid(&request.tree_id)?,
            uuid(&request.branch_id)?,
            Value::I64(min),
            Value::I64(request.max_node_id),
            Value::I32(request.page_size),
        ];
        let rows = self.backend.transaction(|tx| {
            tx.query(
                self.backend
                    .dialect()
                    .statement(Statement::ReadHistoryNodes),
                &params,
                &[Column::I64, Column::I64, Column::Bytes, Column::Text],
            )
        })?;
        if rows.is_empty() {
            return Ok(HistoryBranchPage::default());
        }
        let nodes = rows
            .iter()
            .map(|row| {
                Ok(history_sql::HistoryNode {
                    node_id: row[0].integer()?,
                    transaction_id: row[1].integer()?.wrapping_neg(),
                    data: row[2].bytes()?,
                    data_encoding: row[3].text()?,
                })
            })
            .collect::<Result<Vec<_>, PersistenceError>>()?;
        history_sql::build_history_branch_page(
            &nodes,
            request.last_node_id,
            request.last_transaction_id,
            request.page_size,
        )
        .map_err(|error| match error {
            history_sql::HistoryPersistenceError::InternalDataInconsistency(m) => {
                PersistenceError::InternalDataInconsistency(m)
            }
            e => internal(e),
        })
    }
    async fn fork_history_branch(
        &self,
        request: HistoryBranchRequest,
        fork_node_id: i64,
        new_branch_id: String,
    ) -> Result<HistoryBranchRequest, PersistenceError> {
        let new = crate::history_branch::fork_request(&request, fork_node_id, new_branch_id);
        let (data, encoding) = self.tree_blob(&new)?;
        let params = [
            Value::I32(new.shard_id),
            uuid(&new.tree_id)?,
            uuid(&new.branch_id)?,
            Value::Bytes(data),
            Value::Text(encoding),
        ];
        let count = self.backend.transaction(|tx| {
            tx.execute(
                self.backend
                    .dialect()
                    .statement(Statement::InsertHistoryTree),
                &params,
            )
        })?;
        if count != 1 {
            return Err(internal(format!(
                "expected 1 row to be affected for tree table, got {count}"
            )));
        }
        Ok(new)
    }
    async fn delete_history_branch(
        &self,
        request: HistoryBranchRequest,
    ) -> Result<(), PersistenceError> {
        let branches = self.get_history_tree(request.clone()).await?;
        let referred = crate::history_branch::max_referred_node_ids(&branches);
        let tree = uuid(&request.tree_id)?;
        let dialect = self.backend.dialect();
        self.backend.transaction(|tx| {
            tx.execute(
                dialect.statement(Statement::DeleteHistoryTree),
                &[
                    Value::I32(request.shard_id),
                    tree.clone(),
                    uuid(&request.branch_id)?,
                ],
            )?;
            for range in crate::history_branch::deletion_ranges(&request) {
                let shared = referred.get(&range.branch_id);
                let min = shared.copied().unwrap_or(range.begin_node_id);
                let mut params = vec![
                    Value::I32(request.shard_id),
                    tree.clone(),
                    uuid(&range.branch_id)?,
                    Value::I64(min),
                ];
                let statement = if self.delete_batch_size > 0 {
                    params.push(Value::I32(self.delete_batch_size));
                    Statement::RangeDeleteHistoryNodes
                } else {
                    Statement::DeleteHistoryNodes
                };
                loop {
                    let count = tx.execute(dialect.statement(statement), &params)?;
                    if self.delete_batch_size <= 0 || count != self.delete_batch_size as u64 {
                        break;
                    }
                }
                if shared.is_some() {
                    break;
                }
            }
            Ok(())
        })
    }
    async fn get_history_tree(
        &self,
        request: HistoryBranchRequest,
    ) -> Result<Vec<HistoryTreeBranch>, PersistenceError> {
        let params = [Value::I32(request.shard_id), uuid(&request.tree_id)?];
        let rows = self.backend.transaction(|tx| {
            tx.query(
                self.backend.dialect().statement(Statement::GetHistoryTree),
                &params,
                &[Column::Bytes, Column::Bytes, Column::Text],
            )
        })?;
        rows.iter()
            .map(|row| {
                Self::tree_branch(
                    request.shard_id,
                    request.tree_id.clone(),
                    &row[0],
                    &row[1],
                    &row[2],
                )
            })
            .collect()
    }
    async fn get_all_history_tree_branches(
        &self,
        _shard_id: i32,
        page_size: i32,
        next_page_token: Vec<u8>,
    ) -> Result<HistoryTreePage, PersistenceError> {
        let cursor = history_tree_token::decode(&next_page_token)?;
        let params = [
            Value::I32(cursor.shard_id),
            Value::Bytes(cursor.tree_id.clone()),
            Value::Bytes(cursor.branch_id),
            Value::I32(cursor.shard_id),
            Value::Bytes(cursor.tree_id),
            Value::I32(cursor.shard_id),
            Value::I32(page_size),
        ];
        let rows = self.backend.transaction(|tx| {
            tx.query(
                self.backend
                    .dialect()
                    .statement(Statement::GetAllHistoryTreeBranches),
                &params,
                &[
                    Column::I32,
                    Column::Bytes,
                    Column::Bytes,
                    Column::Bytes,
                    Column::Text,
                ],
            )
        })?;
        let branches = rows
            .iter()
            .map(|row| {
                Self::tree_branch(
                    row[0].integer()? as i32,
                    uuid_string(&row[1])?,
                    &row[2],
                    &row[3],
                    &row[4],
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let next_page_token = if !rows.is_empty() && rows.len() >= page_size.max(0) as usize {
            let row = &rows[rows.len() - 1];
            history_tree_token::encode(&HistoryTreeCursor {
                shard_id: row[0].integer()? as i32,
                tree_id: row[1].bytes()?,
                branch_id: row[2].bytes()?,
            })
        } else {
            Vec::new()
        };
        Ok(HistoryTreePage {
            branches,
            next_page_token,
        })
    }
}
