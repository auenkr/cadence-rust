//! Sparse shard-map query pagination from Go `shardscanner/aggregators.go`.

use std::collections::BTreeMap;

pub const MAX_SHARD_QUERY_RESULT: usize = 1_000;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PaginatedShardQueryRequest {
    pub starting_shard_id: Option<i32>,
    pub limit_shards: Option<i32>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ShardQueryPaginationToken {
    pub next_shard_id: Option<i32>,
    pub is_done: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ShardPage<T> {
    pub result: BTreeMap<i32, T>,
    pub pagination: ShardQueryPaginationToken,
}

/// Select an inclusive shard range from a sparse map. Go advances through the
/// numeric shard IDs, so absent entries do not consume the page limit.
pub fn paginate_shards<T: Clone>(
    min_shard_id: i32,
    max_shard_id: i32,
    request: PaginatedShardQueryRequest,
    values: &BTreeMap<i32, T>,
) -> Result<ShardPage<T>, String> {
    let mut current = request.starting_shard_id.unwrap_or(min_shard_id);
    if current < min_shard_id || current > max_shard_id {
        return Err(format!(
            "requested shard {current} is outside of bounds (min: {min_shard_id} and max: {max_shard_id})"
        ));
    }
    let limit = request
        .limit_shards
        .filter(|limit| *limit > 0 && *limit < MAX_SHARD_QUERY_RESULT as i32)
        .map_or(MAX_SHARD_QUERY_RESULT, |limit| limit as usize);
    let mut result = BTreeMap::new();
    while result.len() < limit && current <= max_shard_id {
        if let Some(value) = values.get(&current) {
            result.insert(current, value.clone());
        }
        current = current.saturating_add(1);
    }
    if current > max_shard_id {
        Ok(ShardPage {
            result,
            pagination: ShardQueryPaginationToken {
                next_shard_id: None,
                is_done: true,
            },
        })
    } else {
        Ok(ShardPage {
            result,
            pagination: ShardQueryPaginationToken {
                next_shard_id: Some(current),
                is_done: false,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{paginate_shards, PaginatedShardQueryRequest, ShardQueryPaginationToken};

    #[test]
    fn go_status_query_skips_sparse_shards_and_limits_present_results() {
        let values = BTreeMap::from([(1, "running"), (4, "success"), (8, "failure")]);
        let page = paginate_shards(
            1,
            9,
            PaginatedShardQueryRequest {
                starting_shard_id: None,
                limit_shards: Some(2),
            },
            &values,
        )
        .unwrap();
        assert_eq!(page.result, BTreeMap::from([(1, "running"), (4, "success")]));
        assert_eq!(
            page.pagination,
            ShardQueryPaginationToken {
                next_shard_id: Some(5),
                is_done: false,
            }
        );
    }

    #[test]
    fn go_status_query_uses_default_limit_and_reports_exact_bounds_errors() {
        let values = BTreeMap::from([(3, "done")]);
        let page = paginate_shards(
            3,
            3,
            PaginatedShardQueryRequest {
                starting_shard_id: None,
                limit_shards: Some(0),
            },
            &values,
        )
        .unwrap();
        assert_eq!(page.result.len(), 1);
        assert_eq!(
            page.pagination,
            ShardQueryPaginationToken {
                next_shard_id: None,
                is_done: true,
            }
        );
        assert_eq!(
            paginate_shards(
                3,
                9,
                PaginatedShardQueryRequest {
                    starting_shard_id: Some(10),
                    limit_shards: None,
                },
                &values,
            ),
            Err("requested shard 10 is outside of bounds (min: 3 and max: 9)".to_owned())
        );
    }

    #[test]
    fn next_cursor_points_after_the_last_numeric_id_examined() {
        let values = BTreeMap::from([(3, "done")]);
        let page = paginate_shards(
            3,
            20,
            PaginatedShardQueryRequest {
                starting_shard_id: Some(3),
                limit_shards: Some(1),
            },
            &values,
        )
        .unwrap();
        assert_eq!(page.pagination.next_shard_id, Some(4));
        let empty_page = paginate_shards(
            3,
            20,
            PaginatedShardQueryRequest {
                starting_shard_id: page.pagination.next_shard_id,
                limit_shards: Some(1),
            },
            &values,
        )
        .unwrap();
        assert!(empty_page.result.is_empty());
        assert_eq!(empty_page.pagination.next_shard_id, None);
        assert!(empty_page.pagination.is_done);
    }
}
