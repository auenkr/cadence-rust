//! Go persistence history-branch tokens: 0x59 preamble + binary Thrift.
use cadence_api::{
    errors::BadRequestError,
    types::{HistoryBranch, HistoryBranchRange},
};
use cadence_thrift::{Reader, TType, Writer};

type Result<T> = std::result::Result<T, BadRequestError>;

pub fn encode(branch: &HistoryBranch) -> Vec<u8> {
    let mut writer = Writer::new();
    writer.write_u8(0x59);
    writer.write_field_begin(TType::String, 10);
    writer.write_string(&branch.tree_id);
    writer.write_field_begin(TType::String, 20);
    writer.write_string(&branch.branch_id);
    writer.write_field_begin(TType::List, 30);
    // Even an empty ancestor list has STRUCT element type in Go's codec.
    writer.write_list_begin(TType::Struct, branch.ancestors.len());
    for range in &branch.ancestors {
        writer.write_field_begin(TType::String, 10);
        writer.write_string(&range.branch_id);
        writer.write_field_begin(TType::I64, 20);
        writer.write_i64(range.begin_node_id);
        writer.write_field_begin(TType::I64, 30);
        writer.write_i64(range.end_node_id);
        writer.write_field_stop();
    }
    writer.write_field_stop();
    writer.into_bytes()
}

pub fn decode(token: &[u8]) -> Result<HistoryBranch> {
    match token.first() {
        None => return Err(BadRequestError::new("Missing binary encoding version.")),
        Some(0x59) => (),
        _ => return Err(BadRequestError::new("Invalid binary encoding version.")),
    }
    let decode = || -> cadence_thrift::Result<HistoryBranch> {
        let value = cadence_thrift::value::read_struct(&mut Reader::new(&token[1..]))?;
        let ancestors = match value.get(30) {
            None => Vec::new(),
            Some(cadence_thrift::Value::List(values)) => values
                .iter()
                .map(|v| {
                    let cadence_thrift::Value::Struct(range) = v else {
                        return Err(cadence_thrift::Error::Invalid(
                            "history branch ancestor is not a struct".to_string(),
                        ));
                    };
                    Ok(HistoryBranchRange {
                        branch_id: range.req_string(10, "")?,
                        begin_node_id: range.i64_or_zero(20)?,
                        end_node_id: range.i64_or_zero(30)?,
                    })
                })
                .collect::<cadence_thrift::Result<Vec<_>>>()?,
            _ => {
                return Err(cadence_thrift::Error::Invalid(
                    "history branch ancestors is not a list".to_string(),
                ))
            }
        };
        Ok(HistoryBranch {
            tree_id: value.req_string(10, "")?,
            branch_id: value.req_string(20, "")?,
            ancestors,
        })
    };
    decode().map_err(|e| BadRequestError::new(e.to_string()))
}

pub fn new_token(tree_id: &str, branch_id: &str) -> Vec<u8> {
    encode(&HistoryBranch {
        tree_id: tree_id.to_string(),
        branch_id: branch_id.to_string(),
        ancestors: Vec::new(),
    })
}

/// Go `sqlHistoryStore.ForkHistoryBranch` ancestor selection using the typed
/// History persistence contract. The new branch inherits an existing range
/// when the fork point is before the branch's begin node, otherwise it appends
/// a range for the source branch.
pub fn fork_request(
    source: &crate::history_contract::HistoryBranchRequest,
    fork_node_id: i64,
    new_branch_id: String,
) -> crate::history_contract::HistoryBranchRequest {
    let begin = source.ancestors.last().map_or(1, |range| range.end_node_id);
    let mut ancestors = Vec::with_capacity(source.ancestors.len().saturating_add(1));
    if begin >= fork_node_id {
        for range in &source.ancestors {
            if range.end_node_id >= fork_node_id {
                let mut clipped = range.clone();
                clipped.end_node_id = fork_node_id;
                ancestors.push(clipped);
                break;
            }
            ancestors.push(range.clone());
        }
    } else {
        ancestors.clone_from(&source.ancestors);
        ancestors.push(crate::history_contract::HistoryBranchRange {
            branch_id: source.branch_id.clone(), begin_node_id: begin, end_node_id: fork_node_id,
        });
    }
    let mut result = source.clone();
    result.branch_id = new_branch_id;
    result.ancestors = ancestors;
    result.fork_node_id = fork_node_id;
    result
}

/// Go `GetBranchesMaxReferredNodeIDs`: maximum exclusive end node among all
/// valid branches' ancestor ranges for each physical branch.
pub fn max_referred_node_ids(branches: &[crate::history_contract::HistoryTreeBranch]) -> std::collections::BTreeMap<String, i64> {
    let mut referred = std::collections::BTreeMap::new();
    for branch in branches {
        for range in &branch.ancestors {
            referred.entry(range.branch_id.clone())
                .and_modify(|end: &mut i64| *end = (*end).max(range.end_node_id))
                .or_insert(range.end_node_id);
        }
    }
    referred
}

/// Physical ranges Go's branch deletion considers, ordered from leaf back
/// through ancestors. The caller combines these ranges with
/// `max_referred_node_ids` to choose each deletion lower bound.
pub fn deletion_ranges(branch: &crate::history_contract::HistoryBranchRequest) -> Vec<crate::history_contract::HistoryBranchRange> {
    let mut ranges = branch.ancestors.clone();
    ranges.push(crate::history_contract::HistoryBranchRange {
        branch_id: branch.branch_id.clone(), begin_node_id: branch.ancestors.last().map_or(1, |range| range.end_node_id), end_node_id: 0,
    });
    ranges.reverse();
    ranges
}

/// Same ancestor clipping as Go's SQL/Cassandra ForkHistoryBranch stores.
pub fn fork(source: &HistoryBranch, fork_node: i64, branch_id: &str) -> Result<HistoryBranch> {
    if fork_node <= 1 {
        return Err(BadRequestError::new("ForkNodeID must be > 1"));
    }
    let begin = source.ancestors.last().map(|r| r.end_node_id).unwrap_or(1);
    let mut ancestors = Vec::new();
    if begin >= fork_node {
        for range in &source.ancestors {
            let mut range = range.clone();
            if range.end_node_id >= fork_node {
                range.end_node_id = fork_node;
                ancestors.push(range);
                break;
            }
            ancestors.push(range);
        }
    } else {
        ancestors.clone_from(&source.ancestors);
        ancestors.push(HistoryBranchRange {
            branch_id: source.branch_id.clone(),
            begin_node_id: begin,
            end_node_id: fork_node,
        });
    }
    Ok(HistoryBranch {
        tree_id: source.tree_id.clone(),
        branch_id: branch_id.to_string(),
        ancestors,
    })
}

#[cfg(test)]
mod contract_tests {
    use super::*;
    use crate::history_contract::{HistoryBranchRange as Range, HistoryBranchRequest as Request, HistoryTreeBranch};

    fn source() -> Request {
        Request { tree_id: "tree".into(), branch_id: "b2".into(), ancestors: vec![Range { branch_id: "b1".into(), begin_node_id: 1, end_node_id: 6 }], ..Default::default() }
    }

    #[test]
    fn fork_appends_source_range_or_clips_existing_ancestor() {
        let appended = fork_request(&source(), 8, "b3".into());
        assert_eq!(appended.ancestors, vec![
            Range { branch_id: "b1".into(), begin_node_id: 1, end_node_id: 6 },
            Range { branch_id: "b2".into(), begin_node_id: 6, end_node_id: 8 },
        ]);
        let clipped = fork_request(&source(), 4, "b4".into());
        assert_eq!(clipped.ancestors, vec![Range { branch_id: "b1".into(), begin_node_id: 1, end_node_id: 4 }]);
    }

    #[test]
    fn delete_ranges_are_leaf_to_root_and_shared_end_is_maximized() {
        let ranges = deletion_ranges(&source());
        assert_eq!(ranges.iter().map(|range| range.branch_id.as_str()).collect::<Vec<_>>(), ["b2", "b1"]);
        let branches = vec![
            HistoryTreeBranch { ancestors: vec![Range { branch_id: "b1".into(), begin_node_id: 1, end_node_id: 5 }], ..Default::default() },
            HistoryTreeBranch { ancestors: vec![Range { branch_id: "b1".into(), begin_node_id: 1, end_node_id: 9 }], ..Default::default() },
        ];
        assert_eq!(max_referred_node_ids(&branches).get("b1"), Some(&9));
    }
}
