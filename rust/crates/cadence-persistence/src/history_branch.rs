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
