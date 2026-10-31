//! Scanner shard selection contract from Go `scanner/shardscanner/types.go`.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardRange {
    /// Inclusive lower bound.
    pub min: i32,
    /// Exclusive upper bound.
    pub max: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Shards {
    /// `None` is Go nil; `Some(vec![])` is an explicitly empty list.
    pub list: Option<Vec<i32>>,
    pub range: Option<ShardRange>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShardsError {
    MustProvideListOrRange,
    OnlyOneOfListOrRange,
    EmptyList,
    EmptyRange,
}

impl ShardsError {
    pub fn go_message(self) -> &'static str {
        match self {
            Self::MustProvideListOrRange => "must provide either List or Range",
            Self::OnlyOneOfListOrRange => "only one of List or Range can be provided",
            Self::EmptyList => "empty List provided",
            Self::EmptyRange => "empty Range provided",
        }
    }
}

impl Shards {
    pub fn validate(&self) -> Result<(), ShardsError> {
        if self.list.is_none() && self.range.is_none() {
            return Err(ShardsError::MustProvideListOrRange);
        }
        if self.list.is_some() && self.range.is_some() {
            return Err(ShardsError::OnlyOneOfListOrRange);
        }
        if self.list.as_ref().is_some_and(Vec::is_empty) {
            return Err(ShardsError::EmptyList);
        }
        if self.range.is_some_and(|range| range.max <= range.min) {
            return Err(ShardsError::EmptyRange);
        }
        Ok(())
    }

    /// Flatten a validated selection and report min/max selected shard IDs.
    /// Range selection uses the Go inclusive-min/exclusive-max convention;
    /// explicit lists retain caller order and may contain duplicates.
    pub fn flatten(&self) -> Result<(Vec<i32>, i32, i32), ShardsError> {
        self.validate()?;
        let ids = match (&self.list, self.range) {
            (Some(ids), None) => ids.clone(),
            (None, Some(range)) => (range.min..range.max).collect(),
            _ => return Err(ShardsError::MustProvideListOrRange),
        };
        let min = *ids.iter().min().ok_or(ShardsError::EmptyList)?;
        let max = *ids.iter().max().ok_or(ShardsError::EmptyList)?;
        Ok((ids, min, max))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScannerSelection {
    pub workflow_name: String,
    pub shard_ids: Vec<i32>,
    pub min_shard: i32,
    pub max_shard: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScannerSelectionError {
    WorkflowNameMissing,
    InvalidShards(ShardsError),
}

impl ScannerSelection {
    /// Mirrors `NewScannerWorkflow`: require a name before validating and
    /// flattening the configured shard set.
    pub fn new(name: impl Into<String>, shards: &Shards) -> Result<Self, ScannerSelectionError> {
        let workflow_name = name.into();
        if workflow_name.is_empty() {
            return Err(ScannerSelectionError::WorkflowNameMissing);
        }
        let (shard_ids, min_shard, max_shard) = shards
            .flatten()
            .map_err(ScannerSelectionError::InvalidShards)?;
        Ok(Self {
            workflow_name,
            shard_ids,
            min_shard,
            max_shard,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ScannerSelection, ScannerSelectionError, ShardRange, Shards, ShardsError,
    };

    #[test]
    fn go_shards_validation_distinguishes_nil_empty_and_conflicting_inputs() {
        assert_eq!(
            Shards {
                list: None,
                range: None,
            }
            .validate(),
            Err(ShardsError::MustProvideListOrRange)
        );
        assert_eq!(
            Shards {
                list: Some(vec![]),
                range: None,
            }
            .validate(),
            Err(ShardsError::EmptyList)
        );
        assert_eq!(
            Shards {
                list: Some(vec![1]),
                range: Some(ShardRange { min: 0, max: 2 }),
            }
            .validate(),
            Err(ShardsError::OnlyOneOfListOrRange)
        );
        assert_eq!(
            Shards {
                list: None,
                range: Some(ShardRange { min: 2, max: 2 }),
            }
            .validate(),
            Err(ShardsError::EmptyRange)
        );
    }

    #[test]
    fn go_shards_flatten_uses_half_open_ranges_and_list_extrema() {
        assert_eq!(
            Shards {
                list: None,
                range: Some(ShardRange { min: 5, max: 9 }),
            }
            .flatten(),
            Ok((vec![5, 6, 7, 8], 5, 8))
        );
        assert_eq!(
            Shards {
                list: Some(vec![9, 3, 7, 3]),
                range: None,
            }
            .flatten(),
            Ok((vec![9, 3, 7, 3], 3, 9))
        );
    }

    #[test]
    fn scanner_construction_checks_name_before_shards_like_go() {
        let no_shards = Shards {
            list: None,
            range: None,
        };
        assert_eq!(
            ScannerSelection::new("", &no_shards),
            Err(ScannerSelectionError::WorkflowNameMissing)
        );
        assert_eq!(
            ScannerSelection::new("executions", &no_shards),
            Err(ScannerSelectionError::InvalidShards(
                ShardsError::MustProvideListOrRange
            ))
        );
        assert_eq!(
            ScannerSelection::new(
                "executions",
                &Shards {
                    list: None,
                    range: Some(ShardRange { min: 4, max: 7 }),
                }
            ),
            Ok(ScannerSelection {
                workflow_name: "executions".to_owned(),
                shard_ids: vec![4, 5, 6],
                min_shard: 4,
                max_shard: 6,
            })
        );
    }
}
