//! Generic execution scanner and fixer workflow configuration semantics.
//!
//! Mirrors `scanner/shardscanner/activities.go` and
//! `scanner/shardscanner/fixer_workflow.go`. Dynamic property lookup itself is
//! provided by the service activity; these functions apply the Go overrides
//! after those values have been read.

use std::collections::BTreeMap;

pub type CustomScannerConfig = BTreeMap<String, String>;

pub const ERR_SCAN_WORKFLOW_NOT_CLOSED: &str =
    "scan workflow is not closed, only can run fix on output of finished scan workflow";
pub const ERR_SERIALIZATION: &str = "encountered serialization error";
pub const ERR_MISSING_HOOKS: &str = "hooks are not provided for this scanner";

pub const SHORT_ACTIVITY_SCHEDULE_TO_START_SECONDS: u64 = 60;
pub const SHORT_ACTIVITY_START_TO_CLOSE_SECONDS: u64 = 5 * 60;
pub const SHORT_ACTIVITY_RETRY_INITIAL_SECONDS: u64 = 1;
pub const SHORT_ACTIVITY_RETRY_MAXIMUM_SECONDS: u64 = 5 * 60;
pub const SHORT_ACTIVITY_RETRY_EXPIRATION_SECONDS: u64 = 10 * 60;
pub const LONG_ACTIVITY_SCHEDULE_TO_START_SECONDS: u64 = 60;
pub const LONG_ACTIVITY_START_TO_CLOSE_SECONDS: u64 = 48 * 60 * 60;
pub const LONG_ACTIVITY_HEARTBEAT_SECONDS: u64 = 60;
pub const LONG_ACTIVITY_RETRY_INITIAL_SECONDS: u64 = 1;
pub const LONG_ACTIVITY_RETRY_MAXIMUM_SECONDS: u64 = 5 * 60;
pub const LONG_ACTIVITY_RETRY_EXPIRATION_SECONDS: u64 = 48 * 60 * 60;
pub const SHARD_ACTIVITY_RETRY_BACKOFF_COEFFICIENT_MILLI: u64 = 1_700;

/// Go `supportsFixerConfig` uses this version gate for historical runs.
pub const FIXER_CONFIG_CHANGE_VERSION_ID: &str = "dynamic fixer config";
pub const FIXER_CONFIG_CHANGE_MIN_VERSION: i32 = 1;

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct GenericScannerConfig {
    pub enabled: bool,
    pub concurrency: i32,
    pub page_size: i32,
    pub blobstore_flush_threshold: i32,
    pub activity_batch_size: i32,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct GenericScannerConfigOverrides {
    pub enabled: Option<bool>,
    pub concurrency: Option<i32>,
    pub page_size: Option<i32>,
    pub blobstore_flush_threshold: Option<i32>,
    pub activity_batch_size: Option<i32>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct ScannerWorkflowConfigOverrides {
    #[serde(rename = "GenericScannerConfig")]
    pub generic: GenericScannerConfigOverrides,
    /// `Some(empty)` is distinct from `None` and replaces the hook config.
    #[serde(rename = "CustomScannerConfig")]
    pub custom: Option<CustomScannerConfig>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct ResolvedScannerWorkflowConfig {
    #[serde(rename = "GenericScannerConfig")]
    pub generic: GenericScannerConfig,
    #[serde(rename = "CustomScannerConfig")]
    pub custom: Option<CustomScannerConfig>,
}

/// Go `ScannerWorkflowParams` payload sent when starting a scan workflow.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct ScannerWorkflowParams {
    pub shards: super::shards::Shards,
    pub scanner_workflow_config_overwrites: ScannerWorkflowConfigOverrides,
}

/// Go `ScannerConfigActivityParams` payload.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct ScannerConfigActivityParams {
    pub overwrites: ScannerWorkflowConfigOverrides,
}

/// Go `ScanShardActivityParams` payload. A nil Go map encodes as JSON null.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct ScanShardActivityParams {
    pub shards: Option<Vec<i32>>,
    pub page_size: i32,
    pub blobstore_flush_threshold: i32,
    pub scanner_config: Option<CustomScannerConfig>,
}

/// Go `FixerWorkflowParams` payload for fixing a completed scanner execution.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct FixerWorkflowParams {
    #[serde(rename = "ScannerWorkflowWorkflowID")]
    pub scanner_workflow_workflow_id: String,
    #[serde(rename = "ScannerWorkflowRunID")]
    pub scanner_workflow_run_id: String,
    pub fixer_workflow_config_overwrites: FixerWorkflowConfigOverrides,
}

/// Go `FixerCorruptedKeysActivityParams` payload, including optional range
/// restart position used by heartbeat recovery.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct FixerCorruptedKeysActivityParams {
    #[serde(rename = "ScannerWorkflowWorkflowID")]
    pub scanner_workflow_workflow_id: String,
    #[serde(rename = "ScannerWorkflowRunID")]
    pub scanner_workflow_run_id: String,
    #[serde(rename = "StartingShardID")]
    pub starting_shard_id: Option<i32>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct ShardQueryPaginationToken {
    pub next_shard_id: Option<i32>,
    pub is_done: bool,
}

/// One corrupted-key entry. `keys` remains a JSON value here because the
/// store's blob key schema is owned by the storage implementation.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct CorruptedKeysEntry {
    pub shard_id: i32,
    pub corrupted_keys: serde_json::Value,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct FixerCorruptedKeysActivityResult {
    pub corrupted_keys: Vec<CorruptedKeysEntry>,
    pub min_shard: Option<i32>,
    pub max_shard: Option<i32>,
    pub shard_query_pagination_token: ShardQueryPaginationToken,
}

/// Accumulate Go `GetCorruptedKeys` activity pages. It requests at least one
/// page, follows `NextShardID` even across empty pages, and incorporates min /
/// max shard bounds only from pages containing entries.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CorruptedKeysAccumulator {
    entries: Vec<CorruptedKeysEntry>,
    min_shard: Option<i32>,
    max_shard: Option<i32>,
    next_shard_id: Option<i32>,
    fetched_page: bool,
}

impl CorruptedKeysAccumulator {
    pub fn should_fetch(&self) -> bool {
        !self.fetched_page || self.next_shard_id.is_some()
    }

    pub fn starting_shard_id(&self) -> Option<i32> {
        self.next_shard_id
    }

    pub fn add_page(&mut self, page: FixerCorruptedKeysActivityResult) {
        self.fetched_page = true;
        self.next_shard_id = page.shard_query_pagination_token.next_shard_id;
        if page.corrupted_keys.is_empty() {
            return;
        }
        self.entries.extend(page.corrupted_keys);
        if let Some(min) = page.min_shard {
            self.min_shard = Some(self.min_shard.map_or(min, |current| current.min(min)));
        }
        if let Some(max) = page.max_shard {
            self.max_shard = Some(self.max_shard.map_or(max, |current| current.max(max)));
        }
    }

    pub fn finish(self) -> FixerCorruptedKeysActivityResult {
        FixerCorruptedKeysActivityResult {
            corrupted_keys: self.entries,
            min_shard: self.min_shard,
            max_shard: self.max_shard,
            shard_query_pagination_token: ShardQueryPaginationToken {
                next_shard_id: None,
                is_done: true,
            },
        }
    }
}

pub fn resolve_scanner_config(
    dynamic: GenericScannerConfig,
    hook_custom: Option<CustomScannerConfig>,
    overrides: ScannerWorkflowConfigOverrides,
) -> ResolvedScannerWorkflowConfig {
    let mut generic = dynamic;
    if let Some(value) = overrides.generic.enabled {
        generic.enabled = value;
    }
    if let Some(value) = overrides.generic.concurrency {
        generic.concurrency = value;
    }
    if let Some(value) = overrides.generic.page_size {
        generic.page_size = value;
    }
    if let Some(value) = overrides.generic.blobstore_flush_threshold {
        generic.blobstore_flush_threshold = value;
    }
    if let Some(value) = overrides.generic.activity_batch_size {
        generic.activity_batch_size = value;
    }

    ResolvedScannerWorkflowConfig {
        generic,
        custom: overrides.custom.or(hook_custom),
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct FixerWorkflowConfigOverrides {
    pub concurrency: Option<i32>,
    pub blobstore_flush_threshold: Option<i32>,
    pub activity_batch_size: Option<i32>,
}

#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize,
)]
#[serde(default, rename_all = "PascalCase")]
pub struct ResolvedFixerWorkflowConfig {
    pub concurrency: i32,
    pub blobstore_flush_threshold: i32,
    pub activity_batch_size: i32,
}

pub fn resolve_fixer_config(
    overrides: FixerWorkflowConfigOverrides,
) -> ResolvedFixerWorkflowConfig {
    ResolvedFixerWorkflowConfig {
        concurrency: overrides.concurrency.unwrap_or(25),
        blobstore_flush_threshold: overrides.blobstore_flush_threshold.unwrap_or(1_000),
        activity_batch_size: overrides.activity_batch_size.unwrap_or(200),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        resolve_fixer_config, resolve_scanner_config, CustomScannerConfig,
        FixerWorkflowConfigOverrides, GenericScannerConfig, GenericScannerConfigOverrides,
        ScannerWorkflowConfigOverrides,
    };

    #[test]
    fn scanner_config_reads_dynamic_values_then_applies_each_go_override() {
        let dynamic = GenericScannerConfig {
            enabled: false,
            concurrency: 25,
            page_size: 100,
            blobstore_flush_threshold: 1_000,
            activity_batch_size: 10,
        };
        let mut hook_config = CustomScannerConfig::new();
        hook_config.insert("check-a".to_owned(), "enabled".to_owned());
        let mut custom_override = CustomScannerConfig::new();
        custom_override.insert("check-b".to_owned(), "enabled".to_owned());

        let resolved = resolve_scanner_config(
            dynamic,
            Some(hook_config),
            ScannerWorkflowConfigOverrides {
                generic: GenericScannerConfigOverrides {
                    enabled: Some(true),
                    concurrency: Some(7),
                    activity_batch_size: Some(1),
                    ..Default::default()
                },
                custom: Some(custom_override.clone()),
            },
        );
        assert!(resolved.generic.enabled);
        assert_eq!(resolved.generic.concurrency, 7);
        assert_eq!(resolved.generic.page_size, 100);
        assert_eq!(resolved.generic.blobstore_flush_threshold, 1_000);
        assert_eq!(resolved.generic.activity_batch_size, 1);
        assert_eq!(resolved.custom, Some(custom_override));
    }

    #[test]
    fn scanner_custom_config_none_preserves_hook_but_empty_override_replaces_it() {
        let mut hook = CustomScannerConfig::new();
        hook.insert("invariant".to_owned(), "true".to_owned());
        let preserve = resolve_scanner_config(
            GenericScannerConfig::default(),
            Some(hook.clone()),
            ScannerWorkflowConfigOverrides::default(),
        );
        assert_eq!(preserve.custom, Some(hook));

        let replace = resolve_scanner_config(
            GenericScannerConfig::default(),
            preserve.custom,
            ScannerWorkflowConfigOverrides {
                custom: Some(CustomScannerConfig::new()),
                ..Default::default()
            },
        );
        assert_eq!(replace.custom, Some(CustomScannerConfig::new()));
    }

    #[test]
    fn fixer_config_matches_go_defaults_and_pointer_overrides() {
        assert_eq!(
            resolve_fixer_config(FixerWorkflowConfigOverrides::default()),
            super::ResolvedFixerWorkflowConfig {
                concurrency: 25,
                blobstore_flush_threshold: 1_000,
                activity_batch_size: 200,
            }
        );
        assert_eq!(
            resolve_fixer_config(FixerWorkflowConfigOverrides {
                concurrency: Some(1_000),
                ..Default::default()
            })
            .concurrency,
            1_000
        );
    }

    #[test]
    fn scanner_workflow_params_use_go_field_names_and_null_pointer_fields() {
        let params = super::ScannerWorkflowParams {
            shards: super::super::shards::Shards {
                list: Some(vec![3, 7]),
                range: None,
            },
            scanner_workflow_config_overwrites: super::ScannerWorkflowConfigOverrides {
                generic: GenericScannerConfigOverrides {
                    concurrency: Some(4),
                    ..Default::default()
                },
                custom: None,
            },
        };
        let value = serde_json::to_value(params).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "Shards": {"List": [3, 7], "Range": null},
                "ScannerWorkflowConfigOverwrites": {
                    "GenericScannerConfig": {
                        "Enabled": null,
                        "Concurrency": 4,
                        "PageSize": null,
                        "BlobstoreFlushThreshold": null,
                        "ActivityBatchSize": null
                    },
                    "CustomScannerConfig": null
                }
            })
        );
    }

    #[test]
    fn scan_activity_preserves_null_custom_config_map() {
        let params = super::ScanShardActivityParams {
            shards: Some(vec![4, 5]),
            page_size: 100,
            blobstore_flush_threshold: 1_000,
            scanner_config: None,
        };
        assert_eq!(
            serde_json::to_value(params).unwrap(),
            serde_json::json!({
                "Shards": [4, 5],
                "PageSize": 100,
                "BlobstoreFlushThreshold": 1000,
                "ScannerConfig": null
            })
        );
    }

    #[test]
    fn fixer_workflow_params_keep_go_id_acronyms_and_nil_overrides() {
        let params = super::FixerWorkflowParams {
            scanner_workflow_workflow_id: "scan-id".to_owned(),
            scanner_workflow_run_id: "run-id".to_owned(),
            fixer_workflow_config_overwrites: FixerWorkflowConfigOverrides::default(),
        };
        assert_eq!(
            serde_json::to_value(params).unwrap(),
            serde_json::json!({
                "ScannerWorkflowWorkflowID": "scan-id",
                "ScannerWorkflowRunID": "run-id",
                "FixerWorkflowConfigOverwrites": {
                    "Concurrency": null,
                    "BlobstoreFlushThreshold": null,
                    "ActivityBatchSize": null
                }
            })
        );
    }

    #[test]
    fn fixer_corrupted_keys_payload_keeps_go_ids_and_nil_starting_shard() {
        let params = super::FixerCorruptedKeysActivityParams {
            scanner_workflow_workflow_id: "scan-id".to_owned(),
            scanner_workflow_run_id: "run-id".to_owned(),
            starting_shard_id: None,
        };
        assert_eq!(
            serde_json::to_value(params).unwrap(),
            serde_json::json!({
                "ScannerWorkflowWorkflowID": "scan-id",
                "ScannerWorkflowRunID": "run-id",
                "StartingShardID": null
            })
        );
    }

    #[test]
    fn shard_activity_timeouts_retry_errors_and_fixer_version_match_go() {
        assert_eq!(super::ERR_SCAN_WORKFLOW_NOT_CLOSED, "scan workflow is not closed, only can run fix on output of finished scan workflow");
        assert_eq!(super::ERR_SERIALIZATION, "encountered serialization error");
        assert_eq!(super::ERR_MISSING_HOOKS, "hooks are not provided for this scanner");
        assert_eq!(super::SHORT_ACTIVITY_SCHEDULE_TO_START_SECONDS, 60);
        assert_eq!(super::SHORT_ACTIVITY_START_TO_CLOSE_SECONDS, 300);
        assert_eq!(super::SHORT_ACTIVITY_RETRY_INITIAL_SECONDS, 1);
        assert_eq!(super::SHORT_ACTIVITY_RETRY_MAXIMUM_SECONDS, 300);
        assert_eq!(super::SHORT_ACTIVITY_RETRY_EXPIRATION_SECONDS, 600);
        assert_eq!(super::LONG_ACTIVITY_SCHEDULE_TO_START_SECONDS, 60);
        assert_eq!(super::LONG_ACTIVITY_START_TO_CLOSE_SECONDS, 172_800);
        assert_eq!(super::LONG_ACTIVITY_HEARTBEAT_SECONDS, 60);
        assert_eq!(super::LONG_ACTIVITY_RETRY_INITIAL_SECONDS, 1);
        assert_eq!(super::LONG_ACTIVITY_RETRY_MAXIMUM_SECONDS, 300);
        assert_eq!(super::LONG_ACTIVITY_RETRY_EXPIRATION_SECONDS, 172_800);
        assert_eq!(super::SHARD_ACTIVITY_RETRY_BACKOFF_COEFFICIENT_MILLI, 1_700);
        assert_eq!(super::FIXER_CONFIG_CHANGE_VERSION_ID, "dynamic fixer config");
        assert_eq!(super::FIXER_CONFIG_CHANGE_MIN_VERSION, 1);
    }

    #[test]
    fn corrupted_key_pages_follow_empty_pages_and_accumulate_bounds_in_go_order() {
        use super::{
            CorruptedKeysAccumulator, CorruptedKeysEntry, FixerCorruptedKeysActivityResult,
            ShardQueryPaginationToken,
        };
        let mut accumulator = CorruptedKeysAccumulator::default();
        assert!(accumulator.should_fetch());
        assert_eq!(accumulator.starting_shard_id(), None);
        accumulator.add_page(FixerCorruptedKeysActivityResult {
            corrupted_keys: Vec::new(),
            min_shard: Some(1),
            max_shard: Some(4),
            shard_query_pagination_token: ShardQueryPaginationToken {
                next_shard_id: Some(5),
                is_done: false,
            },
        });
        assert!(accumulator.should_fetch());
        assert_eq!(accumulator.starting_shard_id(), Some(5));
        accumulator.add_page(FixerCorruptedKeysActivityResult {
            corrupted_keys: vec![CorruptedKeysEntry {
                shard_id: 7,
                corrupted_keys: serde_json::json!({"Corrupt": "blob-key"}),
            }],
            min_shard: Some(6),
            max_shard: Some(8),
            shard_query_pagination_token: ShardQueryPaginationToken {
                next_shard_id: Some(9),
                is_done: false,
            },
        });
        accumulator.add_page(FixerCorruptedKeysActivityResult {
            corrupted_keys: vec![CorruptedKeysEntry {
                shard_id: 12,
                corrupted_keys: serde_json::json!({"Corrupt": "next-key"}),
            }],
            min_shard: Some(10),
            max_shard: Some(13),
            shard_query_pagination_token: ShardQueryPaginationToken {
                next_shard_id: None,
                is_done: true,
            },
        });
        assert!(!accumulator.should_fetch());
        let result = accumulator.finish();
        assert_eq!(result.corrupted_keys.len(), 2);
        assert_eq!(result.corrupted_keys[0].shard_id, 7);
        assert_eq!(result.corrupted_keys[1].shard_id, 12);
        assert_eq!(result.min_shard, Some(6));
        assert_eq!(result.max_shard, Some(13));
        assert_eq!(result.shard_query_pagination_token.next_shard_id, None);
        assert!(result.shard_query_pagination_token.is_done);
    }
}
