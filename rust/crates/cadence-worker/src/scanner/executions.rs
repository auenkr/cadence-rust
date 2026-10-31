//! Generic execution scanner and fixer workflow configuration semantics.
//!
//! Mirrors `scanner/shardscanner/activities.go` and
//! `scanner/shardscanner/fixer_workflow.go`. Dynamic property lookup itself is
//! provided by the service activity; these functions apply the Go overrides
//! after those values have been read.

use std::collections::BTreeMap;

pub type CustomScannerConfig = BTreeMap<String, String>;

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
    pub shards: Vec<i32>,
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
            shards: vec![4, 5],
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
}
