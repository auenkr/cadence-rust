//! Generic execution scanner and fixer workflow configuration semantics.
//!
//! Mirrors `scanner/shardscanner/activities.go` and
//! `scanner/shardscanner/fixer_workflow.go`. Dynamic property lookup itself is
//! provided by the service activity; these functions apply the Go overrides
//! after those values have been read.

use std::collections::BTreeMap;

pub type CustomScannerConfig = BTreeMap<String, String>;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GenericScannerConfig {
    pub enabled: bool,
    pub concurrency: i32,
    pub page_size: i32,
    pub blobstore_flush_threshold: i32,
    pub activity_batch_size: i32,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GenericScannerConfigOverrides {
    pub enabled: Option<bool>,
    pub concurrency: Option<i32>,
    pub page_size: Option<i32>,
    pub blobstore_flush_threshold: Option<i32>,
    pub activity_batch_size: Option<i32>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ScannerWorkflowConfigOverrides {
    pub generic: GenericScannerConfigOverrides,
    /// `Some(empty)` is distinct from `None` and replaces the hook config.
    pub custom: Option<CustomScannerConfig>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResolvedScannerWorkflowConfig {
    pub generic: GenericScannerConfig,
    pub custom: CustomScannerConfig,
}

pub fn resolve_scanner_config(
    dynamic: GenericScannerConfig,
    hook_custom: CustomScannerConfig,
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
        custom: overrides.custom.unwrap_or(hook_custom),
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FixerWorkflowConfigOverrides {
    pub concurrency: Option<i32>,
    pub blobstore_flush_threshold: Option<i32>,
    pub activity_batch_size: Option<i32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
            hook_config,
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
        assert_eq!(resolved.custom, custom_override);
    }

    #[test]
    fn scanner_custom_config_none_preserves_hook_but_empty_override_replaces_it() {
        let mut hook = CustomScannerConfig::new();
        hook.insert("invariant".to_owned(), "true".to_owned());
        let preserve = resolve_scanner_config(
            GenericScannerConfig::default(),
            hook.clone(),
            ScannerWorkflowConfigOverrides::default(),
        );
        assert_eq!(preserve.custom, hook);

        let replace = resolve_scanner_config(
            GenericScannerConfig::default(),
            preserve.custom,
            ScannerWorkflowConfigOverrides {
                custom: Some(CustomScannerConfig::new()),
                ..Default::default()
            },
        );
        assert!(replace.custom.is_empty());
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
}
