//! Service registry and health model.
//!
//! Mirrors the four Cadence services (`frontend`, `matching`, `history`,
//! `worker`) plus `sharddistributor`. Each service reports health in the
//! same shape as Go `common/types.HealthStatus` (`{ok, msg}`).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceName {
    Frontend,
    History,
    Matching,
    Worker,
    Sharddistributor,
}

impl ServiceName {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Frontend => "frontend",
            Self::History => "history",
            Self::Matching => "matching",
            Self::Worker => "worker",
            Self::Sharddistributor => "sharddistributor",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "frontend" => Some(Self::Frontend),
            "history" => Some(Self::History),
            "matching" => Some(Self::Matching),
            "worker" => Some(Self::Worker),
            "sharddistributor" | "shard-distributor" => Some(Self::Sharddistributor),
            _ => None,
        }
    }

    pub fn all() -> &'static [Self] {
        &[
            Self::Frontend,
            Self::History,
            Self::Matching,
            Self::Worker,
            Self::Sharddistributor,
        ]
    }
}

impl std::fmt::Display for ServiceName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Mirrors Go `types.HealthStatus{Ok, Msg}` with Go-identical JSON names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthStatus {
    #[serde(rename = "ok")]
    pub ok: bool,
    #[serde(rename = "msg", default, skip_serializing_if = "String::is_empty")]
    pub msg: String,
}

impl HealthStatus {
    #[must_use]
    pub fn healthy() -> Self {
        Self {
            ok: true,
            msg: String::new(),
        }
    }

    #[must_use]
    #[allow(dead_code)]
    pub fn unhealthy(msg: impl Into<String>) -> Self {
        Self {
            ok: false,
            msg: msg.into(),
        }
    }
}

#[derive(Debug, Default)]
pub struct ServiceRegistry {
    health: HashMap<ServiceName, HealthStatus>,
}

impl ServiceRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn mark_healthy(&mut self, service: ServiceName) {
        self.health.insert(service, HealthStatus::healthy());
    }

    #[allow(dead_code)]
    pub fn mark_unhealthy(&mut self, service: ServiceName, msg: impl Into<String>) {
        self.health.insert(service, HealthStatus::unhealthy(msg));
    }

    #[must_use]
    #[allow(dead_code)]
    pub fn health(&self, service: ServiceName) -> HealthStatus {
        self.health
            .get(&service)
            .cloned()
            .unwrap_or_else(|| HealthStatus::unhealthy("not started"))
    }

    #[must_use]
    #[allow(dead_code)]
    pub fn all_healthy(&self) -> bool {
        ServiceName::all()
            .iter()
            .all(|s| self.health.get(s).is_some_and(|h| h.ok))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_json_matches_go_field_names() {
        let h = HealthStatus::healthy();
        let v: serde_json::Value = serde_json::to_value(&h).unwrap();
        assert_eq!(v["ok"], true);
        assert!(v.get("msg").is_none());

        let u = HealthStatus::unhealthy("boom");
        let v: serde_json::Value = serde_json::to_value(&u).unwrap();
        assert_eq!(v["msg"], "boom");
    }

    #[test]
    fn registry_tracks_service_health() {
        let mut r = ServiceRegistry::new();
        assert!(!r.all_healthy());
        for s in ServiceName::all() {
            r.mark_healthy(*s);
        }
        assert!(r.all_healthy());
        r.mark_unhealthy(ServiceName::History, "shard lost");
        assert!(!r.all_healthy());
        assert_eq!(r.health(ServiceName::History).msg, "shard lost");
    }

    #[test]
    fn service_name_parsing() {
        assert_eq!(ServiceName::parse("Frontend"), Some(ServiceName::Frontend));
        assert_eq!(
            ServiceName::parse("shard-distributor"),
            Some(ServiceName::Sharddistributor)
        );
        assert_eq!(ServiceName::parse("nope"), None);
    }
}
