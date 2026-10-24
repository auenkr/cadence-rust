//! Production Matching metrics exporter selection and lifecycle.
//!
//! This follows `common/config.Metrics.NewScope`: at most one exporter is
//! accepted, and the shared prefix/tags/reporting interval configure its root.

use crate::{
    config::MetricsConfig,
    matching_metrics::{MetricSink, NoopMetricSink},
    matching_metrics_migration::{MigrationMetricSink, MigrationPolicy},
};
use std::{io, sync::Arc, time::Duration};

enum Exporter {
    Statsd(Arc<crate::matching_metrics_statsd::StatsdMetricSink>),
    M3(Arc<crate::matching_metrics_m3::M3MetricSink>),
    Prometheus(Arc<crate::matching_metrics_prometheus::PrometheusMetricSink>),
}

pub struct MatchingMetricsRuntime {
    sink: Arc<dyn MetricSink>,
    exporter: Option<Exporter>,
    listener: Option<crate::matching_metrics_prometheus::PrometheusListener>,
}

impl MatchingMetricsRuntime {
    pub fn new(
        config: &MetricsConfig,
        pprof: Option<&crate::config::PProfConfig>,
        policy: MigrationPolicy,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        policy.validate()?;
        let mut root_tags = config.tags.clone();
        root_tags.insert("cadence_service".into(), "cadence-matching".into());
        let interval = Duration::from_nanos(if config.reporting_interval_nanos > 0 {
            config.reporting_interval_nanos as u64
        } else {
            1_000_000_000
        });
        let mut exporter = if let Some(m3) = &config.m3 {
            Some(Exporter::M3(Arc::new(
                crate::matching_metrics_m3::M3MetricSink::new(
                    m3.clone(),
                    config.prefix.clone(),
                    root_tags.clone(),
                    interval,
                )?,
            )))
        } else if let Some(statsd) = &config.statsd {
            // Go returns NoopScope for an empty StatsD host, even though the
            // StatsD reporter block itself is configured.
            if statsd.host_port.trim().is_empty() {
                None
            } else {
                let mut statsd_config = statsd.clone();
                statsd_config.reporting_interval = interval;
                Some(Exporter::Statsd(Arc::new(
                    crate::matching_metrics_statsd::StatsdMetricSink::new(
                        statsd_config,
                        config.prefix.clone(),
                        root_tags.clone(),
                    )?,
                )))
            }
        } else {
            None
        };
        if exporter.is_none() {
            if let Some(prom) = &config.prometheus {
                let mut prom_config = prom.clone();
                // Go registers an address-less reporter on the default mux;
                // the service PProf listener is what serves that mux.
                if prom_config.listen_address.trim().is_empty() {
                    if let Some(pprof) = pprof.filter(|pprof| pprof.port > 0) {
                        let host = if pprof.host.trim().is_empty() {
                            "localhost"
                        } else {
                            pprof.host.trim()
                        };
                        let host_port = if host.parse::<std::net::Ipv6Addr>().is_ok() {
                            format!("[{host}]:{}", pprof.port)
                        } else {
                            format!("{host}:{}", pprof.port)
                        };
                        prom_config.listen_address = host_port;
                        prom_config.listen_network = "tcp".into();
                    }
                }
                exporter = Some(Exporter::Prometheus(Arc::new(
                    crate::matching_metrics_prometheus::PrometheusMetricSink::new(
                        prom_config,
                        config.prefix.clone(),
                        root_tags.clone(),
                    )?,
                )));
            }
        }
        let sink: Arc<dyn MetricSink> = match &exporter {
            Some(Exporter::Statsd(sink)) => sink.clone(),
            Some(Exporter::M3(sink)) => sink.clone(),
            Some(Exporter::Prometheus(sink)) => sink.clone(),
            None => Arc::new(NoopMetricSink),
        };
        let sink: Arc<dyn MetricSink> = Arc::new(MigrationMetricSink::new(sink, policy));
        Ok(Self {
            sink,
            exporter,
            listener: None,
        })
    }

    pub fn sink(&self) -> Arc<dyn MetricSink> {
        self.sink.clone()
    }

    pub async fn start(&mut self) -> io::Result<()> {
        if let Some(Exporter::Prometheus(sink)) = &self.exporter {
            match sink.start_listener().await {
                Ok(listener) => self.listener = listener,
                Err(error) => eprintln!("error in prometheus reporter: {error}"),
            }
        }
        Ok(())
    }

    pub async fn shutdown(&mut self) -> anyhow::Result<()> {
        if let Some(listener) = self.listener.take() {
            listener.shutdown().await?;
        }
        self.sink.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_statsd_host_uses_the_go_noop_scope() {
        let config: MetricsConfig = serde_yaml::from_str("statsd: {hostPort: ''}").unwrap();
        let runtime = MatchingMetricsRuntime::new(&config, None, MigrationPolicy::default()).unwrap();
        assert!(runtime.exporter.is_none());
    }

    #[test]
    fn empty_statsd_scope_allows_prometheus_fallback() {
        let config: MetricsConfig = serde_yaml::from_str(
            "statsd: {hostPort: ''}\nprometheus: {listenAddress: '127.0.0.1:0'}",
        )
        .unwrap();
        let runtime = MatchingMetricsRuntime::new(&config, None, MigrationPolicy::default()).unwrap();
        assert!(matches!(&runtime.exporter, Some(Exporter::Prometheus(_))));
    }

    #[tokio::test]
    async fn prometheus_listener_is_owned_and_stopped_by_runtime() {
        let config: MetricsConfig =
            serde_yaml::from_str("prometheus: {listenAddress: '127.0.0.1:0'}").unwrap();
        let mut runtime = MatchingMetricsRuntime::new(&config, None, MigrationPolicy::default()).unwrap();
        runtime.start().await.unwrap();
        assert!(runtime.listener.is_some());
        runtime.shutdown().await.unwrap();
        assert!(runtime.listener.is_none());
    }
}
