fn main() -> anyhow::Result<()> {
    cadence_service_core::cli::not_migrated("cadence-worker", cadence_worker::SERVICE_NAME)
}
