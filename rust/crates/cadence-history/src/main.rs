fn main() -> anyhow::Result<()> {
    cadence_service_core::cli::not_migrated("cadence-history", cadence_history::SERVICE_NAME)
}
