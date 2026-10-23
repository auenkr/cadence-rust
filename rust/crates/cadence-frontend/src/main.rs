fn main() -> anyhow::Result<()> {
    cadence_service_core::cli::not_migrated("cadence-frontend", cadence_frontend::SERVICE_NAME)
}
