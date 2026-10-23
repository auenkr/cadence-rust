use cadence_service_core::service::ServiceName;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    cadence_matching::launcher::run("cadence-matching", Some(&[ServiceName::Matching])).await
}
