#[tokio::main]
async fn main() -> anyhow::Result<()> {
    cadence_matching::launcher::run("cadence-server", None).await
}
