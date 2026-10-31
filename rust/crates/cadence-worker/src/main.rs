fn main() -> anyhow::Result<()> {
    cadence_worker::launcher::run("cadence-worker")
}
