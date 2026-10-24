# Active offer with original Go History

Run: https://github.com/auenkr/cadence-rust/actions/runs/40104204435
Source: c1426544dba42035122ba925a18c1e90534e8f5a. Conclusion: success.

Go Frontend/History/Worker, Rust Matching only. The probe registers a decision
poller before scheduling a real workflow through Go Frontend. It validates the
task token and workflow/run identity, and a scoped MySQL trigger records zero
inserts for this task list, excluding persisted fallback before consumption.
Fresh binary SHA256, build/gate exits and full probe output are preserved here.
Probe implementation merged into master, but this run validates its worker
source only; it is not final integrated Matching acceptance.
