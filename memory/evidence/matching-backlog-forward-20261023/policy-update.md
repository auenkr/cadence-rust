# Host build policy update during validation

The user replaced the original two-lock/-j2 policy with ONE host-wide Cargo job using `flock /tmp/cargo.lock cargo ... -j3`. Separate cargo check is no longer required; final clippy then workspace tests cover validation. No Cargo runs during mixed-cluster gates. Wait if load >6 or MemAvailable <2 GiB; poll no more frequently than once per 60 seconds.

This worker stopped its obsolete queued check sequence immediately. At that point it had not compiled (check.log only said "Blocking waiting for file lock on build directory"). No result is claimed for that interrupted check. Initial actual shell-recorded statuses remain in initial/: check 0, clippy 101 (WIP callback complexity, fixed), stale test 143 (intentionally terminated before tests executed). The final clippy/test evidence will supersede these initial results.
