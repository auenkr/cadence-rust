| criterion | verdict | detail |
|---|---|---|
| no panics/crashes | PASS | panic=False died=[] |
| fe_rss_mb_per_h near flat (<= 5 MB/h or within +5 of Go) | FAIL | rust=12.49 go=5.03 |
| mt_rss_mb_per_h near flat (<= 5 MB/h or within +5 of Go) | PASS | rust=4.17 go=1.49 |
| fe_fds_per_h near flat (<= 10 fds/h or within +10 of Go) | PASS | rust=3.36 go=2.25 |
| mt_fds_per_h near flat (<= 10 fds/h or within +10 of Go) | PASS | rust=0.32 go=0.37 |
| every started workflow completes (excluding in-flight at end) | PASS | started=42000 completed=42000 failed=0 in_flight_at_end=0 (go: 42000/42000/0/0) |
| error rate ~0 (< 0.1%) and not higher than Go | PASS | rust=0.0000% (0) go=0 stages={} |
| query "init" retries not above Go (+10%; Go itself retries ~50% of workflows) | PASS | rust workflows=20949 attempts=29966; go workflows=20862 attempts=27978 |
| e2e p50 within 1.5x of Go (+50ms floor) | PASS | rust=3084ms go=3055ms |
| e2e p95 within 1.5x of Go (+50ms floor) | PASS | rust=4091ms go=4059ms |
| e2e p99 within 1.5x of Go (+50ms floor) | PASS | rust=4109ms go=4073ms |
| query call p50 within 1.5x of Go (+50ms floor) | PASS | rust=11ms go=7ms |
| query call p95 within 1.5x of Go (+50ms floor) | PASS | rust=23ms go=21ms |
| query call p99 within 1.5x of Go (+50ms floor) | PASS | rust=31ms go=29ms |
| start call p50 within 1.5x of Go (+50ms floor) | PASS | rust=8ms go=5ms |
| start call p95 within 1.5x of Go (+50ms floor) | PASS | rust=12ms go=7ms |
| start call p99 within 1.5x of Go (+50ms floor) | PASS | rust=21ms go=14ms |

Go baseline sanity: panic=False failed=0 completed=42000/42000 died=[]

**RUST OVERALL: FAIL**
