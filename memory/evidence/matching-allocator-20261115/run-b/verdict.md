| criterion | verdict | detail |
|---|---|---|
| no panics/crashes | PASS | panic=False died=[] |
| fe_rss_mb_per_h near flat (<= 5 MB/h or within +5 of Go) | PASS | rust=8.57 go=4.81 |
| mt_rss_mb_per_h near flat (<= 5 MB/h or within +5 of Go) | FAIL | rust=8.22 go=1.04 |
| fe_fds_per_h near flat (<= 10 fds/h or within +10 of Go) | PASS | rust=-3.07 go=0.12 |
| mt_fds_per_h near flat (<= 10 fds/h or within +10 of Go) | PASS | rust=0.49 go=0.65 |
| every started workflow completes (excluding in-flight at end) | PASS | started=42000 completed=42000 failed=0 in_flight_at_end=0 (go: 42000/42000/0/0) |
| error rate ~0 (< 0.1%) and not higher than Go | PASS | rust=0.0000% (0) go=0 stages={} |
| query "init" retries not above Go (+10%; Go itself retries ~50% of workflows) | PASS | rust workflows=21227 attempts=29956; go workflows=20905 attempts=28341 |
| e2e p50 within 1.5x of Go (+50ms floor) | PASS | rust=3068ms go=3088ms |
| e2e p95 within 1.5x of Go (+50ms floor) | PASS | rust=4083ms go=4094ms |
| e2e p99 within 1.5x of Go (+50ms floor) | PASS | rust=4293ms go=4115ms |
| query call p50 within 1.5x of Go (+50ms floor) | PASS | rust=8ms go=12ms |
| query call p95 within 1.5x of Go (+50ms floor) | PASS | rust=20ms go=34ms |
| query call p99 within 1.5x of Go (+50ms floor) | PASS | rust=64ms go=45ms |
| start call p50 within 1.5x of Go (+50ms floor) | PASS | rust=6ms go=9ms |
| start call p95 within 1.5x of Go (+50ms floor) | PASS | rust=34ms go=13ms |
| start call p99 within 1.5x of Go (+50ms floor) | FAIL | rust=176ms go=19ms |

Go baseline sanity: panic=False failed=0 completed=42000/42000 died=[]

**RUST OVERALL: FAIL**
