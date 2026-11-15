| criterion | verdict | detail |
|---|---|---|
| no panics/crashes | PASS | panic=False died=[] |
| fe_rss_mb_per_h near flat (<= 5 MB/h or within +5 of Go) | PASS | rust=2.00 go=5.16 |
| mt_rss_mb_per_h near flat (<= 5 MB/h or within +5 of Go) | PASS | rust=5.47 go=1.18 |
| fe_fds_per_h near flat (<= 10 fds/h or within +10 of Go) | PASS | rust=0.05 go=4.04 |
| mt_fds_per_h near flat (<= 10 fds/h or within +10 of Go) | PASS | rust=0.78 go=6.21 |
| every started workflow completes (excluding in-flight at end) | PASS | started=42000 completed=42000 failed=0 in_flight_at_end=0 (go: 42000/42000/0/0) |
| error rate ~0 (< 0.1%) and not higher than Go | PASS | rust=0.0000% (0) go=0 stages={} |
| query "init" retries not above Go (+10%; Go itself retries ~50% of workflows) | PASS | rust workflows=21040 attempts=29870; go workflows=20973 attempts=28303 |
| e2e p50 within 1.5x of Go (+50ms floor) | PASS | rust=3079ms go=3058ms |
| e2e p95 within 1.5x of Go (+50ms floor) | PASS | rust=4085ms go=4065ms |
| e2e p99 within 1.5x of Go (+50ms floor) | PASS | rust=4100ms go=4108ms |
| query call p50 within 1.5x of Go (+50ms floor) | PASS | rust=11ms go=7ms |
| query call p95 within 1.5x of Go (+50ms floor) | PASS | rust=22ms go=24ms |
| query call p99 within 1.5x of Go (+50ms floor) | PASS | rust=29ms go=50ms |
| start call p50 within 1.5x of Go (+50ms floor) | PASS | rust=7ms go=6ms |
| start call p95 within 1.5x of Go (+50ms floor) | PASS | rust=9ms go=12ms |
| start call p99 within 1.5x of Go (+50ms floor) | PASS | rust=15ms go=41ms |

Go baseline sanity: panic=False failed=0 completed=42000/42000 died=[]

**RUST OVERALL: PASS**
