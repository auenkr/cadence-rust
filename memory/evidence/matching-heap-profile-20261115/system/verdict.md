| criterion | verdict | detail |
|---|---|---|
| no panics/crashes | PASS | panic=False died=[] |
| fe_rss_mb_per_h near flat (<= 5 MB/h or within +5 of Go) | PASS | rust=0.54 go=5.58 |
| mt_rss_mb_per_h near flat (<= 5 MB/h or within +5 of Go) | FAIL | rust=6.37 go=0.92 |
| fe_fds_per_h near flat (<= 10 fds/h or within +10 of Go) | PASS | rust=-0.27 go=0.01 |
| mt_fds_per_h near flat (<= 10 fds/h or within +10 of Go) | PASS | rust=0.34 go=0.70 |
| every started workflow completes (excluding in-flight at end) | PASS | started=42000 completed=42000 failed=0 in_flight_at_end=0 (go: 42000/42000/0/0) |
| error rate ~0 (< 0.1%) and not higher than Go | PASS | rust=0.0000% (0) go=0 stages={} |
| query "init" retries not above Go (+10%; Go itself retries ~50% of workflows) | PASS | rust workflows=21055 attempts=30252; go workflows=21042 attempts=28843 |
| e2e p50 within 1.5x of Go (+50ms floor) | PASS | rust=3087ms go=3071ms |
| e2e p95 within 1.5x of Go (+50ms floor) | PASS | rust=4093ms go=4075ms |
| e2e p99 within 1.5x of Go (+50ms floor) | PASS | rust=4115ms go=4091ms |
| query call p50 within 1.5x of Go (+50ms floor) | PASS | rust=12ms go=10ms |
| query call p95 within 1.5x of Go (+50ms floor) | PASS | rust=24ms go=26ms |
| query call p99 within 1.5x of Go (+50ms floor) | PASS | rust=31ms go=36ms |
| start call p50 within 1.5x of Go (+50ms floor) | PASS | rust=8ms go=7ms |
| start call p95 within 1.5x of Go (+50ms floor) | PASS | rust=13ms go=9ms |
| start call p99 within 1.5x of Go (+50ms floor) | PASS | rust=18ms go=16ms |

Go baseline sanity: panic=False failed=0 completed=42000/42000 died=[]

**RUST OVERALL: FAIL**
