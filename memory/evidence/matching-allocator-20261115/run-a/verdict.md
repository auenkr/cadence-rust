| criterion | verdict | detail |
|---|---|---|
| no panics/crashes | PASS | panic=False died=[] |
| fe_rss_mb_per_h near flat (<= 5 MB/h or within +5 of Go) | PASS | rust=5.85 go=5.84 |
| mt_rss_mb_per_h near flat (<= 5 MB/h or within +5 of Go) | FAIL | rust=6.85 go=1.49 |
| fe_fds_per_h near flat (<= 10 fds/h or within +10 of Go) | PASS | rust=-0.57 go=-0.53 |
| mt_fds_per_h near flat (<= 10 fds/h or within +10 of Go) | PASS | rust=0.72 go=-0.45 |
| every started workflow completes (excluding in-flight at end) | PASS | started=42000 completed=42000 failed=0 in_flight_at_end=0 (go: 42000/42000/0/0) |
| error rate ~0 (< 0.1%) and not higher than Go | PASS | rust=0.0000% (0) go=0 stages={} |
| query "init" retries not above Go (+10%; Go itself retries ~50% of workflows) | PASS | rust workflows=21050 attempts=30019; go workflows=21115 attempts=28495 |
| e2e p50 within 1.5x of Go (+50ms floor) | PASS | rust=3084ms go=3072ms |
| e2e p95 within 1.5x of Go (+50ms floor) | PASS | rust=4090ms go=4080ms |
| e2e p99 within 1.5x of Go (+50ms floor) | PASS | rust=4108ms go=4215ms |
| query call p50 within 1.5x of Go (+50ms floor) | PASS | rust=12ms go=9ms |
| query call p95 within 1.5x of Go (+50ms floor) | PASS | rust=23ms go=28ms |
| query call p99 within 1.5x of Go (+50ms floor) | PASS | rust=31ms go=61ms |
| start call p50 within 1.5x of Go (+50ms floor) | PASS | rust=8ms go=7ms |
| start call p95 within 1.5x of Go (+50ms floor) | PASS | rust=12ms go=16ms |
| start call p99 within 1.5x of Go (+50ms floor) | PASS | rust=17ms go=74ms |

Go baseline sanity: panic=False failed=0 completed=42000/42000 died=[]

**RUST OVERALL: FAIL**
