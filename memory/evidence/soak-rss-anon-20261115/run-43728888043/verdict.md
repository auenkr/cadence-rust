| criterion | verdict | detail |
|---|---|---|
| no panics/crashes | PASS | panic=False died=[] |
| fe_rss_mb_per_h near flat (<= 5 MB/h or within +5 of Go) | PASS | rust=1.02 go=5.50 |
| mt_rss_mb_per_h near flat (<= 5 MB/h or within +5 of Go) | PASS | rust=4.79 go=0.98 |
| fe_fds_per_h near flat (<= 10 fds/h or within +10 of Go) | PASS | rust=0.17 go=3.26 |
| mt_fds_per_h near flat (<= 10 fds/h or within +10 of Go) | PASS | rust=-0.33 go=-0.02 |
| every started workflow completes (excluding in-flight at end) | PASS | started=42000 completed=42000 failed=0 in_flight_at_end=0 (go: 42000/42000/0/0) |
| error rate ~0 (< 0.1%) and not higher than Go | PASS | rust=0.0000% (0) go=0 stages={} |
| query "init" retries not above Go (+10%; Go itself retries ~50% of workflows) | PASS | rust workflows=21108 attempts=30175; go workflows=20953 attempts=28250 |
| e2e p50 within 1.5x of Go (+50ms floor) | PASS | rust=3082ms go=3052ms |
| e2e p95 within 1.5x of Go (+50ms floor) | PASS | rust=4088ms go=4054ms |
| e2e p99 within 1.5x of Go (+50ms floor) | PASS | rust=4107ms go=4065ms |
| query call p50 within 1.5x of Go (+50ms floor) | PASS | rust=11ms go=7ms |
| query call p95 within 1.5x of Go (+50ms floor) | PASS | rust=23ms go=19ms |
| query call p99 within 1.5x of Go (+50ms floor) | PASS | rust=30ms go=26ms |
| start call p50 within 1.5x of Go (+50ms floor) | PASS | rust=8ms go=5ms |
| start call p95 within 1.5x of Go (+50ms floor) | PASS | rust=11ms go=6ms |
| start call p99 within 1.5x of Go (+50ms floor) | PASS | rust=17ms go=9ms |

Go baseline sanity: panic=False failed=0 completed=42000/42000 died=[]

**RUST OVERALL: PASS**
