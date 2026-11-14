| criterion | verdict | detail |
|---|---|---|
| no panics/crashes | PASS | panic=False died=[] |
| fe_rss_mb_per_h near flat (<= 5 MB/h or within +5 of Go) | PASS | rust=9.39 go=4.80 |
| mt_rss_mb_per_h near flat (<= 5 MB/h or within +5 of Go) | PASS | rust=4.01 go=1.29 |
| fe_fds_per_h near flat (<= 10 fds/h or within +10 of Go) | PASS | rust=2.69 go=-0.33 |
| mt_fds_per_h near flat (<= 10 fds/h or within +10 of Go) | PASS | rust=0.21 go=0.85 |
| every started workflow completes (excluding in-flight at end) | PASS | started=42000 completed=42000 failed=0 in_flight_at_end=0 (go: 42000/42000/0/0) |
| error rate ~0 (< 0.1%) and not higher than Go | PASS | rust=0.0000% (0) go=0 stages={} |
| query "init" retries not above Go (+10%; Go itself retries ~50% of workflows) | PASS | rust workflows=21108 attempts=30241; go workflows=20798 attempts=28038 |
| e2e p50 within 1.5x of Go (+50ms floor) | PASS | rust=3050ms go=3054ms |
| e2e p95 within 1.5x of Go (+50ms floor) | PASS | rust=4054ms go=4057ms |
| e2e p99 within 1.5x of Go (+50ms floor) | PASS | rust=4068ms go=4068ms |
| query call p50 within 1.5x of Go (+50ms floor) | PASS | rust=7ms go=8ms |
| query call p95 within 1.5x of Go (+50ms floor) | PASS | rust=13ms go=20ms |
| query call p99 within 1.5x of Go (+50ms floor) | PASS | rust=20ms go=28ms |
| start call p50 within 1.5x of Go (+50ms floor) | PASS | rust=5ms go=6ms |
| start call p95 within 1.5x of Go (+50ms floor) | PASS | rust=6ms go=7ms |
| start call p99 within 1.5x of Go (+50ms floor) | PASS | rust=18ms go=10ms |

Go baseline sanity: panic=False failed=0 completed=42000/42000 died=[]

**RUST OVERALL: PASS**
