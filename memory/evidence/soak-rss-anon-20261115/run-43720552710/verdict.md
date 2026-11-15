| criterion | verdict | detail |
|---|---|---|
| no panics/crashes | PASS | panic=False died=[] |
| fe_rss_mb_per_h near flat (<= 5 MB/h or within +5 of Go) | PASS | rust=0.94 go=6.30 |
| mt_rss_mb_per_h near flat (<= 5 MB/h or within +5 of Go) | PASS | rust=4.44 go=0.89 |
| fe_fds_per_h near flat (<= 10 fds/h or within +10 of Go) | PASS | rust=0.49 go=1.51 |
| mt_fds_per_h near flat (<= 10 fds/h or within +10 of Go) | PASS | rust=-0.78 go=1.85 |
| every started workflow completes (excluding in-flight at end) | PASS | started=42000 completed=42000 failed=0 in_flight_at_end=0 (go: 42000/42000/0/0) |
| error rate ~0 (< 0.1%) and not higher than Go | PASS | rust=0.0000% (0) go=0 stages={} |
| query "init" retries not above Go (+10%; Go itself retries ~50% of workflows) | PASS | rust workflows=21162 attempts=30212; go workflows=21304 attempts=28969 |
| e2e p50 within 1.5x of Go (+50ms floor) | PASS | rust=3083ms go=3149ms |
| e2e p95 within 1.5x of Go (+50ms floor) | PASS | rust=4089ms go=4507ms |
| e2e p99 within 1.5x of Go (+50ms floor) | PASS | rust=4107ms go=5889ms |
| query call p50 within 1.5x of Go (+50ms floor) | PASS | rust=12ms go=11ms |
| query call p95 within 1.5x of Go (+50ms floor) | PASS | rust=23ms go=166ms |
| query call p99 within 1.5x of Go (+50ms floor) | PASS | rust=31ms go=503ms |
| start call p50 within 1.5x of Go (+50ms floor) | PASS | rust=8ms go=8ms |
| start call p95 within 1.5x of Go (+50ms floor) | PASS | rust=11ms go=162ms |
| start call p99 within 1.5x of Go (+50ms floor) | PASS | rust=17ms go=362ms |

Go baseline sanity: panic=False failed=0 completed=42000/42000 died=[]

**RUST OVERALL: PASS**
