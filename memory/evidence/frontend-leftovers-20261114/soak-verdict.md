| criterion | verdict | detail |
|---|---|---|
| no panics/crashes | PASS | panic=False died=[] |
| fe_rss_mb_per_h near flat (<= 5 MB/h or within +5 of Go) | PASS | rust=9.60 go=12.60 |
| mt_rss_mb_per_h near flat (<= 5 MB/h or within +5 of Go) | PASS | rust=16.07 go=18.77 |
| fe_fds_per_h near flat (<= 10 fds/h or within +10 of Go) | PASS | rust=-6.86 go=6.00 |
| mt_fds_per_h near flat (<= 10 fds/h or within +10 of Go) | PASS | rust=23.50 go=35.07 |
| every started workflow completes (excluding in-flight at end) | PASS | started=12000 completed=12000 failed=0 in_flight_at_end=0 (go: 12000/12000/0/0) |
| error rate ~0 (< 0.1%) and not higher than Go | PASS | rust=0.0000% (0) go=0 stages={} |
| query "init" after successful signal: none (as Go) | PASS | rust workflows=5982 attempts=8099; go workflows=5999 attempts=8198 |
| e2e p50 within 1.5x of Go (+50ms floor) | PASS | rust=3062ms go=3067ms |
| e2e p95 within 1.5x of Go (+50ms floor) | PASS | rust=4067ms go=4075ms |
| e2e p99 within 1.5x of Go (+50ms floor) | PASS | rust=4105ms go=4475ms |
| query call p50 within 1.5x of Go (+50ms floor) | PASS | rust=9ms go=9ms |
| query call p95 within 1.5x of Go (+50ms floor) | PASS | rust=24ms go=27ms |
| query call p99 within 1.5x of Go (+50ms floor) | PASS | rust=42ms go=103ms |
| start call p50 within 1.5x of Go (+50ms floor) | PASS | rust=6ms go=6ms |
| start call p95 within 1.5x of Go (+50ms floor) | PASS | rust=8ms go=16ms |
| start call p99 within 1.5x of Go (+50ms floor) | PASS | rust=17ms go=131ms |

Go baseline sanity: panic=False failed=0 completed=12000/12000 died=[]

**RUST OVERALL: PASS**
