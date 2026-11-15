| criterion | verdict | detail |
|---|---|---|
| no panics/crashes | PASS | panic=False died=[] |
| fe_rss_mb_per_h near flat (<= 5 MB/h or within +5 of Go) | PASS | rust=1.09 go=4.13 |
| mt_rss_mb_per_h near flat (<= 5 MB/h or within +5 of Go) | FAIL | rust=11.15 go=1.50 |
| fe_fds_per_h near flat (<= 10 fds/h or within +10 of Go) | FAIL | rust=-10.96 go=-0.44 |
| mt_fds_per_h near flat (<= 10 fds/h or within +10 of Go) | PASS | rust=-0.94 go=-2.07 |
| every started workflow completes (excluding in-flight at end) | PASS | started=42000 completed=42000 failed=0 in_flight_at_end=0 (go: 42000/42000/0/0) |
| error rate ~0 (< 0.1%) and not higher than Go | PASS | rust=0.0000% (0) go=0 stages={} |
| query "init" retries not above Go (+10%; Go itself retries ~50% of workflows) | PASS | rust workflows=21050 attempts=29689; go workflows=21039 attempts=28371 |
| e2e p50 within 1.5x of Go (+50ms floor) | PASS | rust=3078ms go=3090ms |
| e2e p95 within 1.5x of Go (+50ms floor) | PASS | rust=4180ms go=4096ms |
| e2e p99 within 1.5x of Go (+50ms floor) | PASS | rust=4902ms go=4117ms |
| query call p50 within 1.5x of Go (+50ms floor) | PASS | rust=8ms go=12ms |
| query call p95 within 1.5x of Go (+50ms floor) | PASS | rust=27ms go=34ms |
| query call p99 within 1.5x of Go (+50ms floor) | FAIL | rust=158ms go=45ms |
| start call p50 within 1.5x of Go (+50ms floor) | PASS | rust=6ms go=9ms |
| start call p95 within 1.5x of Go (+50ms floor) | FAIL | rust=106ms go=14ms |
| start call p99 within 1.5x of Go (+50ms floor) | FAIL | rust=275ms go=20ms |

Go baseline sanity: panic=False failed=0 completed=42000/42000 died=[]

**RUST OVERALL: FAIL**
