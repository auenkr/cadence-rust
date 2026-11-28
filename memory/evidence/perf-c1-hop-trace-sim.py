#!/usr/bin/env python3
"""Closed-loop c1 model: one workflow at a time; each workflow is hashed to one of 4 shards and needs 3 sequential
transfer-queue permits from that shard's token bucket (rate 20/s, burst 20, x/time/rate semantics). Service time per
workflow without waits = base_ms (Rust ~25 ms, Go ~40 ms, the measured unthrottled p50s). Prints fraction of permits
that waited and the latency p50/p90/p95 of workflows."""
import random, sys
def run(base_ms, n=200000, seed=1, rate=20.0, burst=20.0, shards=4, permits=3):
    rnd = random.Random(seed); tok=[burst]*shards; last=[0.0]*shards; t=0.0; waited=0; lat=[]
    for _ in range(n):
        s = rnd.randrange(shards); start=t
        for p in range(permits):
            t += base_ms/permits/1000
            tok[s] = min(burst, tok[s] + (t-last[s])*rate); last[s]=t
            if tok[s] >= 1: tok[s] -= 1
            else:
                w = (1-tok[s])/rate; t += w; waited += 1; tok[s]=0; last[s]=t
        lat.append((t-start)*1000)
    lat.sort(); q=lambda x: lat[int(x*len(lat))]
    return waited/(n*permits), n/t, q(.5), q(.9), q(.95)
for name, base in (("go-like", float(sys.argv[1]) if len(sys.argv)>1 else 42.0), ("rust-like", float(sys.argv[2]) if len(sys.argv)>2 else 27.0)):
    w, wfs, p50, p90, p95 = run(base)
    print(f"{name}: base={base}ms waited={w:.3f} wf/s={wfs:.1f} p50={p50:.1f} p90={p90:.1f} p95={p95:.1f}")
