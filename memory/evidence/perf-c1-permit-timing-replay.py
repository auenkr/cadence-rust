import re, sys, json, collections, datetime as dt, statistics as st
D = sys.argv[1]  # paired-benchmark dir
def ts(s):
    s = s.rstrip('Z')
    if '.' in s:
        a, b = s.split('.'); b = (b + '000000')[:6]; s = a + '.' + b
    return dt.datetime.fromisoformat(s).replace(tzinfo=dt.timezone.utc).timestamp()
def window(arm, rep):
    f = lambda e: dt.datetime.fromisoformat(open(f"{D}/measurement-{rep:02d}-{arm}-c1.{e}").read().strip()[:26].rstrip('Z')).replace(tzinfo=dt.timezone.utc).timestamp()
    return f('start'), f('end')
def sim(reqs, scale=1.0, rate=20.0, burst=20):
    # reqs sorted request times; scale stretches time (>1 = slower arrivals)
    tokens, last, waited, tot = burst, None, 0, 0.0
    t0 = reqs[0]; out = []
    for r in reqs:
        now = t0 + (r - t0) * scale
        if last is not None:
            tokens = min(burst, tokens + (now - last) * rate)
        last = now
        tokens -= 1
        w = -tokens / rate if tokens < 0 else 0
        # reservation at 'now'; later arrivals see negative tokens (golang rate semantic)
        out.append(w)
    n = len(out)
    return sum(1 for w in out if w > 0.001) / n, sum(out) / n
res = collections.defaultdict(list)
for rep in range(5):
    for arm in ('go', 'rust'):
        s, e = window(arm, rep)
        per = collections.defaultdict(list)
        for line in open(f"{D}/services-{rep:02d}-{arm}.log", errors='replace'):
            if 'DTRACE queuev2 load' not in line or 'transfer-queue' not in line:
                continue
            if arm == 'go':
                m = re.search(r'"ts":"([^"]+)".*"shard-id":(\d+).*"wait_ms":([0-9.e+-]+)', line)
                t, sh, w = ts(m.group(1)), m.group(2), float(m.group(3))
            else:
                m = re.search(r'(\S+Z)\s+INFO.*wait_ms=([0-9.]+) limiter=(\w+)', line)
                t, sh, w = ts(m.group(1)), m.group(3), float(m.group(2))
            if s <= t <= e:
                per[sh].append((t - w / 1000.0, w))
        allr = []
        for sh, v in per.items():
            v.sort(); allr.append(v)
        n = sum(len(v) for v in allr)
        waited = sum(1 for v in allr for _, w in v if w > 1) / n
        dur = e - s
        row = dict(arm=arm, rep=rep, permits=n, rate_per_shard=n / dur / max(len(allr), 1), waited=waited,
                   meanwait=sum(w for v in allr for _, w in v) / n)
        for sc in (1.0, 1.15, 1.3):
            rs = [sim([t for t, _ in v], sc) for v in allr]
            tot = sum(len(v) for v in allr)
            row[f'sim{sc}_waited'] = sum(r[0] * len(v) for r, v in zip(rs, allr)) / tot
            row[f'sim{sc}_meanwait_ms'] = 1000 * sum(r[1] * len(v) for r, v in zip(rs, allr)) / tot
        res[arm].append(row)
        print(json.dumps({k: (round(x, 3) if isinstance(x, float) else x) for k, x in row.items()}))
for arm in res:
    print(arm, {k: round(st.median(r[k] for r in res[arm]), 3) for k in res[arm][0] if k not in ('arm', 'rep')})
