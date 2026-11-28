#!/usr/bin/env python3
"""perf-c1-hop-trace: per-hop latency from HOPTRACE (frontend) + DTRACE (history) lines.

Usage: perf-c1-hop-trace-analysis.py <paired-benchmark dir>
Uses services-NN-rust.log (all reps). Cross-process events are joined by wall-clock epoch_ns; at c1 only one
workflow is in flight, so the frontend events (no ids) are matched to the nearest history event of that workflow.
Also reports queuev2 limiter waits per permit for both arms (Go via go-queuev2-trace.patch).
"""
import bisect, glob, re, statistics, sys
from collections import defaultdict

D = sys.argv[1]
EV = re.compile(r"epoch_ns=(\d+).*?stage=(\S+)(?: method=(\S+))?.*? wf=(\S+) run=(\S+)")
WAIT = re.compile(r"queuev2 load(?: wait_ms=|\".*?\"wait_ms\":)([0-9.]+)")


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))] if xs else float("nan")


hops = defaultdict(list)
waits = {"rust": [], "go": []}
for log in sorted(glob.glob(f"{D}/services-*-*.log")):
    arm = "rust" if log.endswith("-rust.log") else "go"
    fe = defaultdict(list)  # (stage,method) -> sorted epochs
    wf_ev = defaultdict(lambda: defaultdict(list))
    for line in open(log, errors="replace"):
        m = WAIT.search(line)
        if m and "DTRACE" in line:
            waits[arm].append(float(m.group(1)))
        if arm != "rust" or ("HOPTRACE" not in line and "DTRACE epoch" not in line):
            continue
        m = EV.search(line)
        if not m:
            continue
        t = int(m.group(1)) / 1e6
        stage, method, wf = m.group(2), m.group(3), m.group(4)
        if "HOPTRACE svc=frontend" in line:
            fe[(stage, method)].append(t)
        elif wf != "-":
            wf_ev[wf][stage].append(t)
    if arm != "rust":
        continue
    for k in fe:
        fe[k].sort()

    def nxt(key, t):
        xs = fe[key]
        i = bisect.bisect_left(xs, t)
        return xs[i] if i < len(xs) else None

    def prev(key, t):
        xs = fe[key]
        i = bisect.bisect_right(xs, t) - 1
        return xs[i] if i >= 0 else None

    for wf, st in wf_ev.items():
        if len(st.get("c0_transfer_task_read", [])) != 2 or len(st.get("g_record_started_returned", [])) != 2:
            continue
        c0, c, d, e, g = (sorted(st[s]) for s in ("c0_transfer_task_read", "c_transfer_picked_up",
                                                    "d_add_decision_sent", "e_record_started_received",
                                                    "g_record_started_returned"))
        a, b = st["a_decision_scheduled_committed"][0], st["b_transfer_notified"][0]
        s_sent = prev(("fe_out_sent", "StartWorkflowExecution"), c0[0])
        s_ret = nxt(("fe_out_returned", "StartWorkflowExecution"), s_sent) if s_sent else None
        if s_sent is None:
            continue
        hops["1 FE start sent -> FE start returned (history start)"].append(s_ret - s_sent)
        hops["2 FE start sent -> transfer task1 read (commit+notify+queue load incl. limiter)"].append(c0[0] - s_sent)
        lw = st.get("c1_queue_rate_limiter_wait", [])
        for k in range(2):
            hops[f"3.{k+1} task{k+1} read -> picked up"].append(c[k] - c0[k])
            hops[f"4.{k+1} task{k+1} picked up -> AddDecision sent"].append(d[k] - c[k])
            hops[f"5.{k+1} task{k+1} AddDecision sent -> RecordStarted recv (matching match+dispatch)"].append(e[k] - d[k])
            hops[f"6.{k+1} task{k+1} RecordStarted handler"].append(g[k] - e[k])
            p_ret = nxt(("fe_out_returned", "PollForDecisionTask"), g[k])
            r_sent = nxt(("fe_out_sent", "RespondDecisionTaskCompleted"), p_ret) if p_ret else None
            r_ret = nxt(("fe_out_returned", "RespondDecisionTaskCompleted"), r_sent) if r_sent else None
            if p_ret and r_sent and r_ret:
                hops[f"7.{k+1} task{k+1} RecordStarted returned -> FE poll returned (matching->FE)"].append(p_ret - g[k])
                hops[f"8.{k+1} task{k+1} FE poll returned -> FE respond sent (SDK worker)"].append(r_sent - p_ret)
                hops[f"9.{k+1} task{k+1} FE respond sent -> returned (history respond)"].append(r_ret - r_sent)
                if k == 0:
                    hops["10 respond1 returned -> decision2 committed (a)"].append(a - r_ret)
                else:
                    hops["13 FE start sent -> respond2 returned (traced server-side workflow span)"].append(r_ret - s_sent)
        hops["11 decision2 committed -> notified (a->b)"].append(b - a)
        hops["12 decision2 notified -> transfer task2 read (b->c0, incl. limiter)"].append(c0[1] - b)
        hops["L per-workflow limiter-wait events >5ms (count)"].append(len(lw))

print(f"{'hop':<92}{'n':>6}{'p50':>9}{'p90':>9}{'p95':>9}{'>=40ms':>8}")
for k in sorted(hops, key=lambda s: [float(x) if x.replace('.', '').isdigit() else 99 for x in s.split()[0].split('.')]):
    xs = hops[k]
    print(f"{k:<92}{len(xs):>6}{pct(xs,.5):>9.2f}{pct(xs,.9):>9.2f}{pct(xs,.95):>9.2f}{sum(x>=40 for x in xs)/len(xs):>8.3f}")
for arm, xs in waits.items():
    if xs:
        w = [x for x in xs if x > 1]
        print(f"limiter {arm}: permits={len(xs)} waited>1ms={len(w)/len(xs):.3f} wait p50={pct(w,.5):.1f} p95={pct(w,.95):.1f}")
