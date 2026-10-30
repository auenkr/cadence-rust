'use strict';
// Opt-in (CADENCE_MATCHING_PROC_EVIDENCE=1), CI-only, bounded snapshot of a
// Rust Matching process taken once at the first direct readiness RPC failure.
// It separates a process-wide stall (threads blocked or spinning, listener
// accept queue growing) from a listener-only problem. Fixed argv, no shell,
// no environment or command-line capture. gdb is attempted only when
// CADENCE_MATCHING_PROC_EVIDENCE_GDB=1 (attach briefly stops the process).
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const MAX_THREADS = 64;
const MAX_TEXT = 8192;
const MAX_GDB_TEXT = 24576;
const CMD_TIMEOUT_MS = 3000;
const GDB_TIMEOUT_MS = 20000;
const CPU_SAMPLE_GAP_MS = 500;

const clip = (text, limit) => {
  const value = String(text || '');
  return value.length > limit ? `${value.slice(0, limit)}\n[truncated ${value.length - limit} chars]` : value;
};
const readProc = file => {
  try { return fs.readFileSync(file, 'utf8').trim(); } catch (error) { return `unavailable:${error.code || 'ERR'}`; }
};
// /proc/<pid>/task/<tid>/stat: "tid (comm) state ..."; comm may contain spaces.
const parseStat = text => {
  const close = text.lastIndexOf(')');
  if (close < 0) return { state: '?', utime: 0, stime: 0 };
  const fields = text.slice(close + 2).split(' ');
  return { state: fields[0], utime: Number(fields[11]) || 0, stime: Number(fields[12]) || 0 };
};
const threadSnapshot = pid => {
  let tids = [];
  try { tids = fs.readdirSync(`/proc/${pid}/task`).sort((a, b) => a - b); } catch (_) { /* process gone */ }
  return tids.slice(0, MAX_THREADS).map(tid => {
    const base = `/proc/${pid}/task/${tid}`;
    return { tid: Number(tid), comm: readProc(`${base}/comm`), wchan: readProc(`${base}/wchan`),
      ...parseStat(readProc(`${base}/stat`)) };
  });
};
const run = (argv, timeoutMs, limit) => {
  const result = spawnSync(argv[0], argv.slice(1), {
    encoding: 'utf8', timeout: timeoutMs, killSignal: 'SIGKILL', maxBuffer: 256 * 1024,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  return { argv, status: result.status, signal: result.signal,
    error: result.error ? result.error.code || result.error.message : null,
    stdout: clip(result.stdout, limit), stderr: clip(result.stderr, 1024) };
};

async function captureProcessEvidence({ pid, grpcPort, workDir, label, reason }) {
  const startedAt = new Date().toISOString();
  const first = threadSnapshot(pid);
  await new Promise(resolve => setTimeout(resolve, CPU_SAMPLE_GAP_MS));
  const second = threadSnapshot(pid);
  const before = new Map(first.map(thread => [thread.tid, thread]));
  const threads = second.map(thread => {
    const prior = before.get(thread.tid);
    return { ...thread, cpuTicksDuringGap: prior ? (thread.utime + thread.stime) - (prior.utime + prior.stime) : null };
  });
  const port = String(Number(grpcPort));
  const evidence = {
    label, reason: clip(reason, 512), startedAt, rustPid: pid, grpcPort: Number(port),
    cpuSampleGapMs: CPU_SAMPLE_GAP_MS, threadCap: MAX_THREADS,
    processStatus: readProc(`/proc/${pid}/status`).split('\n')
      .filter(line => /^(State|Threads|VmRSS|voluntary_ctxt_switches|nonvoluntary_ctxt_switches):/.test(line)),
    threads,
    // Listener (Recv-Q = accept backlog depth) and per-connection states for the gRPC port only.
    listenerQueues: run(['ss', '-ltn', 'sport', '=', `:${port}`], CMD_TIMEOUT_MS, MAX_TEXT),
    connections: run(['ss', '-tn', 'sport', '=', `:${port}`], CMD_TIMEOUT_MS, MAX_TEXT),
    gdb: 'not requested',
  };
  if (process.env.CADENCE_MATCHING_PROC_EVIDENCE_GDB === '1') {
    const which = run(['which', 'gdb'], CMD_TIMEOUT_MS, 256);
    evidence.gdb = which.status === 0
      ? run(['gdb', '-p', String(pid), '-batch', '-nx', '-ex', 'set pagination off',
        '-ex', 'thread apply all bt 12'], GDB_TIMEOUT_MS, MAX_GDB_TEXT)
      : 'gdb not available';
  }
  evidence.finishedAt = new Date().toISOString();
  fs.writeFileSync(path.join(workDir, `rust-process-evidence-${label}.json`), `${JSON.stringify(evidence, null, 2)}\n`);
  return evidence;
}

module.exports = { captureProcessEvidence };
