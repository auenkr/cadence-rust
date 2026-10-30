// Go Frontend/History/Worker routed to the Rust Matching protobuf listener.
const assert = require('node:assert/strict');
const path = require('node:path');
const { randomUUID, X509Certificate } = require('node:crypto');
const fs = require('node:fs');
const { spawn, spawnSync } = require('node:child_process');
const { createRequire } = require('node:module');
const tls = require('node:tls');
const { createTaskObserver } = require('./matching-task-observer.cjs');
const { runProbeScenarios } = require('./matching-probe-scenarios.cjs');
const webRequire = createRequire(path.resolve(__dirname, '../cadence-web/package.json'));
const grpc = webRequire('@grpc/grpc-js');
const loader = webRequire('@grpc/proto-loader');
const root = path.resolve(__dirname, '../go/idls/proto');
const files = ['domain', 'workflow', 'worker'].map(name => path.join(root, `uber/cadence/api/v1/service_${name}.proto`));
const internalRoot = path.resolve(__dirname, '../go/proto/internal');
files.push(path.join(internalRoot, 'uber/cadence/matching/v1/service.proto'));
files.push(path.join(root, 'uber/cadence/api/v1/service_semaphore.proto'));
const schema = grpc.loadPackageDefinition(loader.loadSync(files, {
  includeDirs: [root, internalRoot], enums: String, longs: String, bytes: Buffer, defaults: true, oneofs: true,
})).uber.cadence;
const api = schema.api.v1;
const matchingApi = schema.matching.v1;
const peer = process.argv[2] || '127.0.0.1:7833';
const restartMode = process.argv[7] === 'restart';
const goServicesPid = Number(process.argv[8] || 0);
const productionMode = process.argv[9] === 'production';
const failureRecoveryMode = process.env.CADENCE_MATCHING_FAILURE_RECOVERY === '1';
const matchingCredentials = process.env.CADENCE_MATCHING_GRPC_CA_FILE
  ? grpc.credentials.createSsl(fs.readFileSync(process.env.CADENCE_MATCHING_GRPC_CA_FILE))
  : grpc.credentials.createInsecure();

async function recordVerifiedMatchingTls(work, port, label, pid) {
  const caFile = process.env.CADENCE_MATCHING_GRPC_CA_FILE;
  if (!caFile) return;
  const expected = new X509Certificate(fs.readFileSync(process.env.CADENCE_MATCHING_SERVER_CERT_FILE));
  const ca = new X509Certificate(fs.readFileSync(caFile));
  const observed = await new Promise((resolve, reject) => {
    const socket = tls.connect({
      host: '127.0.0.1', port: Number(port), servername: 'localhost',
      ca: fs.readFileSync(caFile), rejectUnauthorized: true, ALPNProtocols: ['h2'],
    });
    socket.once('error', reject);
    socket.setTimeout(5000, () => socket.destroy(new Error('Matching TLS evidence handshake timed out')));
    socket.once('secureConnect', () => {
      try {
        assert.equal(socket.authorized, true, String(socket.authorizationError));
        assert.equal(socket.alpnProtocol, 'h2');
        const peer = new X509Certificate(socket.getPeerCertificate().raw);
        assert.equal(peer.fingerprint256, expected.fingerprint256, 'running Matching certificate differs from configured certificate');
        resolve({ label, pid, endpoint: `127.0.0.1:${port}`, serverName: 'localhost',
          rejectUnauthorized: true, authorized: socket.authorized, alpn: socket.alpnProtocol,
          caFingerprint256: ca.fingerprint256, serverFingerprint256: peer.fingerprint256,
          subject: peer.subject, issuer: peer.issuer, subjectAltName: peer.subjectAltName,
          verifiedAt: new Date().toISOString() });
      } catch (error) { reject(error); }
      finally { socket.end(); }
    });
  });
  const file = path.join(work, 'matching-tls-handshakes.json');
  const handshakes = fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, 'utf8')) : [];
  handshakes.push(observed);
  fs.writeFileSync(file, JSON.stringify(handshakes, null, 2) + '\n');
}
const shardManagerControl = process.env.CADENCE_MATCHING_SHARD_DISTRIBUTOR_CONTROL;
const shardManagerNamespace = process.env.CADENCE_MATCHING_SHARD_DISTRIBUTOR_NAMESPACE || 'matching';
const clients = Object.fromEntries(['DomainAPI', 'WorkflowAPI', 'WorkerAPI', 'SemaphoreAPI'].map(name => [name, new api[name](peer, grpc.credentials.createInsecure())]));
clients.Matching = new schema.matching.v1.MatchingAPI(peer, matchingCredentials);
const cassandraGate = process.env.CADENCE_MATCHING_CASSANDRA_GATE === '1';
function cassandraCommand(args, label) {
  const result = spawnSync('go', ['run', './tests/matching-cassandra-interop', ...args], {
    cwd: path.resolve(__dirname, '..', 'go'), encoding: 'utf8', timeout: 600000, env: process.env,
  });
  fs.writeFileSync(path.join(path.resolve(process.argv[5]), `${label}.log`), `${result.stdout || ''}\n${result.stderr || ''}`);
  if (result.status !== 0) throw new Error(`Cassandra ${label} failed (${result.error || result.status}); see ${path.resolve(process.argv[5], `${label}.log`)}`);
  return JSON.parse(result.stdout.trim().split('\n').at(-1));
}
function sqliteTaskState(domainId, taskList, label, taskType = 0) {
  if (process.env.CADENCE_MATCHING_MYSQL || process.env.CADENCE_MATCHING_POSTGRES) {
    // The observer contract is the same for every production database: the
    // number of undelivered decision or activity rows for one task list.
    const durable = durableTaskCount(domainId, taskList, taskType, undefined, false, 10000);
    fs.writeFileSync(path.join(path.resolve(process.argv[5]), label + '.log'), (durable.stdout || '') + '\n' + (durable.stderr || ''));
    if (durable.status !== 0) throw new Error('durable task observation failed (' + (durable.error || durable.status) + '); see ' + path.resolve(process.argv[5], label + '.log'));
    const range = durableTaskCount(domainId, taskList, taskType, undefined, true, 10000);
    if (range.status !== 0) throw new Error('task-list range observation failed (' + (range.error || range.status) + '): ' + (range.stderr || ''));
    return { queued_rows: Number(durable.stdout.trim()), range_id: Number(range.stdout.trim()) };
  }
  const database = productionMode ? process.env.CADENCE_MATCHING_SQLITE_DB : path.join(path.resolve(process.argv[5]), 'matching.db');
  if (!database) throw new Error('SQLite production probe requires CADENCE_MATCHING_SQLITE_DB');
  const result = spawnSync('python3', ['-c',
    'import json,sqlite3,sys,uuid; c=sqlite3.connect(sys.argv[1],timeout=30); n=c.execute("select count(*) from tasks where task_list_name=? and task_type=?",(sys.argv[3],int(sys.argv[4]))).fetchone()[0]; r=c.execute("select range_id from task_lists where shard_id=0 and domain_id=? and name=? and task_type=?",(uuid.UUID(sys.argv[2]).bytes,sys.argv[3],int(sys.argv[4]))).fetchone(); print(json.dumps({"queued_rows":n,"range_id":r[0] if r else 0}))',
    database, domainId, taskList, String(taskType)], { encoding: 'utf8' });
  fs.writeFileSync(path.join(path.resolve(process.argv[5]), label + '.log'), (result.stdout || '') + '\n' + (result.stderr || ''));
  if (result.status !== 0) throw new Error('SQLite task observation failed (' + (result.error || result.status) + '); see ' + path.resolve(process.argv[5], label + '.log'));
  return JSON.parse(result.stdout.trim());
}
const taskObserver = createTaskObserver({
  backend: cassandraGate ? 'cassandra' : 'sqlite',
  cassandraCommand: (args, label) => cassandraCommand(args, label),
  sqliteTaskState,
});
function acquireSemaphore(client, request) {
  const metadata = new grpc.Metadata();
  metadata.set('rpc-service', 'cadence-matching');
  metadata.set('rpc-encoding', 'proto');
  metadata.set('rpc-caller', 'cadence-cassandra-matching-gate');
  return new Promise((resolve, reject) => client.AcquireSemaphore(request, metadata,
    { deadline: Date.now() + 20000 }, (error, response) => error ? reject(error) : resolve(response)));
}
let managedShardNames = new Set();
let assignedExecutorId = '';
let shardAssignmentLock = Promise.resolve();
async function shardManagerCommand(command) {
  if (!shardManagerControl) return null;
  const response = await fetch(`${shardManagerControl}/command`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ namespace: shardManagerNamespace, ...command }),
  });
  if (!response.ok) throw new Error(`Go shard-manager control ${command.op} failed: HTTP ${response.status}`);
  return (await response.json()).result;
}
async function registerShardWithGoManager(shardName, force = false) {
  if (!shardManagerControl) return;
  const next = shardAssignmentLock.then(async () => {
    if (realShardDistributor) return registerShardWithRealManager(shardName);
    const wasManaged = managedShardNames.has(shardName);
    managedShardNames.add(shardName);
    const deadline = Date.now() + 30000;
    let executorId, heartbeat;
    while (Date.now() < deadline) {
      const stats = await shardManagerCommand({ op: 'stats' });
      // The probe talks to the primary Rust node's gRPC port, so shards go to the executor that
      // registered that port (alternating onto the secondary would reload task lists mid-scenario).
      const primaryGrpc = String(process.env.CADENCE_RUST_MATCHING_PRODUCTION_GRPC_PORT || '');
      const allEntries = Object.entries(stats || {});
      const primaryEntries = allEntries.filter(([, e]) => sdMatchesGrpc(e, primaryGrpc)
        && Date.now() - (Date.parse(e.LastHeartbeat || e.lastHeartbeat || '') || 0) < 10000);
      const entries = primaryEntries.sort((a, b) =>
        (Date.parse(b[1].LastHeartbeat || b[1].lastHeartbeat || '') || 0)
          - (Date.parse(a[1].LastHeartbeat || a[1].lastHeartbeat || '') || 0));
      if (entries.length) {
        [executorId, heartbeat] = entries[0];
        break;
      }
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    if (!executorId) throw new Error('Rust Matching did not heartbeat to the configured Go shard-manager');
    if (force || executorId !== assignedExecutorId || !wasManaged) {
      await shardManagerCommand({
        op: 'assign', executor: executorId, shards: [...managedShardNames],
        metadata: heartbeat.Metadata || heartbeat.metadata || {},
      });
      assignedExecutorId = executorId;
    }
    const readyDeadline = Date.now() + 15000;
    while (Date.now() < readyDeadline) {
      const stats = await shardManagerCommand({ op: 'stats' });
      const current = stats?.[executorId];
      const reports = current?.ReportedShards || current?.reportedShards || {};
      if ([...managedShardNames].every(name => {
        const status = reports[name]?.Status ?? reports[name]?.status;
        return status === 1 || status === 'READY' || status === 'ready' || status === 'ShardStatusREADY';
      })) return;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    throw new Error(`Rust Matching did not report assigned shard ${shardName} READY`);
  });
  shardAssignmentLock = next.catch(() => {});
  return next;
}

// Shard-distributor lane (--shard-distributor): scenarios against the pinned Go
// shard-manager handlers. Stats shape (field names/status values) is read
// defensively because it has not been observed from a live run yet.
const shardDistributorRestartDir = process.env.CADENCE_MATCHING_SHARD_DISTRIBUTOR_RESTART_DIR;
const shardDistributorEvidence = [];
const sdHeartbeatMs = executor => Date.parse(executor?.LastHeartbeat || executor?.lastHeartbeat || '') || 0;
const sdMetadata = executor => executor?.Metadata || executor?.metadata || {};
function sdMatchesGrpc(executor, port) {
  const advertised = String(sdMetadata(executor).grpc || '');
  // Shard-manager stores executor metadata verbatim. Production registrations may
  // advertise either a bare port or host:port, so compare the port component.
  return advertised === String(port) || advertised.endsWith(`:${port}`);
}
async function sdFreshExecutors(maxAgeMs = 10000) {
  const stats = (await shardManagerCommand({ op: 'stats' })) || {};
  return Object.entries(stats).filter(([, executor]) => Date.now() - sdHeartbeatMs(executor) < maxAgeMs);
}
async function sdAssertHeartbeat(label, { excludeIds = [], minimum = 1 } = {}) {
  if (!shardManagerControl) return null;
  const deadline = Date.now() + 30000;
  for (;;) {
    const fresh = (await sdFreshExecutors()).filter(([executorId]) => !excludeIds.includes(executorId));
    if (fresh.length >= minimum) {
      const record = { label, executors: fresh.map(([executorId]) => executorId), observedAt: new Date().toISOString() };
      shardDistributorEvidence.push({ check: 'executor-heartbeat', ...record });
      return record;
    }
    if (Date.now() > deadline) throw new Error(`shard-distributor ${label}: no fresh Rust Matching executor heartbeat within 30s`);
    await new Promise(resolve => setTimeout(resolve, 250));
  }
}
async function sdAssertExecutorStopped(label, previousIds) {
  if (!shardManagerControl) return;
  const deadline = Date.now() + 30000;
  for (;;) {
    const stats = (await shardManagerCommand({ op: 'stats' })) || {};
    const fresh = (await sdFreshExecutors(3000)).map(([executorId]) => executorId);
    const stopped = previousIds.filter(executorId => !fresh.includes(executorId));
    if (stopped.length > 0) {
      shardDistributorEvidence.push({ check: 'executor-stopped', label, stopped,
        status: Object.fromEntries(stopped.map(executorId => [executorId, stats[executorId]?.Status ?? stats[executorId]?.status ?? 'removed'])) });
      return stopped;
    }
    if (Date.now() > deadline) throw new Error(`shard-distributor ${label}: executor kept heartbeating after Rust Matching stopped`);
    await new Promise(resolve => setTimeout(resolve, 500));
  }
}
// Real shard-distributor only: after Rust Matching is SIGKILLed the real leader loop must
// drop the dead executor once its heartbeat TTL lapses and release its shard assignments.
const realShardDistributor = process.env.CADENCE_MATCHING_SHARD_DISTRIBUTOR_REAL === '1';
async function sdAssertExecutorExpired(label, previousIds, killedAt) {
  if (!shardManagerControl || !realShardDistributor) return;
  const deadline = Date.now() + 90000;
  for (;;) {
    const stats = (await shardManagerCommand({ op: 'stats' })) || {};
    const assignments = (await shardManagerCommand({ op: 'assignments' })) || {};
    const lingering = previousIds.filter(id => id in stats || (assignments[id] || []).length > 0);
    if (lingering.length === 0) {
      shardDistributorEvidence.push({ check: 'executor-expired-by-ttl', label, expired: previousIds, elapsedMsSinceKill: Date.now() - killedAt });
      return;
    }
    if (Date.now() > deadline) {
      const snapshot = { label, killedAt: new Date(killedAt).toISOString(), observedAt: new Date().toISOString(),
        selectedExecutors: previousIds.map(id => ({ id, metadata: sdMetadata(stats[id]), lastHeartbeat: stats[id]?.LastHeartbeat || stats[id]?.lastHeartbeat || null })),
        lingeringExecutors: lingering.map(id => ({ id, metadata: sdMetadata(stats[id]), lastHeartbeat: stats[id]?.LastHeartbeat || stats[id]?.lastHeartbeat || null, assignments: assignments[id] || [] })) };
      const snapshotPath = path.join(work, `shard-distributor-${label}-failure.json`);
      fs.writeFileSync(snapshotPath, `${JSON.stringify(snapshot, null, 2)}\n`);
      throw new Error(`shard-distributor ${label}: real leader loop did not expire executor(s) ${lingering.join(',')} within 90s; diagnostic snapshot: ${snapshotPath}`);
    }
    await new Promise(resolve => setTimeout(resolve, 500));
  }
}
async function sdReassign(label, taskListName) {
  if (!shardManagerControl) return;
  await registerShardWithGoManager(taskListName, true);
  shardDistributorEvidence.push({ check: 'ownership-reassigned', label, taskList: taskListName });
}
async function sdRestartScenario(label, domain, id) {
  if (!shardManagerControl || !shardDistributorRestartDir) return;
  const request = path.join(shardDistributorRestartDir, 'restart.request');
  const done = path.join(shardDistributorRestartDir, 'restart.done');
  if (fs.existsSync(done)) fs.unlinkSync(done);
  fs.writeFileSync(request, label);
  const deadline = Date.now() + 90000;
  while (!fs.existsSync(done)) {
    if (Date.now() > deadline) throw new Error('harness did not restart the shard-distributor fixture within 90s');
    await new Promise(resolve => setTimeout(resolve, 250));
  }
  // The restarted shard-distributor starts with empty state: Rust Matching must
  // re-register through its executor heartbeat and be given ownership again.
  await sdAssertHeartbeat(`${label}-after-restart`);
  const taskList = { name: `sd-restart-${id}` };
  const info = await call('WorkflowAPI', 'DescribeTaskList', { domain, taskList, taskListType: 'TASK_LIST_TYPE_DECISION', includeTaskListStatus: true });
  assert(info.taskListStatus, 'DescribeTaskList through restarted shard-distributor returned no status');
  shardDistributorEvidence.push({ check: 'shard-distributor-restart', label, taskList: taskList.name });
}
// Real shard-distributor: the leader loop picks the owner, so the probe asks GetShardOwner (through the
// launcher's read-only "assign" op), waits until that executor reports the shard READY and then sends
// the task list's RPCs to that executor's gRPC port.
const realShardOwnerPorts = new Map();
const ringOwnedTaskLists = new Set();
const realOwnerClients = new Map();
async function registerShardWithRealManager(shardName) {
  managedShardNames.add(shardName);
  const deadline = Date.now() + 45000;
  let lastProblem = 'no executor heartbeat';
  while (Date.now() < deadline) {
    try {
      const owners = await shardManagerCommand({ op: 'assign', shards: [shardName] });
      const ownerId = owners?.[shardName];
      const stats = (await shardManagerCommand({ op: 'stats' })) || {};
      const current = ownerId && stats[ownerId];
      if (current) {
        const reports = current.ReportedShards || current.reportedShards || {};
        const status = reports[shardName]?.Status ?? reports[shardName]?.status;
        const port = String((current.Metadata || current.metadata || {}).grpc || '');
        if (port && (status === 1 || status === 'READY' || status === 'ready' || status === 'ShardStatusREADY')) {
          if (!ringOwnedTaskLists.has(shardName)) realShardOwnerPorts.set(shardName, port);
          return;
        }
        lastProblem = `owner ${ownerId} status ${status}`;
      } else lastProblem = `owner ${ownerId} not in executor stats`;
    } catch (error) { lastProblem = error.message; }
    await new Promise(resolve => setTimeout(resolve, 200));
  }
  throw new Error(`real shard-distributor did not give ${shardName} a READY owner (${lastProblem})`);
}
function clientsForTaskList(taskListName) {
  const port = realShardDistributor && taskListName ? realShardOwnerPorts.get(taskListName) : undefined;
  if (!port) return clients;
  if (!realOwnerClients.has(port)) {
    const owned = Object.fromEntries(['DomainAPI', 'WorkflowAPI', 'WorkerAPI', 'SemaphoreAPI'].map(name => [name, new api[name](`127.0.0.1:${port}`, grpc.credentials.createInsecure())]));
    owned.Matching = new schema.matching.v1.MatchingAPI(`127.0.0.1:${port}`, matchingCredentials);
    realOwnerClients.set(port, owned);
  }
  return realOwnerClients.get(port);
}
function call(service, method, request) {
  const taskListName = request.taskList?.name || request.request?.taskList?.name;
  if (!taskListName) return callRpc(service, method, request);
  if (!realShardDistributor) return registerShardWithGoManager(taskListName).then(() => callRpc(service, method, request));
  // The real leader loop may move a shard between executors at any time; a non-owner answers a typed
  // ABORTED before doing any work, so the call is re-resolved to the new owner and sent again.
  return (async () => {
    for (let attempt = 0; ; attempt++) {
      await registerShardWithGoManager(taskListName);
      try { return await callRpc(service, method, request); } catch (error) {
        if (attempt >= 8 || !/not owned by this host/.test(error.message)) throw error;
        // Go excludes short-lived task lists from the shard distributor (errIfShardOwnershipLost), so for
        // them the hash-ring owner named in the typed error is the one to follow.
        const ringOwner = /OwnedBy: ([^,\s]+)/.exec(error.message)?.[1];
        const ownerPort = ringOwner && ringOwner !== 'not' ? ringOwner.split(':').at(-1) : undefined;
        if (ownerPort) {
          const stats = (await shardManagerCommand({ op: 'stats' })) || {};
          const owner = Object.values(stats).find(e => String((e.Metadata || e.metadata || {}).tchannel) === ownerPort);
          const grpcPort = owner && String((owner.Metadata || owner.metadata || {}).grpc);
          if (grpcPort) { ringOwnedTaskLists.add(taskListName); realShardOwnerPorts.set(taskListName, grpcPort); }
        }
        await new Promise(resolve => setTimeout(resolve, 300));
      }
    }
  })();
}
function callRpc(service, method, request) {
  const taskListName = request.taskList?.name || request.request?.taskList?.name;
  const metadata = new grpc.Metadata();
  metadata.set('rpc-service', service === 'Matching' ? 'cadence-matching' : 'cadence-frontend');
  metadata.set('rpc-encoding', 'proto');
  metadata.set('rpc-caller', 'cadence-service-compat');
  return new Promise((resolve, reject) => (service === 'Matching' ? clientsForTaskList(taskListName) : clients)[service][method](request, metadata,
    { deadline: Date.now() + 20000 }, (error, response) => {
      if (error) {
        error.message = `${service}.${method} failed${taskListName ? ` for task list ${taskListName}` : ''}: ${error.message}`;
        return reject(error);
      }
      resolve(response);
    }));
}
const payload = data => ({ data: Buffer.from(data) });
function sqlText(value) { return String(value).replaceAll("'", "''"); }
function domainHex(domainId) { return String(domainId).replace(/-/g, '').toLowerCase(); }
const MYSQL_OBSERVATION_TIMEOUT_MS = 30000;
function mysqlQuery(sql) {
  if (!process.env.CADENCE_MATCHING_MYSQL) return { status: 1, stdout: '', stderr: 'MySQL probe mode is not enabled' };
  const container = process.env.CADENCE_MYSQL_DOCKER_CONTAINER;
  const password = process.env.CADENCE_MYSQL_PASSWORD || '';
  const database = process.env.CADENCE_MYSQL_DATABASE || 'cadence';
  const command = container
    ? ['docker', ['exec', container, 'mysql', '--batch', '--skip-column-names', '-uroot',
      `-p${password}`, database, '-e', sql]]
    : ['mysql', ['--batch', '--skip-column-names', '--host', process.env.CADENCE_MYSQL_HOST || '127.0.0.1',
      '--port', process.env.CADENCE_MYSQL_PORT || '3306', '--user', process.env.CADENCE_MYSQL_USER || 'root',
      `--password=${password}`, database, '-e', sql]];
  // The observation is bounded so a stalled database or container cannot hang
  // the gate; every failure mode is reported instead of a silent empty result.
  const result = spawnSync(command[0], command[1], {
    encoding: 'utf8', maxBuffer: 64 * 1024 * 1024, timeout: MYSQL_OBSERVATION_TIMEOUT_MS,
  });
  if (result.error) return { status: null, stdout: result.stdout || '', stderr: `MySQL observation failed: ${result.error.message}` };
  if (result.signal) return { status: null, stdout: result.stdout || '', stderr: `MySQL observation was killed by ${result.signal} after ${MYSQL_OBSERVATION_TIMEOUT_MS}ms` };
  return { status: result.status, stdout: result.stdout || '', stderr: result.stderr || '' };
}
function mysqlRows(sql) {
  const result = mysqlQuery(sql);
  if (result.status !== 0) throw new Error(`MySQL query failed: ${(result.stderr || '').trim() || sql}`);
  return result.stdout.split('\n').map(line => line.trim()).filter(line => line.length > 0).map(line => line.split('\t'));
}
// Every task query is constrained by the canonical domain UUID, the task list
// and the task type, exactly like Go's task table keys.
function durableTaskCount(domainId, taskList, taskType, fallbackDatabase, rangeId = false, timeoutMs) {
  if (process.env.CADENCE_MATCHING_MYSQL) {
    const query = rangeId ? `SELECT COALESCE(MAX(range_id),0) FROM task_lists WHERE domain_id=UNHEX('${domainHex(domainId)}') AND name='${sqlText(taskList)}' AND task_type=${taskType}` : `SELECT COUNT(*) FROM tasks WHERE domain_id=UNHEX('${domainHex(domainId)}') AND task_list_name='${sqlText(taskList)}' AND task_type=${taskType}`;
    const result = mysqlQuery(query);
    if (result.status === 0) result.stdout = result.stdout.trim();
    return result;
  }
  if (process.env.CADENCE_MATCHING_POSTGRES) {
    const pgEnv = { ...process.env, PGHOST: process.env.CADENCE_POSTGRES_HOST,
      PGPORT: process.env.CADENCE_POSTGRES_PORT, PGUSER: process.env.CADENCE_POSTGRES_USER,
      PGDATABASE: process.env.CADENCE_POSTGRES_DATABASE,
      PGPASSWORD: process.env.CADENCE_POSTGRES_PASSWORD || '',
      PGSSLMODE: process.env.CADENCE_POSTGRES_SSLMODE || 'disable' };
    for (const [name, value] of [['PGSSLROOTCERT', process.env.CADENCE_POSTGRES_CA_FILE],
      ['PGSSLCERT', process.env.CADENCE_POSTGRES_CERT_FILE], ['PGSSLKEY', process.env.CADENCE_POSTGRES_KEY_FILE]]) {
      if (value) pgEnv[name] = value;
    }
    const result = spawnSync('psql', ['-X', '-A', '-t', '-w', '-v', 'ON_ERROR_STOP=1', '-v', `task_list=${taskList}`,
      '-v', `domain_id=${domainId}`, '-f', '-'], {
      input: rangeId ? `SELECT COALESCE(MAX(range_id),0) FROM task_lists WHERE domain_id = decode(replace(:'domain_id','-',''),'hex') AND name = :'task_list' AND task_type = ${Number(taskType)};\n` : `SELECT count(*) FROM tasks WHERE domain_id = decode(replace(:'domain_id','-',''),'hex') AND task_list_name = :'task_list' AND task_type = ${Number(taskType)};\n`,
      encoding: 'utf8', env: pgEnv, timeout: timeoutMs,
    });
    if (result.status === 0) result.stdout = result.stdout.trim();
    return result;
  }
  const database = process.env.CADENCE_MATCHING_SQLITE_DB || fallbackDatabase;
  return spawnSync('python3', ['-c', 'import sqlite3,sys; c=sqlite3.connect(sys.argv[1], timeout=30); print(c.execute("select count(*) from tasks where domain_id=? and task_list_name=? and task_type=?", (bytes.fromhex(sys.argv[2]), sys.argv[3], int(sys.argv[4]))).fetchone()[0])', database, domainHex(domainId), taskList, String(taskType)], { encoding: 'utf8', timeout: 60000 });
}
// The production MySQL gate (task garbage collection, persisted
// acknowledgement, range consistency and Go's domain metadata) lives in
// tests/matching-mysql-gate.cjs together with the ThriftRW blob decoder.
const { observeProductionMySqlDomain, verifyProductionMySqlTaskGc, verifyTaskListBlobDecoder } = require('./matching-mysql-gate.cjs');
async function run() {
  const id = process.env.CADENCE_MATCHING_TEST_ID || randomUUID();
  const work = path.resolve(process.argv[5]);
  const domain = `service-compat-${id}`, taskList = { name: `tasks-${id}` };
  // Go's worker registers cadence-system at startup; two concurrent SQL domain registrations race on
  // domain_metadata's notification_version and the loser fails before writing anything, so retry.
  for (let attempt = 0; ; attempt++) {
    try { await call('DomainAPI', 'RegisterDomain', { name: domain, workflowExecutionRetentionPeriod: { seconds: '259200' } }); break; } catch (error) {
      if (attempt >= 5 || !/Failed to update domain metadata/.test(error.message)) throw error;
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  }
  const domainId = (await call('DomainAPI', 'DescribeDomain', { name: domain })).domain.id;
  let pageTokenDomain, domainPages = 0;
  if (cassandraGate) {
    pageTokenDomain = `service-compat-page-${id}`;
    await call('DomainAPI', 'RegisterDomain', { name: pageTokenDomain, workflowExecutionRetentionPeriod: { seconds: '259200' } });
    let pageToken = Buffer.alloc(0);
    const listedDomainNames = new Set();
    do {
      const page = await call('DomainAPI', 'ListDomains', { pageSize: 1, nextPageToken: pageToken });
      for (const item of page.domains) listedDomainNames.add(item.name);
      pageToken = page.nextPageToken;
      domainPages++;
      assert(domainPages < 1000, 'domain page token traversal did not terminate');
    } while (pageToken?.length);
    assert(domainPages > 1, 'Go Cassandra ListDomains must exercise at least one opaque page-token continuation');
    assert(listedDomainNames.has(domain) && listedDomainNames.has(pageTokenDomain),
      'Go Cassandra domain enumeration must return both registered domains through opaque page tokens');
  }
  fs.mkdirSync(work, { recursive: true });
  const ready = path.join(work, 'ready'), stop = path.join(work, 'stop'), fixture = path.join(work, 'fixture.json');
  if (!productionMode) {
    fs.writeFileSync(fixture, JSON.stringify({ domain, domain_id: domainId, workflow_id: '', run_id: '', task_list: taskList.name, history_peer: process.argv[3], kind: 'server', schedule_id: 0, database: path.join(work, 'matching.db'), output: ready, listen: process.argv[4], stop_file: stop, idle_trigger: path.join(work, 'idle-trigger'), idle_done: path.join(work, 'idle-done'), isolation_groups: ['zone-a', 'zone-b'] }));
  }
  const reportedTestBinary = process.env.CADENCE_MATCHING_DELIVERY_TEST_EXECUTABLE;
  const productionBinary = process.env.CADENCE_RUST_MATCHING_PRODUCTION_BINARY;
  const rustConfig = process.env.CADENCE_RUST_MATCHING_PRODUCTION_CONFIG;
  const rustTChannel = process.env.CADENCE_RUST_MATCHING_PRODUCTION_TCHANNEL;
  const rustGrpcPort = process.env.CADENCE_RUST_MATCHING_PRODUCTION_GRPC_PORT;
  const historyGrpcPort = process.env.CADENCE_RUST_MATCHING_PRODUCTION_HISTORY_GRPC_PORT;
  const rustDataDir = process.env.CADENCE_RUST_MATCHING_PRODUCTION_DATA_DIR;
  let testBinary;
  if (productionMode) {
    if (!productionBinary || !rustConfig || !rustTChannel || !rustGrpcPort || !historyGrpcPort || !rustDataDir) throw new Error('production Matching mode requires the Rust binary, config, gRPC/TChannel ports and data directory');
    if (!fs.existsSync(productionBinary)) throw new Error(`Rust cadence-matching binary does not exist: ${productionBinary}`);
  } else {
    if (!reportedTestBinary) throw new Error('CADENCE_MATCHING_DELIVERY_TEST_EXECUTABLE must identify the executable reported by Cargo');
    testBinary = path.resolve(reportedTestBinary);
    if (!fs.existsSync(testBinary) || !fs.statSync(testBinary).isFile()
        || (process.platform !== 'win32' && !(fs.statSync(testBinary).mode & 0o111))) {
      throw new Error(`Cargo-reported matching_delivery test executable is missing or not executable: ${testBinary}`);
    }
  }
  function startRust(label) {
    for (const file of [ready, stop]) if (fs.existsSync(file)) fs.unlinkSync(file);
    const output = fs.openSync(path.join(work, `rust-server-${label}.log`), 'w');
    const command = productionMode ? [productionBinary, [
      'start', '--config', rustConfig, '--services', 'matching', '--port', '0',
      '--history-grpc-port', historyGrpcPort, '--matching-grpc-port', rustGrpcPort,
      '--matching-tchannel-port', rustTChannel, '--data-dir', rustDataDir,
    ], { ...process.env }] : [testBinary,
      ['matching_delivery_go_history', '--ignored', '--exact'],
      { ...process.env, CADENCE_MATCHING_DELIVERY_FIXTURE: fixture }];
    const child = spawn(command[0], command[1], { cwd: path.resolve(__dirname, '..'), env: command[2], stdio: ['ignore', output, output] });
    fs.closeSync(output);
    exited = new Promise(resolve => child.once('exit', (code, signal) => resolve({ code, signal })));
    return child;
  }
  function startSecondaryRust() {
    const output = fs.openSync(path.join(work, 'rust-server-secondary.log'), 'w');
    const child = spawn(productionBinary, [
      'start', '--config', process.env.CADENCE_RUST_MATCHING_SECONDARY_CONFIG || rustConfig, '--services', 'matching', '--port', '0',
      '--history-grpc-port', historyGrpcPort,
      '--matching-grpc-port', process.env.CADENCE_RUST_MATCHING_SECONDARY_GRPC_PORT,
      '--matching-tchannel-port', process.env.CADENCE_RUST_MATCHING_SECONDARY_TCHANNEL,
      '--data-dir', path.join(path.dirname(rustDataDir), 'rust-matching-secondary-data'),
    ], { cwd: path.resolve(__dirname, '..'), env: process.env, stdio: ['ignore', output, output] });
    fs.closeSync(output);
    return child;
  }
  function directMatchingClient(address) {
    return new matchingApi.MatchingAPI(address, matchingCredentials);
  }
  function directDecisionPoll(client, domainId, taskListName, suffix) {
    const metadata = new grpc.Metadata();
    metadata.set('rpc-service', 'cadence-matching');
    metadata.set('rpc-caller', 'cadence-matching-failure-recovery');
    metadata.set('rpc-encoding', 'proto');
    return new Promise((resolve, reject) => client.PollForDecisionTask({
      domainId, pollerId: `failure-probe-${suffix}`, request: {
        domain, taskList: { name: taskListName }, identity: `failure-probe-${suffix}`,
      },
    }, metadata, { deadline: Date.now() + 10000 }, (error, response) => error ? reject(error) : resolve(response)));
  }
  function directPartitionOwner(client, domainName, taskListName, taskType = 'decision') {
    return new Promise((resolve, reject) => client.ListTaskListPartitions({
      domain: domainName, taskList: { name: taskListName },
    }, new grpc.Metadata(), { deadline: Date.now() + 10000 }, (error, response) => {
      if (error) return reject(error);
      const partitions = taskType === 'activity'
        ? response.activityTaskListPartitions || [] : response.decisionTaskListPartitions || [];
      resolve(partitions.find(partition => partition.key === taskListName)?.ownerHostName || '');
    }));
  }
  async function waitForRustReady(label) {
    const deadline = Date.now() + 180000;
    let lastReadinessError;
    let directReadiness = 'not probed';
    let nextDirectProbeAt = 0;
    const primaryReadinessClient = productionMode ? directMatchingClient(`127.0.0.1:${rustGrpcPort}`) : null;
    while (Date.now() < deadline) {
      if (productionMode) {
        // Probe the exact Rust endpoint independently of Go Frontend routing.
        // The production gate still requires Frontend discovery below; this
        // sample only separates a Rust admission/startup failure from a
        // Frontend peer-selection or outbound-client failure.
        if (Date.now() >= nextDirectProbeAt) {
          nextDirectProbeAt = Date.now() + 5000;
          try {
            await directPartitionOwner(primaryReadinessClient, domain, `primary-ready-${id}`);
            directReadiness = 'ready';
          } catch (error) {
            directReadiness = error?.details || error?.message || String(error);
          }
        }
        try {
          // This public Go Frontend call must resolve Matching through Go's
          // real Ringpop PeerProvider; TCP readiness alone is insufficient.
          await call('WorkflowAPI', 'DescribeTaskList', {
            domain, taskList: { name: `discovery-ready-${id}` },
            taskListType: 'TASK_LIST_TYPE_DECISION', includeTaskListStatus: true,
          });
          // A secondary Matching node can satisfy the Frontend probe while the
          // restarted primary is still binding its listener. Verify the exact
          // process endpoint before callers use its direct client.
          await directPartitionOwner(primaryReadinessClient, domain, `primary-ready-${id}`);
          await recordVerifiedMatchingTls(work, rustGrpcPort, label, rust.pid);
          primaryReadinessClient.close();
          return;
        } catch (error) {
          lastReadinessError = error;
          // JOIN, SWIM, label publication, or Go membership propagation may
          // still be in progress. The timeout below leaves the cause in logs.
        }
      } else if (fs.existsSync(ready)) {
        return;
      }
      if (rust.exitCode !== null || rust.signalCode !== null) {
        primaryReadinessClient?.close();
        throw new Error(`Rust Matching ${label} exited before readiness; see ${work}/rust-server-${label}.log`);
      }
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    primaryReadinessClient?.close();
    throw new Error(`Rust Matching ${label} did not become ready through Go Frontend and its primary gRPC endpoint; last Go Frontend error: ${lastReadinessError?.details || lastReadinessError?.message || 'none'}; direct primary gRPC status: ${directReadiness}; see ${work}/rust-server-${label}.log`);
  }
  let exited;
  let rust = startRust('before-restart');
  let secondaryRust;
  let semaphoreEvidence;
  let semaphoreAcquire;
  let configstoreEvidence;
  let semaphoreOwner;
  let probeFailure;
  try {
    await waitForRustReady('before-restart');
  const coldDomain = process.env.CADENCE_MATCHING_COLD_DOMAIN;
  if (coldDomain) {
    await new Promise(resolve => setTimeout(resolve, 1100));
    assert(productionMode, 'cold-domain quota probe requires production Matching');
    const nameLookupDomain = `${coldDomain}-name`;
    const partitionLookupDomain = `${coldDomain}-partitions`;
    await call('DomainAPI', 'RegisterDomain', { name: coldDomain, workflowExecutionRetentionPeriod: { seconds: '259200' } });
    const canonicalDomain = await call('DomainAPI', 'DescribeDomain', { name: coldDomain });
    // These are distinct late-registered domain keys in the shared process
    // cache. Readiness completed before registration, but background cache
    // refreshes may observe either registration before its first RPC; this
    // exercises name-based read-through without proving a cache miss.
    await call('DomainAPI', 'RegisterDomain', { name: nameLookupDomain, workflowExecutionRetentionPeriod: { seconds: '259200' } });
    const nameClient = directMatchingClient(`127.0.0.1:${rustGrpcPort}`);
    const nameLookup = await new Promise((resolve, reject) => nameClient.GetTaskListsByDomain(
      { domain: nameLookupDomain }, new grpc.Metadata(), { deadline: Date.now() + 20000 },
      (error, response) => error ? reject(error) : resolve(response)));
    assert(nameLookup && nameLookup.decisionTaskListMap && nameLookup.activityTaskListMap,
      `GetTaskListsByDomain by late domain name returned an invalid response: ${JSON.stringify(nameLookup)}`);
    nameClient.close();

    await call('DomainAPI', 'RegisterDomain', { name: partitionLookupDomain, workflowExecutionRetentionPeriod: { seconds: '259200' } });
    const partitionTaskList = `cold-name-partitions-${id}`;
    // Go and Rust suppress cache refreshes within one second of a prior
    // scan. Let a domain registered after the first name probe become eligible
    // for read-through; no UUID RPC warms this domain in the meantime.
    await new Promise(resolve => setTimeout(resolve, 1100));
    const partitionClient = directMatchingClient(`127.0.0.1:${rustGrpcPort}`);
    const partitionLookup = await new Promise((resolve, reject) => partitionClient.ListTaskListPartitions(
      { domain: partitionLookupDomain, taskList: { name: partitionTaskList } }, new grpc.Metadata(),
      { deadline: Date.now() + 20000 }, (error, response) => error ? reject(error) : resolve(response)));
    assert(partitionLookup && partitionLookup.decisionTaskListPartitions?.length > 0,
      `ListTaskListPartitions by late domain name returned no decision partitions: ${JSON.stringify(partitionLookup)}`);
    assert.equal(partitionLookup.decisionTaskListPartitions[0].key, partitionTaskList,
      `ListTaskListPartitions returned an unexpected root partition: ${JSON.stringify(partitionLookup.decisionTaskListPartitions)}`);
    partitionClient.close();

    const metadata = new grpc.Metadata();
    metadata.set('rpc-service', 'cadence-matching');
    metadata.set('rpc-encoding', 'proto');
    metadata.set('rpc-caller', 'cadence-cold-domain-metric-probe');
    const coldClient = directMatchingClient(`127.0.0.1:${rustGrpcPort}`);
    // Nonpositive domain rates fall back to host quota. Rate one supplies a
    // one-token initial burst, so concurrent requests can exercise rejection.
    const outcomes = await Promise.all([0, 1].map(index => {
      const taskListName = `cold-domain-${id}-${index}`;
      return new Promise(resolve => coldClient.AddDecisionTask({
        domainId: canonicalDomain.domain.id,
        workflowExecution: { workflowId: `cold-${id}-${index}`, runId: randomUUID() },
        taskList: { name: taskListName }, scheduleId: '1',
        scheduleToStartTimeout: { seconds: '10' }, source: 'TASK_SOURCE_HISTORY',
      }, metadata, { deadline: Date.now() + 20000 }, error => resolve({ taskListName, error })));
    }));
    coldClient.close();
    const rejected = outcomes.find(outcome => outcome.error?.code === grpc.status.RESOURCE_EXHAUSTED);
    assert(rejected, `one-token domain burst must reject a concurrent call: ${JSON.stringify(outcomes.map(outcome => ({ taskList: outcome.taskListName, code: outcome.error?.code })))}`);
    fs.writeFileSync(path.join(work, 'matching-cold-domain-metric-probe.json'), `${JSON.stringify({
      domain: canonicalDomain.domain.name, domainId: canonicalDomain.domain.id,
      nameLookup: {
        domain: nameLookupDomain, firstRpc: 'GetTaskListsByDomain',
        decisionTaskListNames: Object.keys(nameLookup.decisionTaskListMap),
        activityTaskListNames: Object.keys(nameLookup.activityTaskListMap),
        responseShapeValid: true,
      },
      partitionLookup: {
        domain: partitionLookupDomain, firstRpc: 'ListTaskListPartitions', taskList: partitionTaskList,
        decisionPartitionKeys: partitionLookup.decisionTaskListPartitions.map(partition => partition.key),
        activityPartitionKeys: partitionLookup.activityTaskListPartitions.map(partition => partition.key),
      },
      coldClaimLimit: 'Domains were registered after Matching readiness and queried by name without UUID RPC warming; domains use distinct keys in one shared process cache, and a background refresh may observe either registration before its first RPC.',
      taskList: rejected.taskListName, grpcCode: rejected.error.code, grpcMessage: rejected.error.message,
      requests: outcomes.map(outcome => ({ taskList: outcome.taskListName, grpcCode: outcome.error?.code ?? 0 })),
    }, null, 2)}\n`);
  }

  const startRequest = {
    domain, workflowId: id, workflowType: { name: 'ServiceCompatibility' }, taskList,
    requestId: randomUUID(), executionStartToCloseTimeout: { seconds: '300' },
    taskStartToCloseTimeout: { seconds: '10' }, input: payload('start'),
  };
  if (productionMode && failureRecoveryMode) {
    if (!process.env.CADENCE_RUST_MATCHING_SECONDARY_GRPC_PORT || !process.env.CADENCE_RUST_MATCHING_SECONDARY_TCHANNEL) {
      throw new Error('two-instance failure recovery requires secondary Matching ports');
    }
    secondaryRust = startSecondaryRust();
    let lastConvergenceError = '';
    const convergenceDeadline = Date.now() + 30000;
    while (Date.now() < convergenceDeadline) {
      if (secondaryRust.exitCode !== null || secondaryRust.signalCode !== null) throw new Error(`secondary Rust Matching exited; see ${work}/rust-server-secondary.log`);
      try {
        await call('WorkflowAPI', 'DescribeTaskList', { domain, taskList: { name: `secondary-ready-${id}` }, taskListType: 'TASK_LIST_TYPE_DECISION', includeTaskListStatus: true });
        break;
      } catch (error) {
        lastConvergenceError = error.message;
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    }
    if (Date.now() >= convergenceDeadline) throw new Error(`second Rust Matching instance did not join the original Ringpop cluster; last error: ${lastConvergenceError}`);
    if (cassandraGate) {
      const semaphoreName = `durable-${id}`;
      const createdSemaphore = await call('SemaphoreAPI', 'CreateSemaphore', {
        domain, semaphoreName, capacity: 1, bucketCapacity: 1,
      });
      assert.equal(createdSemaphore.semaphore.capacity, 1);
      assert.equal(createdSemaphore.semaphore.bucketCapacity, 1);
      const semaphoreClients = [directMatchingClient(`127.0.0.1:${rustGrpcPort}`), directMatchingClient(`127.0.0.1:${process.env.CADENCE_RUST_MATCHING_SECONDARY_GRPC_PORT}`)];
      semaphoreAcquire = (owner, clientIndex = 0) => acquireSemaphore(semaphoreClients[clientIndex], {
        domainId, semaphoreName, bucket: 0, ownerId: owner,
      }).catch(error => {
        if ((error.code === grpc.status.ABORTED || error.code === grpc.status.UNAVAILABLE)
            && clientIndex + 1 < semaphoreClients.length) return semaphoreAcquire(owner, clientIndex + 1);
        throw error;
      });
      // Parity gap (Go-side): Go never seeds token rows, and a bucket loads its
      // free set once at startup. A fresh capacity-1 semaphore therefore returns
      // NO_SLOT on its first acquire, the same as Go. Use a separate semaphore for
      // that evidence, then seed the race semaphore before its first acquire so
      // a row is visible to the bucket that serves the race.
      const gapSemaphoreName = `durable-gap-${id}`;
      await call('SemaphoreAPI', 'CreateSemaphore', {
        domain, semaphoreName: gapSemaphoreName, capacity: 1, bucketCapacity: 1,
      });
      const gapRequest = { domainId, semaphoreName: gapSemaphoreName, bucket: 0, ownerId: `${id.length}:${id}:${randomUUID()}:1` };
      const gapGrant = await acquireSemaphore(semaphoreClients[0], gapRequest).catch(error => {
        if (error.code === grpc.status.ABORTED || error.code === grpc.status.UNAVAILABLE) return acquireSemaphore(semaphoreClients[1], gapRequest);
        throw error;
      });
      assert.equal(gapGrant.outcome, 'SEMAPHORE_ACQUIRE_OUTCOME_NO_SLOT', 'fresh Cassandra semaphore with no token rows must answer NO_SLOT on its first acquire');
      cassandraCommand(['seed-semaphore', domainId, semaphoreName], 'semaphore-free-token-seed');
      const owners = [`${id.length}:${id}:${randomUUID()}:1`, `${id.length}:${id}:${randomUUID()}:2`];
      const grants = await Promise.all(owners.map(owner => semaphoreAcquire(owner)));
      const acquired = grants.map((grant, index) => ({ grant, owner: owners[index] }))
        .filter(({ grant }) => grant.outcome === 'SEMAPHORE_ACQUIRE_OUTCOME_ACQUIRED');
      const unavailable = grants.filter(grant => grant.outcome === 'SEMAPHORE_ACQUIRE_OUTCOME_NO_SLOT');
      assert.equal(acquired.length, 1, `Cassandra semaphore capacity one must grant exactly one concurrent owner; grants=${JSON.stringify(grants)}`);
      assert.equal(unavailable.length, 1, 'Cassandra semaphore capacity one must reject the competing owner');
      const expectedToken = Number(acquired[0].grant.tokenId);
      const stored = cassandraCommand(['verify-semaphore', domainId, semaphoreName, acquired[0].owner, String(expectedToken)], 'semaphore-before-recovery');
      assert.equal(stored.owner_id, acquired[0].owner, 'Go gocql must observe the exact Rust semaphore owner row');
      assert.equal(stored.token_id, expectedToken, 'Go gocql must observe the positive Rust semaphore token row');
      semaphoreOwner = stored.owner_id;
      // Go's semaphore.Identifier.RingKey is domainID_semaphoreName_bucket.
      // Resolve that exact key through both production Rust handlers before any
      // crash, so the durable hold is tied to the process that owns its manager.
      const semaphoreRingKey = `${domainId}_${semaphoreName}_0`;
      const primarySemaphoreOwner = await directPartitionOwner(
        directMatchingClient(`127.0.0.1:${rustGrpcPort}`), domain, semaphoreRingKey,
      );
      const secondarySemaphoreOwner = await directPartitionOwner(
        directMatchingClient(`127.0.0.1:${process.env.CADENCE_RUST_MATCHING_SECONDARY_GRPC_PORT}`), domain, semaphoreRingKey,
      );
      assert(primarySemaphoreOwner, 'primary did not resolve the semaphore bucket ring key');
      assert.equal(secondarySemaphoreOwner, primarySemaphoreOwner, 'Rust instances disagree on the semaphore bucket owner');
      const semaphoreOwnerIndex = primarySemaphoreOwner === `127.0.0.1:${rustTChannel}` ? 0
        : primarySemaphoreOwner === `127.0.0.1:${process.env.CADENCE_RUST_MATCHING_SECONDARY_TCHANNEL}` ? 1 : -1;
      assert(semaphoreOwnerIndex >= 0, `semaphore bucket resolves to an unknown Rust host: ${primarySemaphoreOwner}`);
      semaphoreEvidence = {
        semaphoreName, bucket: 0, ringKey: semaphoreRingKey,
        bucketOwner: primarySemaphoreOwner, bucketOwnerIndex: semaphoreOwnerIndex,
        createdThroughGoFrontend: true, ownerId: semaphoreOwner, tokenId: stored.token_id,
        concurrentGrants: acquired.length, noSlotResponses: unavailable.length,
        persistedHoldBeforeCrash: { ownerId: stored.owner_id, tokenId: stored.token_id },
        freshSemaphoreFirstAcquire: { semaphoreName: gapSemaphoreName, outcome: gapGrant.outcome },
      };
    }
    const primaryMembershipPeer = directMatchingClient(`127.0.0.1:${rustGrpcPort}`);
    const secondaryPeer = directMatchingClient(`127.0.0.1:${process.env.CADENCE_RUST_MATCHING_SECONDARY_GRPC_PORT}`);
    const primaryAddress = `127.0.0.1:${rustTChannel}`;
    const secondaryAddress = `127.0.0.1:${process.env.CADENCE_RUST_MATCHING_SECONDARY_TCHANNEL}`;
    const ownerProbeNames = Array.from({ length: 64 }, (_, index) => `ringpop-convergence-${id}-${index}`);
    const ringpopDeadline = Date.now() + 30000;
    let observedOwners = new Map();
    while (Date.now() < ringpopDeadline) {
      let observations;
      try {
      observations = await Promise.all(ownerProbeNames.map(async name => {
        const [primaryOwner, secondaryOwner] = await Promise.all([
          directPartitionOwner(primaryMembershipPeer, domain, name),
          directPartitionOwner(secondaryPeer, domain, name),
        ]);
        return [name, primaryOwner, secondaryOwner];
      }));
      } catch (error) {
        // Configstore startup can finish after the first node is discoverable.
        // A not-yet-bound second listener is not a membership observation.
        if (error.code !== grpc.status.UNAVAILABLE) throw error;
        if (secondaryRust.exitCode !== null || secondaryRust.signalCode !== null) {
          throw new Error('secondary Rust Matching exited during membership convergence');
        }
        lastConvergenceError = String(error);
        await new Promise(resolve => setTimeout(resolve, 250));
        continue;
      }
      observedOwners = new Map(observations.map(([name, primaryOwner, secondaryOwner]) => [name, { primaryOwner, secondaryOwner }]));
      const owners = new Set(observations.flatMap(([, primaryOwner, secondaryOwner]) => [primaryOwner, secondaryOwner]));
      if (observations.every(([, primaryOwner, secondaryOwner]) => primaryOwner === secondaryOwner)
          && owners.has(primaryAddress) && owners.has(secondaryAddress)) break;
      await new Promise(resolve => setTimeout(resolve, 250));
    }
    const convergedOwnership = [...observedOwners.values()];
    const convergedOwnerSet = new Set(convergedOwnership.map(owner => owner.primaryOwner));
    assert(convergedOwnership.length === ownerProbeNames.length
      && convergedOwnership.every(owner => owner.primaryOwner === owner.secondaryOwner)
      && convergedOwnerSet.has(primaryAddress) && convergedOwnerSet.has(secondaryAddress),
    'both Rust Matching members must expose the same Ringpop ownership map, including keys owned by each host');
    const idleList = { name: `production-idle-${id}` };
    const pendingIdlePoll = call('WorkerAPI', 'PollForDecisionTask', { domain, taskList: idleList, identity: 'production-idle-probe' });
    let idlePollSettled = 'pending';
    pendingIdlePoll.then(() => { idlePollSettled = 'resolved'; }, error => { idlePollSettled = `rejected: ${error.message}`; });
    const idleDeadline = Date.now() + 10000;
    for (;;) {
      const info = await call('WorkflowAPI', 'DescribeTaskList', { domain, taskList: idleList, taskListType: 'TASK_LIST_TYPE_DECISION', includeTaskListStatus: true });
      if (info.pollers.some(poller => poller.identity === 'production-idle-probe')) break;
      if (Date.now() > idleDeadline) throw new Error(`production idle probe poll was not registered (poll ${idlePollSettled}; describe ${JSON.stringify(info).slice(0, 400)}; sd ${JSON.stringify(await shardManagerCommand({ op: 'stats' })).slice(0, 1500)})`);
      await new Promise(resolve => setTimeout(resolve, 20));
    }
    const evictionDeadline = Date.now() + 30000;
    const idleResult = await Promise.race([pendingIdlePoll, new Promise(resolve => setTimeout(() => resolve(null), evictionDeadline - Date.now()))]);
    if (!idleResult) throw new Error('production Matching idle manager did not evict and release its long poll');
    assert.equal(idleResult.taskToken.length, 0, 'production idle manager eviction must release its long poll with an empty successful response');
    const idleRecoveryID = `production-idle-recovery-${id}`;
    await call('WorkflowAPI', 'StartWorkflowExecution', {
      ...startRequest, workflowId: idleRecoveryID, workflowType: { name: 'main.EchoWorkflow' },
      taskList: idleList, requestId: randomUUID(), input: payload(JSON.stringify('restart-proof')),
    });
    const idleSdk = spawnSync('go', ['run', '.'], { cwd: path.resolve(__dirname, 'go-sdk'), env: {
      ...process.env, CADENCE_MIXED_MATCHING_DOMAIN: domain, CADENCE_TCHANNEL_PEER: process.argv[6],
      CADENCE_MIXED_RESTART_WORKFLOW_ID: idleRecoveryID, CADENCE_MIXED_RESTART_TASK_LIST: idleList.name,
    }, encoding: 'utf8', timeout: 120000 });
    fs.writeFileSync(path.join(work, 'production-idle-recovery-sdk.log'), `${idleSdk.stdout || ''}\n${idleSdk.stderr || ''}`);
    if (idleSdk.status !== 0) throw new Error(`existing SDK could not complete work after production idle eviction: ${idleSdk.error || idleSdk.status}; see ${work}/production-idle-recovery-sdk.log`);
    const idleHistory = await call('WorkflowAPI', 'GetWorkflowExecutionHistory', { domain, workflowExecution: { workflowId: idleRecoveryID } });
    const idleCompletions = idleHistory.history.events.filter(event => event.workflowExecutionCompletedEventAttributes).length;
    assert.equal(idleCompletions, 1, 'production idle eviction recovery must complete exactly once');
    fs.writeFileSync(path.join(work, 'production-failure-recovery-checkpoints.json'), JSON.stringify({
      topology: 'two Rust Matching hosts with Go Frontend/History/Worker',
      sameRingOwnershipMap: convergedOwnership,
      idleEviction: {
        taskList: idleList.name, releasedWithEmptyResponse: idleResult.taskToken.length === 0,
        sdkCompletedWorkflow: idleRecoveryID, completionEvents: idleCompletions,
      },
    }, null, 2) + '\n');

    const crashList = `production-crash-${id}`;
    const crashProbeID = `production-crash-owner-probe-${id}`;
    const crashTaskList = { name: crashList };
    const probeStart = await call('WorkflowAPI', 'StartWorkflowExecution', {
      ...startRequest, workflowId: crashProbeID, workflowType: { name: 'main.EchoWorkflow' },
      taskList: crashTaskList, requestId: randomUUID(), input: payload(JSON.stringify('restart-proof')),
    });
    const directPeers = [
      directMatchingClient(`127.0.0.1:${rustGrpcPort}`),
      directMatchingClient(`127.0.0.1:${process.env.CADENCE_RUST_MATCHING_SECONDARY_GRPC_PORT}`),
    ];
    let directPolls;
    const directDeadline = Date.now() + 30000;
    while (Date.now() < directDeadline) {
      directPolls = await Promise.allSettled(directPeers.map((client, index) =>
        directDecisionPoll(client, domainId, crashList, `${index}-${randomUUID()}`)));
      if (directPolls.some(result => result.status === 'fulfilled' && result.value.taskToken?.length > 0)) break;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    const ownerIndex = directPolls?.findIndex(result => result.status === 'fulfilled' && result.value.taskToken?.length > 0) ?? -1;
    assert(ownerIndex >= 0, 'neither production Rust Matching instance delivered the queued owner-probe task');
    const staleResult = directPolls[1 - ownerIndex];
    assert.equal(staleResult.status, 'rejected', 'direct request to non-owner must be rejected with Go-compatible typed ownership');
    assert.equal(staleResult.reason.code, grpc.status.ABORTED, 'non-owner must return the typed TaskListNotOwnedByHost status');
    assert.match(staleResult.reason.details || '', /task list is not owned by this host/i);
    const nonOwnerIndex = 1 - ownerIndex;
    const nonOwnerGrpcPort = nonOwnerIndex === 0
      ? rustGrpcPort : process.env.CADENCE_RUST_MATCHING_SECONDARY_GRPC_PORT;
    const goTypedOwnership = spawnSync('go', ['run', './tests/matching-error-oracle'], {
      cwd: path.resolve(__dirname, '..', 'go'),
      env: {
        ...process.env,
        CADENCE_MATCHING_ERROR_TARGET: `127.0.0.1:${nonOwnerGrpcPort}`,
        CADENCE_MATCHING_ERROR_DOMAIN_ID: domainId,
        CADENCE_MATCHING_ERROR_DOMAIN: domain,
        CADENCE_MATCHING_ERROR_TASK_LIST: crashList,
        CADENCE_MATCHING_ERROR_EXPECTED_OWNER: ownerIndex === 0 ? primaryAddress : secondaryAddress,
        CADENCE_MATCHING_ERROR_EXPECTED_SELF: nonOwnerIndex === 0 ? primaryAddress : secondaryAddress,
      }, encoding: 'utf8', timeout: 120000,
    });
    fs.writeFileSync(path.join(work, 'production-typed-ownership-go.log'), `${goTypedOwnership.stdout || ''}\n${goTypedOwnership.stderr || ''}`);
    assert.equal(goTypedOwnership.status, 0, `pinned Go Matching client did not decode Rust's typed non-owner response; see ${work}/production-typed-ownership-go.log`);
    const ownerProbeTask = directPolls[ownerIndex].value;
    await call('WorkerAPI', 'RespondDecisionTaskCompleted', {
      taskToken: ownerProbeTask.taskToken,
      identity: 'production-owner-probe',
      decisions: [{ completeWorkflowExecutionDecisionAttributes: { result: payload('owner-probe') } }],
    });
    const probeHistory = await call('WorkflowAPI', 'GetWorkflowExecutionHistory', { domain, workflowExecution: { workflowId: crashProbeID, runId: probeStart.runId } });
    assert.equal(probeHistory.history.events.filter(event => event.workflowExecutionCompletedEventAttributes).length, 1, 'ownership probe must complete exactly once');

    const crashID = `production-crash-recovery-${id}`;
    const crashStart = await call('WorkflowAPI', 'StartWorkflowExecution', {
      ...startRequest, workflowId: crashID, workflowType: { name: 'main.EchoWorkflow' },
      taskList: crashTaskList, requestId: randomUUID(), input: payload(JSON.stringify('restart-proof')),
    });
    let queued;
    try {
      queued = await taskObserver.observe(domainId, crashList, 20, 'task-before-crash');
    } catch (error) {
      // Keep the persistence assertion strict and capture whether History dispatched
      // the task or Matching consumed it before the intended crash checkpoint.
      const diagnostics = await Promise.allSettled([
        call('WorkflowAPI', 'GetWorkflowExecutionHistory', {
          domain, workflowExecution: { workflowId: crashID, runId: crashStart.runId },
        }),
        call('WorkflowAPI', 'DescribeTaskList', {
          domain, taskList: crashTaskList, taskListType: 'TASK_LIST_TYPE_DECISION',
          includeTaskListStatus: true,
        }),
      ]);
      fs.writeFileSync(path.join(work, 'task-before-crash-diagnostics.json'), JSON.stringify({
        workflowId: crashID, runId: crashStart.runId, taskList: crashList,
        diagnostics: diagnostics.map(result => result.status === 'fulfilled'
          ? { status: result.status, value: result.value }
          : { status: result.status, error: String(result.reason) }),
      }, null, 2) + '\n');
      throw error;
    }
    assert(queued.queued_rows > 0, 'decision task was not visible in Matching persistence before production owner crash');
    const beforeCrash = await call('WorkflowAPI', 'DescribeTaskList', {
      domain, taskList: { name: crashList }, taskListType: 'TASK_LIST_TYPE_DECISION', includeTaskListStatus: true,
    });
    const beforeAck = Number(beforeCrash.taskListStatus.ackLevel);
    const beforeEnd = Number(beforeCrash.taskListStatus.taskIdBlock.endId);
    const rangeBefore = Number(queued.range_id);
    assert(rangeBefore > 0, 'task-list range was not leased before the owner crash');
    const failedPid = ownerIndex === 0 ? rust.pid : secondaryRust.pid;
    const secondaryExited = new Promise(resolve => secondaryRust.once('exit', (code, signal) => resolve({ code, signal })));
    if (ownerIndex === 0) rust.kill('SIGKILL');
    else secondaryRust.kill('SIGKILL');
    const failedExit = ownerIndex === 0 ? await exited : await secondaryExited;
    assert.equal(failedExit.signal, 'SIGKILL', 'selected Rust Matching owner did not crash');
    const survivor = ownerIndex === 0 ? secondaryRust : rust;
    const survivorDeadline = Date.now() + 45000;
    let survivorVisible = false;
    while (Date.now() < survivorDeadline) {
      if (survivor.exitCode !== null || survivor.signalCode !== null) throw new Error(`surviving Rust Matching exited; see ${ownerIndex === 0 ? `${work}/rust-server-secondary.log` : `${work}/rust-server-before-restart.log`}`);
      try {
        await call('WorkflowAPI', 'DescribeTaskList', {
          domain, taskList: { name: crashList }, taskListType: 'TASK_LIST_TYPE_DECISION', includeTaskListStatus: true,
        });
        survivorVisible = true;
        break;
      } catch (_) {
        await new Promise(resolve => setTimeout(resolve, 200));
      }
    }
    assert(survivorVisible, 'Go Frontend did not converge to the surviving Rust Matching host after primary loss');
    const crashSdk = spawnSync('go', ['run', '.'], { cwd: path.resolve(__dirname, 'go-sdk'), env: {
      ...process.env, CADENCE_MIXED_MATCHING_DOMAIN: domain, CADENCE_TCHANNEL_PEER: process.argv[6],
      CADENCE_MIXED_RESTART_WORKFLOW_ID: crashID, CADENCE_MIXED_RESTART_TASK_LIST: crashList,
    }, encoding: 'utf8', timeout: 120000 });
    fs.writeFileSync(path.join(work, 'production-crash-recovery-sdk.log'), `${crashSdk.stdout || ''}\n${crashSdk.stderr || ''}`);
    if (crashSdk.status !== 0) throw new Error(`existing SDK could not finish work recovered after Rust owner crash: ${crashSdk.error || crashSdk.status}; see ${work}/production-crash-recovery-sdk.log`);
    const crashHistory = await call('WorkflowAPI', 'GetWorkflowExecutionHistory', { domain, workflowExecution: { workflowId: crashID, runId: crashStart.runId } });
    const crashCompletions = crashHistory.history.events.filter(event => event.workflowExecutionCompletedEventAttributes).length;
    assert.equal(crashCompletions, 1, 'crash-recovered workflow must have exactly one visible completion');
    const recoveredStatus = await call('WorkflowAPI', 'DescribeTaskList', {
      domain, taskList: { name: crashList }, taskListType: 'TASK_LIST_TYPE_DECISION', includeTaskListStatus: true,
    });
    assert(Number(recoveredStatus.taskListStatus.taskIdBlock.endId) >= beforeEnd, 'task ID range moved backwards after Matching owner crash');
    const ackDeadline = Date.now() + 30000;
    let afterAck = Number(recoveredStatus.taskListStatus.ackLevel);
    while (afterAck <= beforeAck && Date.now() < ackDeadline) {
      await new Promise(resolve => setTimeout(resolve, 100));
      const status = await call('WorkflowAPI', 'DescribeTaskList', {
        domain, taskList: { name: crashList }, taskListType: 'TASK_LIST_TYPE_DECISION', includeTaskListStatus: true,
      });
      afterAck = Number(status.taskListStatus.ackLevel);
    }
    assert(afterAck > beforeAck, 'recovered task acknowledgement did not advance beyond the pre-crash level');
    const recoveredRows = await taskObserver.waitEmpty(domainId, crashList, 30, 'task-after-recovery');
    const rangeAfter = Number(recoveredRows?.range_id);
    assert(rangeAfter > rangeBefore, 'surviving Rust Matching did not acquire a higher persisted task-list range');
    const afterCount = Number(recoveredRows?.queued_rows);
    assert.equal(afterCount, 0, 'recovered task remained queued after SDK completion');
    fs.writeFileSync(path.join(work, 'production-failure-recovery-checkpoints.json'), JSON.stringify({
      topology: 'two Rust Matching hosts with Go Frontend/History/Worker',
      sameRingOwnershipMap: convergedOwnership,
      idleEviction: {
        taskList: idleList.name, releasedWithEmptyResponse: idleResult.taskToken.length === 0,
        sdkCompletedWorkflow: idleRecoveryID, completionEvents: idleCompletions,
      },
      crashRecovery: {
        taskList: crashList, crashedPid: failedPid, crashedHostIndex: ownerIndex,
        survivorPid: survivor.pid, workflowId: crashID, completionEvents: crashCompletions,
        rangeBefore, rangeAfter, ackBefore: beforeAck, ackAfter: afterAck,
        queuedRowsAfterCompletion: afterCount,
      },
      cassandraSemaphore: semaphoreEvidence,
      cassandraDomainPageToken: cassandraGate ? { pages: domainPages, domains: [domain, pageTokenDomain] } : null,
    }, null, 2) + '\n');
    if (ownerIndex === 0) {
      rust = startRust('after-failure-recovery');
      await waitForRustReady('after-failure-recovery');
    } else {
      secondaryRust = startSecondaryRust();
      const secondaryReadyDeadline = Date.now() + 30000;
      let secondaryReady = false;
      while (Date.now() < secondaryReadyDeadline) {
        try {
          await call('WorkflowAPI', 'DescribeTaskList', { domain, taskList: { name: `secondary-restarted-${id}` }, taskListType: 'TASK_LIST_TYPE_DECISION', includeTaskListStatus: true });
          secondaryReady = true;
          break;
        } catch (_) { await new Promise(resolve => setTimeout(resolve, 100)); }
      }
      assert(secondaryReady, 'restarted Rust Matching member did not rejoin Go discovery');
    }
    if (process.env.CADENCE_MATCHING_POSTGRES) {
      // Separate decision and activity lists let the Go SDK create real activity
      // work while its seed worker polls only decisions. The activity remains
      // durable and unconsumed until the API-reported Rust owner is SIGKILLed.
      const activityCrashId = `production-activity-crash-${id}`;
      const activityWorkflowTaskList = `production-activity-decisions-${id}`;
      const activityTaskList = `production-activity-queue-${id}`;
      const seedOutput = fs.openSync(path.join(work, 'production-activity-crash-seed-sdk.log'), 'w');
      const seedSdk = spawn('go', ['run', '.'], {
        cwd: path.resolve(__dirname, 'go-sdk'), detached: true,
        env: {
          ...process.env, CADENCE_MIXED_MATCHING_DOMAIN: domain,
          CADENCE_TCHANNEL_PEER: process.argv[6], CADENCE_MIXED_ACTIVITY_CRASH_SEED: '1',
          CADENCE_MIXED_ACTIVITY_CRASH_WORKFLOW_ID: activityCrashId,
          CADENCE_MIXED_ACTIVITY_CRASH_WORKFLOW_TASK_LIST: activityWorkflowTaskList,
          CADENCE_MIXED_ACTIVITY_CRASH_ACTIVITY_TASK_LIST: activityTaskList,
        }, stdio: ['ignore', seedOutput, seedOutput],
      });
      fs.closeSync(seedOutput);
      const cleanupSeedSdk = () => { try { process.kill(-seedSdk.pid, 'SIGKILL'); } catch (_) {} };
      process.once('exit', cleanupSeedSdk);
      const seedExited = new Promise(resolve => seedSdk.once('exit', (code, signal) => resolve({ code, signal })));
      let activityQueued;
      try {
        activityQueued = await taskObserver.observe(domainId, activityTaskList, 45,
          'production-activity-task-before-owner-crash', 1);
      assert.equal(activityQueued.queued_rows, 1,
        'existing Go SDK must persist exactly one activity task before owner crash');
      assert(Number(activityQueued.range_id) > 0,
        'activity task-list range row must be visible before owner crash');
      } catch (error) {
        cleanupSeedSdk();
        await Promise.race([seedExited, new Promise(resolve => setTimeout(resolve, 5000))]);
        process.removeListener('exit', cleanupSeedSdk);
        throw error;
      }
      const activityPeers = [
        directMatchingClient(`127.0.0.1:${rustGrpcPort}`),
        directMatchingClient(`127.0.0.1:${process.env.CADENCE_RUST_MATCHING_SECONDARY_GRPC_PORT}`),
      ];
      const [activityPrimaryOwner, activitySecondaryOwner] = await Promise.all(activityPeers.map(client =>
        directPartitionOwner(client, domain, activityTaskList, 'activity')));
      assert(activityPrimaryOwner && activityPrimaryOwner === activitySecondaryOwner,
        `Rust Matching members disagree on activity owner for ${activityTaskList}: ${activityPrimaryOwner} vs ${activitySecondaryOwner}`);
      const activityOwnerIndex = activityPrimaryOwner === primaryAddress ? 0
        : activityPrimaryOwner === secondaryAddress ? 1 : -1;
      assert(activityOwnerIndex >= 0,
        `activity owner ${activityPrimaryOwner} is not one of the two Rust Matching members`);
      const activityOwnerProcess = activityOwnerIndex === 0 ? rust : secondaryRust;
      const activitySurvivorProcess = activityOwnerIndex === 0 ? secondaryRust : rust;
      const activitySurvivorAddress = activityOwnerIndex === 0 ? secondaryAddress : primaryAddress;
      const activityOwnerExitPromise = new Promise(resolve => activityOwnerProcess.once('exit', (code, signal) => resolve({ code, signal })));
      activityOwnerProcess.kill('SIGKILL');
      const activityOwnerExit = await activityOwnerExitPromise;
      assert.equal(activityOwnerExit.signal, 'SIGKILL', 'selected activity task owner did not SIGKILL');
      const activityRecoveryDeadline = Date.now() + 45000;
      let recoveredActivityOwner = '';
      while (Date.now() < activityRecoveryDeadline) {
        if (activitySurvivorProcess.exitCode !== null || activitySurvivorProcess.signalCode !== null) {
          throw new Error('surviving Rust Matching exited during queued activity recovery');
        }
        try {
          recoveredActivityOwner = await directPartitionOwner(activityPeers[1 - activityOwnerIndex], domain, activityTaskList, 'activity');
          if (recoveredActivityOwner === activitySurvivorAddress) break;
        } catch (_) {}
        await new Promise(resolve => setTimeout(resolve, 200));
      }
      assert.equal(recoveredActivityOwner, activitySurvivorAddress,
        'surviving Rust Matching did not become the reported owner of the durable activity partition');
      const recoveredActivitySdk = spawnSync('go', ['run', '.'], {
        cwd: path.resolve(__dirname, 'go-sdk'), env: {
          ...process.env, CADENCE_MIXED_MATCHING_DOMAIN: domain,
          CADENCE_TCHANNEL_PEER: process.argv[6],
          CADENCE_MIXED_ACTIVITY_CRASH_RECOVERY_WORKFLOW_ID: activityCrashId,
          CADENCE_MIXED_ACTIVITY_CRASH_WORKFLOW_TASK_LIST: activityWorkflowTaskList,
          CADENCE_MIXED_ACTIVITY_CRASH_ACTIVITY_TASK_LIST: activityTaskList,
        }, encoding: 'utf8', timeout: 180000,
      });
      fs.writeFileSync(path.join(work, 'production-activity-crash-recovery-sdk.log'),
        `${recoveredActivitySdk.stdout || ''}\n${recoveredActivitySdk.stderr || ''}`);
      cleanupSeedSdk();
      const seedExit = await Promise.race([seedExited, new Promise(resolve => setTimeout(() => resolve(null), 5000))]);
      assert(seedExit, 'activity crash seed Go SDK process group did not exit after cleanup');
      assert.equal(seedExit.signal, 'SIGKILL', 'activity crash seed worker unexpectedly exited before cleanup');
      process.removeListener('exit', cleanupSeedSdk);
      assert.equal(recoveredActivitySdk.status, 0,
        `existing Go SDK could not consume the queued activity after owner crash: ${recoveredActivitySdk.error || recoveredActivitySdk.status}; see ${work}/production-activity-crash-recovery-sdk.log`);
      const recoveredActivityHistory = await call('WorkflowAPI', 'GetWorkflowExecutionHistory', {
        domain, workflowExecution: { workflowId: activityCrashId },
      });
      const recoveredActivityCompletions = recoveredActivityHistory.history.events
        .filter(event => event.workflowExecutionCompletedEventAttributes).length;
      const recoveredActivityEvents = recoveredActivityHistory.history.events
        .filter(event => event.activityTaskCompletedEventAttributes).length;
      const recoveredActivityScheduled = recoveredActivityHistory.history.events
        .filter(event => event.activityTaskScheduledEventAttributes).length;
      const recoveredActivityStarted = recoveredActivityHistory.history.events
        .filter(event => event.activityTaskStartedEventAttributes).length;
      const recoveredActivityTimeouts = recoveredActivityHistory.history.events
        .filter(event => event.activityTaskTimedOutEventAttributes).length;
      const recoveredActivityFailures = recoveredActivityHistory.history.events
        .filter(event => event.activityTaskFailedEventAttributes).length;
      assert.equal(recoveredActivityScheduled, 1, 'crash-recovered activity must be scheduled exactly once');
      assert.equal(recoveredActivityStarted, 1, 'crash-recovered activity must start exactly once');
      assert.equal(recoveredActivityTimeouts, 0, 'crash-recovered activity must not time out or be retried');
      assert.equal(recoveredActivityFailures, 0, 'crash-recovered activity must not fail before its successful attempt');
      assert.equal(recoveredActivityEvents, 1, 'crash-recovered activity must complete exactly once');
      assert.equal(recoveredActivityCompletions, 1, 'crash-recovered activity workflow must complete exactly once');
      const activityQueueAfterRecovery = await taskObserver.waitEmpty(domainId, activityTaskList, 30,
        'production-activity-task-after-owner-recovery', 1);
      assert.equal(activityQueueAfterRecovery.queued_rows, 0,
        'crash-recovered activity remained in durable Matching persistence');
      const activityCrashEvidence = {
        topology: 'Go Frontend/History/Worker with two Rust Matching members; no Go Matching',
        workflowId: activityCrashId,
        workflowTaskList: activityWorkflowTaskList, activityTaskList,
        durableActivityRowsBeforeCrash: activityQueued.queued_rows,
        activityOwnerBeforeCrash: activityPrimaryOwner, crashedPid: activityOwnerProcess.pid,
        crashSignal: activityOwnerExit.signal, survivorPid: activitySurvivorProcess.pid,
        activityOwnerAfterCrash: recoveredActivityOwner,
        activityCompletionEvents: recoveredActivityEvents, workflowCompletionEvents: recoveredActivityCompletions,
        activityScheduledEvents: recoveredActivityScheduled, activityStartedEvents: recoveredActivityStarted,
        activityTimeoutEvents: recoveredActivityTimeouts, activityFailureEvents: recoveredActivityFailures,
        durableActivityRowsAfterRecovery: activityQueueAfterRecovery.queued_rows,
        sdkResultAssertion: 'activity-crash-recovered',
      };
      if (activityOwnerIndex === 0) {
        rust = startRust('after-activity-owner-crash');
        await waitForRustReady('after-activity-owner-crash');
      } else {
        secondaryRust = startSecondaryRust();
        await waitForRustReady('after-activity-owner-crash');
      }
      const restartedActivityAddress = activityOwnerIndex === 0 ? primaryAddress : secondaryAddress;
      const activityRejoinKeys = Array.from({ length: 64 }, (_, index) =>
        `activity-rejoin-${id}-${index}`);
      const activityRejoinDeadline = Date.now() + 30000;
      let activityMemberRejoined = false;
      let activityRejoinOwnership = [];
      while (Date.now() < activityRejoinDeadline) {
        try {
          activityRejoinOwnership = await Promise.all(activityRejoinKeys.map(async taskListName => {
            const [primaryOwner, secondaryOwner] = await Promise.all(activityPeers.map(client =>
              directPartitionOwner(client, domain, taskListName, 'activity')));
            return { taskListName, primaryOwner, secondaryOwner };
          }));
          const converged = activityRejoinOwnership.every(({ primaryOwner, secondaryOwner }) =>
            primaryOwner && primaryOwner === secondaryOwner);
          const restartedOwnsActivityPartition = activityRejoinOwnership.some(({ primaryOwner }) =>
            primaryOwner === restartedActivityAddress);
          if (!converged || !restartedOwnsActivityPartition) {
            throw new Error('activity partition ownership has not converged onto the restarted member');
          }
          await call('WorkflowAPI', 'DescribeTaskList', {
            domain, taskList: { name: `activity-owner-restarted-${id}` },
            taskListType: 'TASK_LIST_TYPE_ACTIVITY', includeTaskListStatus: true,
          });
          activityMemberRejoined = true;
          break;
        } catch (_) { await new Promise(resolve => setTimeout(resolve, 100)); }
      }
      assert(activityMemberRejoined,
        `restarted Rust Matching member ${restartedActivityAddress} did not appear as an activity partition owner in both converged Rust views and become describable through Go Frontend`);
      activityCrashEvidence.restartedMemberAddress = restartedActivityAddress;
      activityCrashEvidence.activityRejoinPartitionCount = activityRejoinOwnership.length;
      activityCrashEvidence.activityRejoinOwnership = activityRejoinOwnership;
      activityCrashEvidence.rejoinEvidence = 'restarted address owns at least one activity partition; all 64 direct owner observations agree across both Rust endpoints; Go Frontend activity DescribeTaskList succeeds';
      fs.writeFileSync(path.join(work, 'production-activity-crash-recovery.json'),
        JSON.stringify(activityCrashEvidence, null, 2) + '\n');
    }
    if (cassandraGate && semaphoreEvidence) {
      const heldBeforeOwnerCrash = cassandraCommand(['verify-semaphore', domainId, semaphoreEvidence.semaphoreName, semaphoreOwner, String(semaphoreEvidence.tokenId)], 'semaphore-immediately-before-owner-crash');
      assert.equal(heldBeforeOwnerCrash.owner_id, semaphoreOwner);
      assert.equal(heldBeforeOwnerCrash.token_id, semaphoreEvidence.tokenId);
      const semaphoreFailed = semaphoreEvidence.bucketOwnerIndex === 0 ? rust : secondaryRust;
      const semaphoreSurvivor = semaphoreEvidence.bucketOwnerIndex === 0 ? secondaryRust : rust;
      const semaphoreFailedExitPromise = new Promise(resolve => semaphoreFailed.once('exit', (code, signal) => resolve({ code, signal })));
      semaphoreFailed.kill('SIGKILL');
      const semaphoreFailedExit = await semaphoreFailedExitPromise;
      assert.equal(semaphoreFailedExit.signal, 'SIGKILL', 'persisted semaphore bucket owner did not crash');
      const survivorPeer = directMatchingClient(`127.0.0.1:${semaphoreEvidence.bucketOwnerIndex === 0 ? process.env.CADENCE_RUST_MATCHING_SECONDARY_GRPC_PORT : rustGrpcPort}`);
      const recoveryDeadline = Date.now() + 45000;
      let recoveredOwner = '';
      while (Date.now() < recoveryDeadline) {
        try {
          recoveredOwner = await directPartitionOwner(survivorPeer, domain, semaphoreEvidence.ringKey);
          if (recoveredOwner === (semaphoreEvidence.bucketOwnerIndex === 0
            ? `127.0.0.1:${process.env.CADENCE_RUST_MATCHING_SECONDARY_TCHANNEL}` : `127.0.0.1:${rustTChannel}`)) break;
        } catch (_) {}
        await new Promise(resolve => setTimeout(resolve, 200));
      }
      assert(recoveredOwner, 'surviving Rust host did not resolve the semaphore bucket after owner crash');
      const recovered = await acquireSemaphore(survivorPeer, { domainId, semaphoreName: semaphoreEvidence.semaphoreName, bucket: 0, ownerId: semaphoreOwner });
      assert.equal(Number(recovered.tokenId), semaphoreEvidence.tokenId, 'surviving semaphore owner must recover the persisted hold without granting a new token');
      const afterRestart = cassandraCommand(['verify-semaphore', domainId, semaphoreEvidence.semaphoreName, semaphoreOwner, String(semaphoreEvidence.tokenId)], 'semaphore-after-owner-recovery');
      assert.equal(afterRestart.owner_id, semaphoreOwner);
      assert.equal(afterRestart.token_id, semaphoreEvidence.tokenId);
      semaphoreEvidence.ownerCrash = {
        crashedPid: semaphoreFailed.pid, survivorPid: semaphoreSurvivor.pid,
        persistedHoldBeforeCrash: { ownerId: heldBeforeOwnerCrash.owner_id, tokenId: heldBeforeOwnerCrash.token_id },
        persistedHoldAfterRecovery: { ownerId: afterRestart.owner_id, tokenId: afterRestart.token_id },
        recoveredOwner, recoveredTokenId: Number(recovered.tokenId),
      };
      const release = spawnSync('go', ['run', './tests/matching-cassandra-interop', 'release-semaphore', domainId, semaphoreEvidence.semaphoreName, semaphoreOwner, String(semaphoreEvidence.tokenId)], {
        cwd: path.resolve(__dirname, '..', 'go'), env: process.env, encoding: 'utf8', timeout: 120000,
      });
      fs.writeFileSync(path.join(work, 'semaphore-release.log'), `${release.stdout || ''}\n${release.stderr || ''}`);
      assert.equal(release.status, 0, `Go persistence release protocol failed; see ${work}/semaphore-release.log`);
      const released = cassandraCommand(['verify-semaphore-free', domainId, semaphoreEvidence.semaphoreName, semaphoreOwner, String(semaphoreEvidence.tokenId)], 'semaphore-after-release');
      assert.equal(released.released_owner_absent, true);
      assert.equal(released.holder_free, true);

      // The recovered process has a cache built while the hold existed. Restart
      // it after the actual Go persistence release so a fresh manager must load
      // the now-free row before a new owner can claim it.
      const recoveredOwnerIndex = 1 - semaphoreEvidence.bucketOwnerIndex;
      const recoveredOwnerProcess = recoveredOwnerIndex === 0 ? rust : secondaryRust;
      const remainingOwnerIndex = semaphoreEvidence.bucketOwnerIndex;
      const remainingPeer = directMatchingClient(`127.0.0.1:${remainingOwnerIndex === 0 ? rustGrpcPort : process.env.CADENCE_RUST_MATCHING_SECONDARY_GRPC_PORT}`);
      // The originally crashed bucket owner is still dead here; bring it back so a
      // second host exists to own the released bucket once the recovered one is killed.
      if (remainingOwnerIndex === 0) {
        rust = startRust('after-semaphore-owner-crash');
        await waitForRustReady('after-semaphore-owner-crash');
      } else {
        secondaryRust = startSecondaryRust();
        const rejoinDeadline = Date.now() + 30000;
        let rejoined = false;
        while (Date.now() < rejoinDeadline) {
          try {
            await call('WorkflowAPI', 'DescribeTaskList', { domain, taskList: { name: `secondary-semaphore-owner-restarted-${id}` }, taskListType: 'TASK_LIST_TYPE_DECISION', includeTaskListStatus: true });
            rejoined = true;
            break;
          } catch (_) { await new Promise(resolve => setTimeout(resolve, 100)); }
        }
        assert(rejoined, 'restarted semaphore bucket owner did not rejoin Go discovery');
      }
      const recoveredOwnerExitPromise = new Promise(resolve => recoveredOwnerProcess.once('exit', (code, signal) => resolve({ code, signal })));
      recoveredOwnerProcess.kill('SIGKILL');
      const recoveredOwnerExit = await recoveredOwnerExitPromise;
      assert.equal(recoveredOwnerExit.signal, 'SIGKILL', 'semaphore recovery host did not restart after persisted release');
      const targetOwner = remainingOwnerIndex === 0
        ? `127.0.0.1:${rustTChannel}` : `127.0.0.1:${process.env.CADENCE_RUST_MATCHING_SECONDARY_TCHANNEL}`;
      const freshOwnerDeadline = Date.now() + 45000;
      let freshBucketOwner = '';
      while (Date.now() < freshOwnerDeadline) {
        try {
          freshBucketOwner = await directPartitionOwner(remainingPeer, domain, semaphoreEvidence.ringKey);
          if (freshBucketOwner === targetOwner) break;
        } catch (_) {}
        await new Promise(resolve => setTimeout(resolve, 200));
      }
      assert.equal(freshBucketOwner, targetOwner, 'remaining Rust host did not own the released semaphore bucket');
      const freshOwnerID = `${id.length}:${id}:${randomUUID()}:3`;
      const freshGrant = await acquireSemaphore(remainingPeer, {
        domainId, semaphoreName: semaphoreEvidence.semaphoreName, bucket: 0, ownerId: freshOwnerID,
      });
      assert.equal(freshGrant.outcome, 'SEMAPHORE_ACQUIRE_OUTCOME_ACQUIRED', 'fresh owner did not acquire the released semaphore slot');
      assert.equal(Number(freshGrant.tokenId), semaphoreEvidence.tokenId, 'fresh owner must acquire the released token slot');
      const freshPersisted = cassandraCommand(['verify-semaphore', domainId, semaphoreEvidence.semaphoreName, freshOwnerID, String(semaphoreEvidence.tokenId)], 'semaphore-fresh-owner-after-release');
      assert.equal(freshPersisted.owner_id, freshOwnerID);
      assert.equal(freshPersisted.token_id, semaphoreEvidence.tokenId);
      semaphoreEvidence.releaseAndFreshAcquire = {
        releaseFixture: 'Cassandra storage fixture mirroring Go CDB.ReleaseSemaphoreToken; not SDK/API release evidence',
        releaseFixtureOutput: release.stdout.trim(),
        oldOwnerAbsentAndTokenFree: released,
        freshOwnerId: freshOwnerID, freshTokenId: Number(freshGrant.tokenId),
        persistedFreshHold: freshPersisted,
      };

      if (recoveredOwnerIndex === 0) {
        rust = startRust('after-semaphore-release-recovery');
        await waitForRustReady('after-semaphore-release-recovery');
      } else {
        secondaryRust = startSecondaryRust();
        const readyDeadline = Date.now() + 30000;
        let ready = false;
        while (Date.now() < readyDeadline) {
          try {
            await call('WorkflowAPI', 'DescribeTaskList', { domain, taskList: { name: `secondary-semaphore-restarted-${id}` }, taskListType: 'TASK_LIST_TYPE_DECISION', includeTaskListStatus: true });
            ready = true;
            break;
          } catch (_) { await new Promise(resolve => setTimeout(resolve, 100)); }
        }
        assert(ready, 'restarted semaphore recovery host did not rejoin Go discovery');
      }
    }
  }
  // The persisted watermark of a queue is read while its task is stored and
  // undelivered. A task that a poller already holds may be collected before
  // the poll returns, so the watermark is never read after a delivery.
  const awaitPersistedTask = async (listName, taskType, timeoutMs = 20000) => {
    if (!process.env.CADENCE_MATCHING_MYSQL) return null;
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const rows = mysqlRows(`SELECT COUNT(*), MAX(task_id) FROM tasks WHERE domain_id=UNHEX('${domainHex(domainId)}') AND task_list_name='${sqlText(listName)}' AND task_type=${taskType}`);
      const count = Number(rows[0][0]);
      const value = rows[0][1] && rows[0][1] !== 'NULL' ? Number(rows[0][1]) : null;
      if (count > 0 && value !== null && Number.isFinite(value)) return { taskType, rows: count, maxTaskId: value };
      if (Date.now() >= deadline) return { taskType, rows: count, maxTaskId: value === null ? null : value };
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  };
  // Completes the first decision task of a workflow by scheduling exactly the
  // activity the existing SDK's EchoWorkflow schedules, so the activity row is
  // persisted and undelivered while its watermark is read. The SDK then
  // replays the same history and drains the activity.
  const scheduleEchoActivityAndObserve = async (listName, runId, label) => {
    const taskList = { name: listName };
    let decision;
    const deadline = Date.now() + 30000;
    while (Date.now() < deadline) {
      decision = await call('WorkerAPI', 'PollForDecisionTask', { domain, taskList, identity: 'gc-probe' });
      if (decision.taskToken?.length > 0) break;
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    assert(decision?.taskToken?.length > 0, `${label}: no decision task was delivered on ${listName}`);
    assert.equal(decision.workflowExecution?.runId, runId, `${label}: decision task belongs to a different run`);
    await call('WorkerAPI', 'RespondDecisionTaskCompleted', {
      taskToken: decision.taskToken, identity: 'gc-probe', decisions: [{ scheduleActivityTaskDecisionAttributes: {
        activityId: '0', activityType: { name: 'main.EchoActivity' }, taskList,
        input: payload('"restart-proof"\n'),
        scheduleToCloseTimeout: { seconds: '120' }, scheduleToStartTimeout: { seconds: '60' },
        startToCloseTimeout: { seconds: '60' }, heartbeatTimeout: { seconds: '0' },
      } }],
    });
    const watermark = await awaitPersistedTask(listName, 1);
    assert(watermark && watermark.maxTaskId !== null, `${label}: the activity task row was not observed before delivery`);
    return watermark;
  };
  const restartState = { taskList: null, watermark: null, activityWatermark: null };
  let restartEvidence;
  if (restartMode) {
    const restartID = `restart-${id}`, restartTaskList = `restart-tasks-${id}`;
    restartState.taskList = restartTaskList;
    const restartRequest = { ...startRequest, workflowId: restartID, workflowType: { name: 'main.EchoWorkflow' }, taskList: { name: restartTaskList }, requestId: randomUUID(), input: payload(JSON.stringify('restart-proof')) };
    let restartStarted;
    const restartProvisionDeadline = Date.now() + 15000;
    for (;;) {
      try { restartStarted = await call('WorkflowAPI', 'StartWorkflowExecution', restartRequest); break; }
      catch (error) {
        if (!error.details?.includes('DefaultDomainCache encounter case') || Date.now() >= restartProvisionDeadline) throw error;
        await new Promise(resolve => setTimeout(resolve, 200));
      }
    }
    const persisted = await taskObserver.observe(domainId, restartTaskList, 20, 'task-before-restart');
    assert(persisted.queued_rows > 0, 'workflow task was not persisted before Matching restart');
    // The persisted watermark is read while the row is stored and undelivered.
    restartState.watermark = await awaitPersistedTask(restartTaskList, 0);
    const beforePid = rust.pid;
    const sdBeforeKill = await sdAssertHeartbeat('before-sigkill');
    // Only the SIGKILLed primary's executor may expire; the secondary Rust node keeps heartbeating.
    const sdBeforeKillStats = await shardManagerCommand({ op: 'stats' }) || {};
    const sdPrimaryPort = process.env.CADENCE_RUST_MATCHING_PRODUCTION_GRPC_PORT;
    const sdKilledIds = Object.entries(sdBeforeKillStats).filter(([, e]) => sdMatchesGrpc(e, sdPrimaryPort)
      && Date.now() - sdHeartbeatMs(e) < 10000).map(([id]) => id);
    const sdSelection = { primaryGrpcPort: sdPrimaryPort, selected: sdKilledIds.map(id => ({ id, metadata: sdMetadata(sdBeforeKillStats[id]) })),
      observed: Object.entries(sdBeforeKillStats).map(([id, e]) => ({ id, metadata: sdMetadata(e), lastHeartbeat: e.LastHeartbeat || e.lastHeartbeat || null })) };
    if (sdBeforeKill && sdKilledIds.length !== 1) {
      const diagnosticPath = path.join(work, 'shard-distributor-before-sigkill-selection-failure.json');
      fs.writeFileSync(diagnosticPath, `${JSON.stringify(sdSelection, null, 2)}\n`);
      throw new Error(`shard-distributor before-sigkill: expected exactly one fresh executor advertising primary gRPC port ${sdPrimaryPort}, found ${sdKilledIds.length}; diagnostic snapshot: ${diagnosticPath}`);
    }
    if (sdBeforeKill) shardDistributorEvidence.push({ check: 'sigkill-target-selected', ...sdSelection });
    const sdKilledAt = Date.now();
    rust.kill('SIGKILL');
    const beforeExit = await exited;
    assert.equal(beforeExit.signal, 'SIGKILL', 'first Rust Matching process did not terminate');
    if (goServicesPid) process.kill(goServicesPid, 0);
    rust = startRust('after-restart');
    await waitForRustReady('after-restart');
    if (sdBeforeKill) {
      await sdAssertExecutorExpired('after-sigkill', sdKilledIds, sdKilledAt);
      await sdAssertHeartbeat('after-sigkill-restart', { excludeIds: [] });
      await sdReassign('after-sigkill-restart', restartTaskList);
    }
    if (process.env.CADENCE_MATCHING_MYSQL) restartState.activityWatermark = await scheduleEchoActivityAndObserve(restartTaskList, restartStarted.runId, 'restart');
    const sdk = spawnSync('go', ['run', '.'], { cwd: path.resolve(__dirname, 'go-sdk'), env: { ...process.env, CADENCE_MIXED_MATCHING_DOMAIN: domain, CADENCE_TCHANNEL_PEER: process.argv[6], CADENCE_MIXED_RESTART_WORKFLOW_ID: restartID, CADENCE_MIXED_RESTART_TASK_LIST: restartTaskList }, encoding: 'utf8', timeout: 120000 });
    fs.writeFileSync(path.join(work, 'restart-sdk.log'), `${sdk.stdout || ''}\n${sdk.stderr || ''}`);
    if (sdk.status !== 0) throw new Error(`Existing SDK did not complete pre-restart workflow/activity: ${sdk.error || sdk.status}; see ${work}/restart-sdk.log`);
    const finalHistory = await call('WorkflowAPI', 'GetWorkflowExecutionHistory', { domain, workflowExecution: { workflowId: restartID, runId: restartStarted.runId } });
    const completions = finalHistory.history.events.filter(event => event.workflowExecutionCompletedEventAttributes).length;
    assert.equal(completions, 1, 'restart workflow must have exactly one visible completion');
    if (sdBeforeKill) await sdRestartScenario('restart-flow', domain, restartID);
    restartEvidence = { workflowId: restartID, runId: restartStarted.runId, taskList: restartTaskList, durableTaskRowsBeforeKill: persisted.queued_rows, taskRangeBeforeKill: persisted.range_id, durableWatermarkBeforeKill: restartState.watermark, firstRustPid: beforePid, secondRustPid: rust.pid, completionEvents: completions, goServicesPid };
    // Reuse the second live process for the existing compatibility coverage below.
  }
  let started;
  const provisionDeadline = Date.now() + 15000;
  for (;;) {
    try { started = await call('WorkflowAPI', 'StartWorkflowExecution', startRequest); break; }
    catch (error) {
      if (!error.details?.includes('DefaultDomainCache encounter case') || Date.now() >= provisionDeadline) throw error;
      await new Promise(resolve => setTimeout(resolve, 200));
    }
  }
  const execution = { workflowId: id, runId: started.runId };
  const mainDecisionWatermark = await awaitPersistedTask(taskList.name, 0);
  let decision;
  const decisionDeadline = Date.now() + 30000;
  while (Date.now() < decisionDeadline) {
    decision = await call('WorkerAPI', 'PollForDecisionTask', { domain, taskList, identity: 'service-probe' });
    if (decision.taskToken?.length > 0) break;
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  assert(decision?.taskToken?.length > 0, `Go Frontend did not return the started workflow task within 30s; task list ${taskList.name}`);
  assert.equal(decision.workflowExecution?.runId, started.runId, 'Go Frontend returned a decision task for a different workflow run');
  const described = await call('WorkflowAPI', 'DescribeTaskList', { domain, taskList, taskListType: 'TASK_LIST_TYPE_DECISION', includeTaskListStatus: true });
  assert(described.pollers.some(poller => poller.identity === 'service-probe'));
  assert.equal(described.taskListStatus.taskIdBlock.startId, '1');
  assert.equal(described.taskListStatus.taskIdBlock.endId, '100000');
  await call('WorkerAPI', 'RespondDecisionTaskCompleted', {
    taskToken: decision.taskToken, identity: 'service-probe', decisions: [{ scheduleActivityTaskDecisionAttributes: {
      activityId: 'echo', activityType: { name: 'Echo' }, taskList, input: payload('activity'),
      scheduleToCloseTimeout: { seconds: '60' }, scheduleToStartTimeout: { seconds: '30' },
      startToCloseTimeout: { seconds: '30' }, heartbeatTimeout: { seconds: '10' },
    } }],
  });
  const mainActivityWatermark = await awaitPersistedTask(taskList.name, 1);
  const activity = await call('WorkerAPI', 'PollForActivityTask', { domain, taskList, identity: 'service-probe' });
  assert.equal(activity.input.data.toString(), 'activity');
  assert.equal((await call('WorkerAPI', 'RecordActivityTaskHeartbeat', { taskToken: activity.taskToken, details: payload('heartbeat') })).cancelRequested, false);
  await call('WorkerAPI', 'RespondActivityTaskCompleted', { taskToken: activity.taskToken, result: payload('activity-result'), identity: 'service-probe' });
  const finalWatermark = await awaitPersistedTask(taskList.name, 0);
  const final = await call('WorkerAPI', 'PollForDecisionTask', { domain, taskList, identity: 'service-probe' });
  await call('WorkerAPI', 'RespondDecisionTaskCompleted', {
    taskToken: final.taskToken, identity: 'service-probe', decisions: [{ completeWorkflowExecutionDecisionAttributes: { result: payload('done') } }],
  });
  const history = await call('WorkflowAPI', 'GetWorkflowExecutionHistory', { domain, workflowExecution: execution });
  const events = history.history.events;
  const attributes = events.map(event => Object.keys(event).find(key => key.endsWith('EventAttributes') && event[key] != null));
  assert.deepEqual(attributes, [
    'workflowExecutionStartedEventAttributes', 'decisionTaskScheduledEventAttributes', 'decisionTaskStartedEventAttributes',
    'decisionTaskCompletedEventAttributes', 'activityTaskScheduledEventAttributes', 'activityTaskStartedEventAttributes',
    'activityTaskCompletedEventAttributes', 'decisionTaskScheduledEventAttributes', 'decisionTaskStartedEventAttributes',
    'decisionTaskCompletedEventAttributes', 'workflowExecutionCompletedEventAttributes',
  ]);
  assert.equal(events.at(-1).workflowExecutionCompletedEventAttributes.result.data.toString(), 'done');
  let gracefulDrainEvidence;
  if (productionMode) {
    const drainID = `graceful-drain-${id}`, drainTaskList = `graceful-drain-tasks-${id}`;
    const drainRequest = { ...startRequest, workflowId: drainID, workflowType: { name: 'main.EchoWorkflow' }, taskList: { name: drainTaskList }, requestId: randomUUID(), input: payload(JSON.stringify('restart-proof')) };
    const drainStarted = await call('WorkflowAPI', 'StartWorkflowExecution', drainRequest);
    if (!cassandraGate && !process.env.CADENCE_MATCHING_SQLITE_DB && !process.env.CADENCE_MATCHING_MYSQL && !process.env.CADENCE_MATCHING_POSTGRES) throw new Error('production Matching mode requires a configured persistence probe target');
    const queued = await taskObserver.observe(domainId, drainTaskList, 20, 'task-before-graceful-drain');
    assert(queued.queued_rows > 0, 'Go History did not persist the graceful-drain decision task');
    const drainMembershipPeer = directMatchingClient('127.0.0.1:' + rustGrpcPort);
    const drainOwner = await directPartitionOwner(drainMembershipPeer, domain, drainTaskList);
    const primaryAddress = '127.0.0.1:' + rustTChannel;
    const secondaryAddress = '127.0.0.1:' + process.env.CADENCE_RUST_MATCHING_SECONDARY_TCHANNEL;
    const drainingPrimary = drainOwner === primaryAddress;
    const drainingSecondary = drainOwner === secondaryAddress;
    assert(drainingPrimary || drainingSecondary, 'graceful-drain task list owner ' + drainOwner + ' is not either Rust Matching host');
    const drainingProcess = drainingPrimary ? rust : secondaryRust;
    const drainExit = drainingPrimary ? exited : new Promise(resolve => drainingProcess.once('exit', (code, signal) => resolve({ code, signal })));
    const drainingPid = drainingProcess.pid;
    const sdBeforeDrain = shardManagerControl ? (await sdAssertHeartbeat('before-sigterm')).executors : null;
    drainingProcess.kill('SIGTERM');
    const drained = await Promise.race([drainExit, new Promise(resolve => setTimeout(() => resolve(null), 60000))]);
    assert(drained, `Rust Matching did not finish SIGTERM drain; see ${work}/rust-server-before-graceful-restart.log`);
    assert.equal(drained.code, 0, `Rust Matching graceful shutdown failed: ${JSON.stringify(drained)}`);
    if (sdBeforeDrain) await sdAssertExecutorStopped('after-sigterm-drain', sdBeforeDrain);
    if (goServicesPid) process.kill(goServicesPid, 0);
    if (drainingPrimary) {
      rust = startRust('after-graceful-drain');
      await waitForRustReady('after-graceful-drain');
    } else {
      secondaryRust = startSecondaryRust();
      const secondaryMembershipPeer = directMatchingClient('127.0.0.1:' + process.env.CADENCE_RUST_MATCHING_SECONDARY_GRPC_PORT);
      const membershipDeadline = Date.now() + 60000;
      let membershipRestored = false;
      while (Date.now() < membershipDeadline) {
        try {
          const names = Array.from({ length: 16 }, (_, index) => 'graceful-rejoin-' + id + '-' + index);
          const owners = await Promise.all(names.map(async name => [
            await directPartitionOwner(drainMembershipPeer, domain, name),
            await directPartitionOwner(secondaryMembershipPeer, domain, name),
          ]));
          if (owners.every(([primary, secondary]) => primary === secondary)
              && owners.some(([primary]) => primary === secondaryAddress)) {
            membershipRestored = true;
            break;
          }
        } catch (_) {
          // The restarted member may not have rebound its listener or joined
          // the shared Ringpop view yet; retry until the bounded deadline.
        }
        await new Promise(resolve => setTimeout(resolve, 250));
      }
      assert(membershipRestored, 'gracefully restarted Rust Matching member did not rejoin the shared Ringpop ownership map');
    }
    if (sdBeforeDrain) {
      await sdAssertHeartbeat('after-sigterm-restart');
      await sdReassign('after-sigterm-restart', drainTaskList);
    }
    const drainSdk = spawnSync('go', ['run', '.'], { cwd: path.resolve(__dirname, 'go-sdk'), env: {
      ...process.env, CADENCE_MIXED_MATCHING_DOMAIN: domain, CADENCE_TCHANNEL_PEER: process.argv[6],
      CADENCE_MIXED_RESTART_WORKFLOW_ID: drainID, CADENCE_MIXED_RESTART_TASK_LIST: drainTaskList,
    }, encoding: 'utf8', timeout: 120000 });
    fs.writeFileSync(path.join(work, 'graceful-drain-sdk.log'), `${drainSdk.stdout || ''}\n${drainSdk.stderr || ''}`);
    if (drainSdk.status !== 0) throw new Error(`Existing SDK could not resume the task preserved across Rust SIGTERM: ${drainSdk.error || drainSdk.status}; see ${work}/graceful-drain-sdk.log`);
    const drainHistory = await call('WorkflowAPI', 'GetWorkflowExecutionHistory', { domain, workflowExecution: { workflowId: drainID, runId: drainStarted.runId } });
    const drainCompletions = drainHistory.history.events.filter(event => event.workflowExecutionCompletedEventAttributes).length;
    assert.equal(drainCompletions, 1, 'gracefully drained workflow must have exactly one visible completion');
    const drainedRows = await taskObserver.waitEmpty(domainId, drainTaskList, 30, 'task-after-graceful-drain');
    if (sdBeforeDrain) await sdRestartScenario('drain-flow', domain, drainID);
    gracefulDrainEvidence = { workflowId: drainID, runId: drainStarted.runId, taskList: drainTaskList, taskListOwner: drainOwner, queuedRowsBeforeSignal: queued.queued_rows, rangeBeforeSignal: queued.range_id, queuedRowsAfterRecovery: drainedRows.queued_rows, drainingPid, recoveredByRustPid: drainingPrimary ? rust.pid : secondaryRust.pid, completionEvents: drainCompletions, goServicesPid };
  } else {
  async function expireIdleOwners() {
    const done = path.join(work, 'idle-done');
    if (fs.existsSync(done)) fs.unlinkSync(done);
    fs.writeFileSync(path.join(work, 'idle-trigger'), 'advance-clock');
    const deadline = Date.now() + 10000;
    while (!fs.existsSync(done)) {
      if (Date.now() > deadline) throw new Error('Rust idle teardown did not finish');
      await new Promise(resolve => setTimeout(resolve, 10));
    }
  }
  const idleList = { name: `idle-${id}` };
  const pendingPoll = call('WorkerAPI', 'PollForDecisionTask', { domain, taskList: idleList, identity: 'idle-probe' });
  // Observe the wait through Go Frontend before expiring its owner.
  const pollDeadline = Date.now() + 10000;
  for (;;) {
    const info = await call('WorkflowAPI', 'DescribeTaskList', { domain, taskList: idleList, taskListType: 'TASK_LIST_TYPE_DECISION', includeTaskListStatus: true });
    if (info.pollers.some(poller => poller.identity === 'idle-probe')) break;
    if (Date.now() > pollDeadline) throw new Error('idle probe poll was not registered');
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  await expireIdleOwners();
  assert.equal((await pendingPoll).taskToken.length, 0, 'closed task-list pump must return an empty successful poll');
  const recovery = await call('WorkflowAPI', 'StartWorkflowExecution', { ...startRequest, workflowId: `idle-recovery-${id}`, taskList: idleList, requestId: randomUUID() });
  const queuedDeadline = Date.now() + 10000;
  for (;;) {
    const info = await call('WorkflowAPI', 'DescribeTaskList', { domain, taskList: idleList, taskListType: 'TASK_LIST_TYPE_DECISION', includeTaskListStatus: true });
    if (!info.taskListStatus.empty) break;
    if (Date.now() > queuedDeadline) throw new Error('Go History did not enqueue the idle recovery task');
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  await expireIdleOwners();
  const recovered = await call('WorkerAPI', 'PollForDecisionTask', { domain, taskList: idleList, identity: 'idle-recovery-probe' });
  assert.equal(recovered.workflowExecution.runId, recovery.runId);
  await call('WorkerAPI', 'RespondDecisionTaskCompleted', { taskToken: recovered.taskToken, decisions: [{ completeWorkflowExecutionDecisionAttributes: { result: payload('idle-recovered') } }] });
  const recoveredHistory = await call('WorkflowAPI', 'GetWorkflowExecutionHistory', { domain, workflowExecution: { workflowId: `idle-recovery-${id}`, runId: recovery.runId } });
  assert.equal(recoveredHistory.history.events.at(-1).workflowExecutionCompletedEventAttributes.result.data.toString(), 'idle-recovered');
  }
  // Rate-limited dispatch: the worker-reported maxTasksPerSecond is applied to
  // the shared limiter, so later backlog dispatches are paced. The first
  // dispatch may still use the pre-report burst, so only assert the last poll.
  const pacedList = { name: `paced-${id}` };
  // The rate lives in the owner's in-memory limiter. If the two Rust members
  // disagree on the owner (or the ring moves between the poll and the describe),
  // the describe can be answered by a second owner with a fresh default limiter.
  // Check ring agreement around the window and report that instead of the rate.
  const pacedOwnerPeers = process.env.CADENCE_RUST_MATCHING_SECONDARY_GRPC_PORT ? {
    primary: directMatchingClient(`127.0.0.1:${rustGrpcPort}`),
    secondary: directMatchingClient(`127.0.0.1:${process.env.CADENCE_RUST_MATCHING_SECONDARY_GRPC_PORT}`),
  } : null;
  const pacedOwnerLog = [];
  async function observePacedOwners(label) {
    if (!pacedOwnerPeers) return null;
    const [primaryOwner, secondaryOwner] = await Promise.all([
      directPartitionOwner(pacedOwnerPeers.primary, domain, pacedList.name),
      directPartitionOwner(pacedOwnerPeers.secondary, domain, pacedList.name),
    ]);
    const observed = { label, primaryOwner, secondaryOwner, agree: primaryOwner === secondaryOwner };
    pacedOwnerLog.push(observed);
    console.log(JSON.stringify({ pacedListOwner: observed }));
    fs.writeFileSync(path.join(work, 'paced-owner.json'), JSON.stringify(pacedOwnerLog, null, 2) + '\n');
    return observed;
  }
  const pacedOwnershipBefore = await (async () => {
    const deadline = Date.now() + 30000;
    let observed = await observePacedOwners('before-paced-poll');
    while (observed && !observed.agree && Date.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 1000));
      observed = await observePacedOwners('before-paced-poll');
    }
    if (observed) assert(observed.agree, `paced list owners did not agree before the poll: ${JSON.stringify(observed)}`);
    return observed;
  })();
  const pacedStart = await call('WorkflowAPI', 'StartWorkflowExecution', {
    ...startRequest, workflowId: `paced-${id}`, taskList: pacedList, requestId: randomUUID(),
  });
  const pacedDecision = await call('WorkerAPI', 'PollForDecisionTask', { domain, taskList: pacedList, identity: 'paced-probe' });
  assert.equal(pacedDecision.workflowExecution.runId, pacedStart.runId);
  const pacedSchedules = [0, 1, 2].map(index => ({ scheduleActivityTaskDecisionAttributes: {
    activityId: `paced-${index}`, activityType: { name: 'Echo' }, taskList: pacedList, input: payload(`paced-${index}`),
    scheduleToCloseTimeout: { seconds: '60' }, scheduleToStartTimeout: { seconds: '60' }, startToCloseTimeout: { seconds: '30' },
  } }));
  await call('WorkerAPI', 'RespondDecisionTaskCompleted', { taskToken: pacedDecision.taskToken, identity: 'paced-probe', decisions: pacedSchedules });
  const pacedLatency = [], pacedInputs = [];
  for (let index = 0; index < pacedSchedules.length; index++) {
    const begin = Date.now();
    const task = await call('WorkerAPI', 'PollForActivityTask', {
      domain, taskList: pacedList, identity: 'paced-probe', taskListMetadata: { maxTasksPerSecond: { value: 1 } },
    });
    pacedLatency.push(Date.now() - begin);
    pacedInputs.push(task.input.data.toString());
    await call('WorkerAPI', 'RespondActivityTaskCompleted', { taskToken: task.taskToken, result: payload('paced-ok'), identity: 'paced-probe' });
  }
  // Dispatch order is not identity-FIFO; assert the delivered set and that the
  // cumulative pacing delayed the later backlog deliveries (the wait may land on any poll after the first).
  assert.deepEqual(pacedInputs.slice().sort(), ['paced-0', 'paced-1', 'paced-2']);
  if (pacedLatency.slice(1).reduce((a, b) => a + b, 0) < 500) {
    // Trace-only diagnostics for a failed pacing assertion: the assertion itself is unchanged.
    const traced = await call('WorkflowAPI', 'DescribeTaskList', { domain, taskList: pacedList, taskListType: 'TASK_LIST_TYPE_ACTIVITY', includeTaskListStatus: true }).catch(error => ({ describeError: String(error) }));
    const owners = await observePacedOwners('after-paced-failure');
    fs.writeFileSync(path.join(work, 'paced-failure-trace.json'), JSON.stringify({ pacedLatency, pacedInputs, traced, owners }, null, 2) + '\n');
  }
  assert(pacedLatency.slice(1).reduce((a, b) => a + b, 0) >= 500, `paced activity dispatch was not rate limited: ${JSON.stringify(pacedLatency)}`);
  const pacedOwnerChanged = (before, after) => !after.agree || after.primaryOwner !== before.primaryOwner || after.secondaryOwner !== before.secondaryOwner;
  const pacedOwnershipAtDescribe = await observePacedOwners('before-paced-describe');
  if (pacedOwnershipBefore && pacedOwnershipAtDescribe && pacedOwnerChanged(pacedOwnershipBefore, pacedOwnershipAtDescribe)) {
    throw new Error(`paced list owner changed between poll and describe: ${JSON.stringify({ beforePoll: pacedOwnershipBefore, beforeDescribe: pacedOwnershipAtDescribe })}`);
  }
  const pacedDescribe = await call('WorkflowAPI', 'DescribeTaskList', { domain, taskList: pacedList, taskListType: 'TASK_LIST_TYPE_ACTIVITY', includeTaskListStatus: true });
  if (pacedDescribe.taskListStatus.ratePerSecond !== 1 && pacedOwnershipBefore) {
    // A wrong rate with a changed owner is an ownership split, not a limiter bug.
    const pacedOwnershipAfter = await observePacedOwners('after-paced-describe');
    if (pacedOwnerChanged(pacedOwnershipBefore, pacedOwnershipAfter)) {
      throw new Error(`paced list owner changed between poll and describe: ${JSON.stringify({ beforePoll: pacedOwnershipBefore, afterDescribe: pacedOwnershipAfter, ratePerSecond: pacedDescribe.taskListStatus.ratePerSecond })}`);
    }
  }
  assert.equal(pacedDescribe.taskListStatus.ratePerSecond, 1, 'DescribeTaskList must report the reported backing rate');
  if (process.env.CADENCE_MATCHING_CONFIGSTORE_POLL_MS) {
    // configstore lane: matching.longPollExpirationInterval was seeded into Go's cluster_config row
    // (2s); the default is 60s, so an empty direct poll with a 10s deadline only returns early
    // (an empty response, not DEADLINE_EXCEEDED) if the Rust configstore client applied the row.
    const expected = Number(process.env.CADENCE_MATCHING_CONFIGSTORE_POLL_MS);
    const taskListName = `configstore-empty-${id}`;
    const membershipClient = directMatchingClient(`127.0.0.1:${rustGrpcPort}`);
    const owner = await directPartitionOwner(membershipClient, domain, taskListName);
    membershipClient.close();
    const primaryOwner = owner === `127.0.0.1:${rustTChannel}`;
    assert(primaryOwner || owner === `127.0.0.1:${process.env.CADENCE_RUST_MATCHING_SECONDARY_TCHANNEL}`, `configstore task list has unknown owner: ${owner}`);
    const pollClient = directMatchingClient(`127.0.0.1:${primaryOwner ? rustGrpcPort : process.env.CADENCE_RUST_MATCHING_SECONDARY_GRPC_PORT}`);
    const begin = Date.now();
    let emptyPoll;
    try {
      emptyPoll = await directDecisionPoll(pollClient, domainId, taskListName, 'configstore');
    } finally {
      pollClient.close();
    }
    const elapsed = Date.now() - begin;
    assert(elapsed >= expected - 500 && elapsed <= expected + 4000, `configstore long-poll expiry not applied: empty poll took ${elapsed}ms, expected about ${expected}ms`);
    assert(!emptyPoll.taskToken || emptyPoll.taskToken.length === 0, 'configstore empty poll returned a task');
    configstoreEvidence = { seededLongPollExpiryMs: expected, emptyPollElapsedMs: elapsed, owner };
  }
  // Query dispatched directly through matching: the workflow is open with no
  // pending decision, so Go History routes the query to Rust Matching, which
  // offers it to a waiting decision poller on the same long poll.
  const queryList = { name: `query-${id}` };
  const queryStart = await call('WorkflowAPI', 'StartWorkflowExecution', {
    ...startRequest, workflowId: `query-${id}`, taskList: queryList, requestId: randomUUID(),
  });
  const queryDecision = await call('WorkerAPI', 'PollForDecisionTask', { domain, taskList: queryList, identity: 'query-probe' });
  assert.equal(queryDecision.workflowExecution.runId, queryStart.runId);
  await call('WorkerAPI', 'RespondDecisionTaskCompleted', { taskToken: queryDecision.taskToken, identity: 'query-probe', decisions: [] });
  const pendingQuery = call('WorkerAPI', 'PollForDecisionTask', { domain, taskList: queryList, identity: 'query-probe' });
  const queryPollDeadline = Date.now() + 10000;
  for (;;) {
    const info = await call('WorkflowAPI', 'DescribeTaskList', { domain, taskList: queryList, taskListType: 'TASK_LIST_TYPE_DECISION', includeTaskListStatus: true });
    if (info.pollers.some(poller => poller.identity === 'query-probe')) break;
    if (Date.now() > queryPollDeadline) throw new Error('query probe poll was not registered');
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  const pendingAnswer = call('WorkflowAPI', 'QueryWorkflow', {
    domain, workflowExecution: { workflowId: `query-${id}`, runId: queryStart.runId },
    query: { queryType: 'probe' }, queryConsistencyLevel: 'QUERY_CONSISTENCY_LEVEL_EVENTUAL',
  });
  const queryTask = await pendingQuery;
  assert.equal(queryTask.query?.queryType, 'probe', 'Rust Matching must deliver the query to a decision poller');
  assert.equal(queryTask.workflowExecution.runId, queryStart.runId);
  assert(queryTask.history.events.length > 0, 'a non-sticky query poll must carry history');
  assert(!queryTask.queries || Object.keys(queryTask.queries).length === 0, 'direct queries must not use the buffered query map');
  await call('WorkerAPI', 'RespondQueryTaskCompleted', { taskToken: queryTask.taskToken, result: { resultType: 'QUERY_RESULT_TYPE_ANSWERED', answer: payload('pong') } });
  assert.equal((await pendingAnswer).queryResult.data.toString(), 'pong');
  // Real Go History start through an isolated Rust queue. Create a scheduled
  // activity normally, then reference that schedule from a second task list to
  // control its isolation metadata without changing Go History configuration.
  clients.Matching = new schema.matching.v1.MatchingAPI(process.argv[4], matchingCredentials);
  const isolatedList = { name: `isolated-${id}` };
  if (failureRecoveryMode) {
    // Two Rust instances share the ring: like Go's Matching, the non-owner answers
    // the typed ABORTED status, so aim the direct client at whichever host owns the list.
    try {
      await call('Matching', 'DescribeTaskList', { domainId, request: { taskList: isolatedList, taskListType: 'TASK_LIST_TYPE_ACTIVITY' } });
    } catch (error) {
      if (error.code !== grpc.status.ABORTED) throw error;
      clients.Matching = directMatchingClient(`127.0.0.1:${process.env.CADENCE_RUST_MATCHING_SECONDARY_GRPC_PORT}`);
    }
  }
  const isoStart = await call('WorkflowAPI', 'StartWorkflowExecution', {
    ...startRequest, workflowId: `isolated-${id}`, taskList: isolatedList, requestId: randomUUID(),
  });
  const isoDecision = await call('WorkerAPI', 'PollForDecisionTask', { domain, taskList: isolatedList, identity: 'isolation-scheduler' });
  await call('WorkerAPI', 'RespondDecisionTaskCompleted', { taskToken: isoDecision.taskToken, decisions: [{ scheduleActivityTaskDecisionAttributes: {
    activityId: 'isolated', activityType: { name: 'Echo' }, taskList: { name: `unpolled-${id}` },
    scheduleToCloseTimeout: { seconds: '60' }, scheduleToStartTimeout: { seconds: '60' }, startToCloseTimeout: { seconds: '30' },
  } }] });
  let scheduled;
  const scheduleDeadline = Date.now() + 10000;
  while (!scheduled) {
    const history = await call('WorkflowAPI', 'GetWorkflowExecutionHistory', { domain, workflowExecution: { workflowId: `isolated-${id}`, runId: isoStart.runId } });
    scheduled = history.history.events.find(event => event.activityTaskScheduledEventAttributes);
    if (!scheduled && Date.now() > scheduleDeadline) throw new Error('isolated activity not scheduled');
  }
  const pollA = call('Matching', 'PollForActivityTask', { domainId, pollerId: randomUUID(), isolationGroup: 'zone-a', request: { taskList: isolatedList, identity: 'isolated-worker-a' } });
  const pollerB = randomUUID();
  const pollB = call('Matching', 'PollForActivityTask', { domainId, pollerId: pollerB, isolationGroup: 'zone-b', request: { taskList: isolatedList, identity: 'isolated-worker-b' } }).then(response => ({ response }), error => ({ error }));
  const registeredDeadline = Date.now() + 10000;
  for (;;) {
    const info = await call('Matching', 'DescribeTaskList', { domainId, request: { taskList: isolatedList, taskListType: 'TASK_LIST_TYPE_ACTIVITY', includeTaskListStatus: true } });
    if (info.taskListStatus.isolationGroupMetrics['zone-a']?.pollerCount === '1' && info.taskListStatus.isolationGroupMetrics['zone-b']?.pollerCount === '1') break;
    if (Date.now() > registeredDeadline) throw new Error('isolated poller was not registered');
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  await call('Matching', 'AddActivityTask', { domainId, sourceDomainId: domainId, workflowExecution: { workflowId: `isolated-${id}`, runId: isoStart.runId }, taskList: isolatedList, scheduleId: scheduled.eventId, partitionConfig: { 'isolation-group': 'zone-a' } });
  const isolatedTask = await pollA;
  assert.equal(isolatedTask.activityId, 'isolated');
  await call('Matching', 'CancelOutstandingPoll', { domainId, taskList: isolatedList, taskListType: 'TASK_LIST_TYPE_ACTIVITY', pollerId: pollerB });
  // Go's CancelOutstandingPoll ends the poll with an empty response (ErrNoTasks), not a gRPC error.
  const cancelledB = await pollB;
  assert(!cancelledB.error, `cancelled poll must not fail: ${cancelledB.error?.message}`);
  assert(!cancelledB.response?.activityId && !cancelledB.response?.taskToken?.length, 'other isolation group must not consume the isolated activity');
  await call('WorkerAPI', 'RespondActivityTaskCompleted', { taskToken: isolatedTask.taskToken, result: payload('isolated-ok') });
  // Empty partition entries must survive Rust -> Go Frontend -> protobufjs.
  // The Go gogo decoder treats an omitted default map value as a nil pointer.
  await call('Matching', 'UpdateTaskListPartitionConfig', {
    domainId, taskList: isolatedList, taskListType: 'TASK_LIST_TYPE_ACTIVITY',
    partitionConfig: { version: '999', numReadPartitions: 2, numWritePartitions: 1,
      readPartitions: { 0: {}, 5: { isolationGroups: ['zone-a'] } }, writePartitions: { 0: {} } },
  });
  // Thrift's DescribeTaskListResponse has no partitionConfig (Go's thrift mapper drops it), so with Go
  // Frontend -> Matching over Thrift the config is read from Matching's own gRPC API instead.
  const partitionInfo = process.env.CADENCE_MATCHING_TRANSPORT === 'thrift'
    ? await call('Matching', 'DescribeTaskList', { domainId, request: { domain, taskList: isolatedList, taskListType: 'TASK_LIST_TYPE_ACTIVITY', includeTaskListStatus: true } })
    : await call('WorkflowAPI', 'DescribeTaskList', { domain, taskList: isolatedList, taskListType: 'TASK_LIST_TYPE_ACTIVITY', includeTaskListStatus: true });
  assert.equal(partitionInfo.partitionConfig.version, '1');
  assert.deepEqual(partitionInfo.partitionConfig.readPartitions['0'].isolationGroups, []);
  assert.deepEqual(partitionInfo.partitionConfig.readPartitions['5'].isolationGroups, ['zone-a']);
  assert.deepEqual(partitionInfo.partitionConfig.writePartitions['0'].isolationGroups, []);
  // Backlog forward-to-parent (Go matcher.go Offer forwarding at 216-236 and 373-427, forwarder.go ForwardTask
  // at 117 and ForwardPoll at 211). A task persisted in a child partition's backlog reaches the root poller
  // when nothing polls the child, and a poll on the child with no local task is forwarded to the root backlog.
  // Only the root is polled in the first delivery, and only the child in the second.
  let backlogForwardEvidence;
  if (productionMode && process.env.CADENCE_MATCHING_BACKLOG_FORWARD === '1') {
    const forwardPrimary = directMatchingClient(process.argv[4]);
    const forwardSecondary = failureRecoveryMode ? directMatchingClient(`127.0.0.1:${process.env.CADENCE_RUST_MATCHING_SECONDARY_GRPC_PORT}`) : null;
    const forwardRpc = (client, method, request) => {
      const metadata = new grpc.Metadata();
      metadata.set('rpc-service', 'cadence-matching');
      metadata.set('rpc-encoding', 'proto');
      metadata.set('rpc-caller', 'cadence-backlog-forward-probe');
      return new Promise((resolve, reject) => client[method](request, metadata, { deadline: Date.now() + 20000 },
        (error, response) => error ? reject(error) : resolve(response)));
    };
    // With two Rust nodes each partition has one owner, and the non-owner answers a typed ABORTED.
    const matchingOwnerCall = async (method, request) => {
      try { return await forwardRpc(forwardPrimary, method, request); } catch (error) {
        if (error.code !== grpc.status.ABORTED || !forwardSecondary) throw error;
        return forwardRpc(forwardSecondary, method, request);
      }
    };
    const scheduleBacklogActivity = async (label, activityId) => {
      const workflowId = `fwd-${label}-${id}`;
      const decisionList = { name: `fwd-decision-${label}-${id}` };
      const started = await call('WorkflowAPI', 'StartWorkflowExecution', {
        ...startRequest, workflowId, taskList: decisionList, requestId: randomUUID(),
      });
      const decision = await call('WorkerAPI', 'PollForDecisionTask', { domain, taskList: decisionList, identity: `fwd-${label}-scheduler` });
      // The scheduled activity names a list nobody polls. Matching is then fed the same schedule id directly,
      // as the isolation scenario does, so the add is what places the task on the partition under test.
      await call('WorkerAPI', 'RespondDecisionTaskCompleted', { taskToken: decision.taskToken, decisions: [{ scheduleActivityTaskDecisionAttributes: {
        activityId, activityType: { name: 'Echo' }, taskList: { name: `fwd-unpolled-${label}-${id}` },
        scheduleToCloseTimeout: { seconds: '60' }, scheduleToStartTimeout: { seconds: '60' }, startToCloseTimeout: { seconds: '30' },
      } }] });
      const scheduleDeadline = Date.now() + 10000;
      for (;;) {
        const history = await call('WorkflowAPI', 'GetWorkflowExecutionHistory', { domain, workflowExecution: { workflowId, runId: started.runId } });
        const scheduled = history.history.events.find(event => event.activityTaskScheduledEventAttributes);
        if (scheduled) return { workflowId, runId: started.runId, scheduleId: scheduled.eventId };
        if (Date.now() > scheduleDeadline) throw new Error(`backlog forward ${label} activity not scheduled`);
        await new Promise(resolve => setTimeout(resolve, 10));
      }
    };
    const waitForBacklogRows = async (name, label) => {
      const deadline = Date.now() + 15000;
      for (;;) {
        const rows = durableTaskCount(domainId, name, 1, undefined);
        assert.equal(rows.status, 0, `could not count ${label} backlog rows: ${rows.stderr}`);
        const count = Number(rows.stdout.trim());
        if (count >= 1) return count;
        if (Date.now() > deadline) throw new Error(`${label} backlog task was not persisted under ${name}`);
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    };
    const backlogRoot = { name: `backlog-forward-${id}` };
    const backlogChild = { name: `/__cadence_sys/backlog-forward-${id}/1` };
    const childActivity = await scheduleBacklogActivity('child', 'forwarded-from-child');
    await matchingOwnerCall('UpdateTaskListPartitionConfig', {
      domainId, taskList: backlogRoot, taskListType: 'TASK_LIST_TYPE_ACTIVITY',
      partitionConfig: { version: '999', numReadPartitions: 2, numWritePartitions: 2,
        readPartitions: { 0: {}, 1: {} }, writePartitions: { 0: {}, 1: {} } },
    });
    // The child gets the task with no poller on either partition, so it has to persist in the child backlog.
    await matchingOwnerCall('AddActivityTask', { domainId, sourceDomainId: domainId, workflowExecution: { workflowId: childActivity.workflowId, runId: childActivity.runId }, taskList: backlogChild, scheduleId: childActivity.scheduleId });
    const childBacklogRows = await waitForBacklogRows(backlogChild.name, 'child partition');
    const rootPoll = matchingOwnerCall('PollForActivityTask', { domainId, pollerId: randomUUID(), request: { taskList: backlogRoot, identity: 'backlog-forward-root-poller' } })
      .catch(error => { throw new Error(`root poll did not receive the forwarded child backlog task: ${error.message}`); });
    const fromChild = await rootPoll;
    assert.equal(fromChild.activityId, 'forwarded-from-child', 'root poller must receive the child backlog task');
    await call('WorkerAPI', 'RespondActivityTaskCompleted', { taskToken: fromChild.taskToken, result: payload('forwarded-from-child-ok') });
    // Reverse path: the root backlog is drained by a poll that only the child receives (Go ForwardPoll).
    const parentActivity = await scheduleBacklogActivity('parent', 'forwarded-from-parent');
    await matchingOwnerCall('AddActivityTask', { domainId, sourceDomainId: domainId, workflowExecution: { workflowId: parentActivity.workflowId, runId: parentActivity.runId }, taskList: backlogRoot, scheduleId: parentActivity.scheduleId });
    const rootBacklogRows = await waitForBacklogRows(backlogRoot.name, 'root partition');
    const fromParent = await matchingOwnerCall('PollForActivityTask', { domainId, pollerId: randomUUID(), request: { taskList: backlogChild, identity: 'backlog-forward-child-poller' } });
    assert.equal(fromParent.activityId, 'forwarded-from-parent', 'child poller must receive the root backlog task');
    await call('WorkerAPI', 'RespondActivityTaskCompleted', { taskToken: fromParent.taskToken, result: payload('forwarded-from-parent-ok') });
    backlogForwardEvidence = {
      rootPartition: backlogRoot.name, childPartition: backlogChild.name, numReadPartitions: 2, numWritePartitions: 2,
      childToRoot: { workflowId: childActivity.workflowId, activityId: fromChild.activityId, childBacklogRowsBeforeRootPoll: childBacklogRows, rootPollOnly: true },
      parentToChild: { workflowId: parentActivity.workflowId, activityId: fromParent.activityId, rootBacklogRowsBeforeChildPoll: rootBacklogRows, childPollOnly: true },
    };
    fs.writeFileSync(path.join(work, 'backlog-forward.json'), JSON.stringify(backlogForwardEvidence, null, 2) + '\n');
  }
  // Opt-in scenarios (CADENCE_PROBE_*), see tests/matching-probe-scenarios.cjs.
  let probeScenarios = 'not requested';
  if (productionMode && Object.keys(process.env).some(name => name.startsWith('CADENCE_PROBE_') && process.env[name] === '1')) {
    const directClients = [{ client: directMatchingClient(`127.0.0.1:${rustGrpcPort}`), tchannel: `127.0.0.1:${rustTChannel}` }];
    if (process.env.CADENCE_RUST_MATCHING_SECONDARY_GRPC_PORT) {
      directClients.push({ client: directMatchingClient(`127.0.0.1:${process.env.CADENCE_RUST_MATCHING_SECONDARY_GRPC_PORT}`), tchannel: `127.0.0.1:${process.env.CADENCE_RUST_MATCHING_SECONDARY_TCHANNEL}` });
    }
    probeScenarios = await runProbeScenarios({
      grpc, call, directClients, domain, domainId, id, startRequest, work, durableTaskCount,
      restartRust: async () => {
        const killedPid = rust.pid;
        rust.kill('SIGKILL');
        const killed = await exited;
        assert.equal(killed.signal, 'SIGKILL', 'Rust Matching did not terminate for the durable restart scenario');
        rust = startRust('after-durable-restart');
        await waitForRustReady('after-durable-restart');
        return { killedPid, restartedPid: rust.pid };
      },
    });
  }
  const sdk = spawnSync('go', ['run', '.'], { cwd: path.resolve(__dirname, 'go-sdk'), env: { ...process.env, CADENCE_MIXED_MATCHING_DOMAIN: domain, CADENCE_TCHANNEL_PEER: process.argv[6] }, encoding: 'utf8', timeout: 180000 });
  fs.writeFileSync(path.join(work, 'go-sdk.log'), `${sdk.stdout || ''}\n${sdk.stderr || ''}`);
  if (sdk.status !== 0) throw new Error(`Existing SDK against mixed Matching failed: ${sdk.error || sdk.status}; see ${work}/go-sdk.log`);
  let productionDatabaseEvidence;
  if (productionMode && process.env.CADENCE_MATCHING_MYSQL) {
    // The GC assertions below decode Go's task-list metadata, so the decoder
    // is checked against the committed blob fixtures first.
    const decoderFixtureCases = verifyTaskListBlobDecoder();
    // A dedicated backlog on a task list without any poller. Its persisted
    // watermark is read while the decision task is stored and undelivered,
    // and the existing SDK drains it afterwards.
    const backlogList = { name: `gc-backlog-${id}` };
    const backlogWorkflowId = `gc-backlog-${id}`;
    const backlogStarted = await call('WorkflowAPI', 'StartWorkflowExecution', {
      ...startRequest, workflowId: backlogWorkflowId, workflowType: { name: 'main.EchoWorkflow' },
      taskList: backlogList, requestId: randomUUID(), input: payload(JSON.stringify('restart-proof')),
    });
    const backlogWatermark = await awaitPersistedTask(backlogList.name, 0);
    assert(backlogWatermark && backlogWatermark.maxTaskId !== null,
      'Go History did not persist the dedicated garbage collection backlog task');
    const backlogActivityWatermark = await scheduleEchoActivityAndObserve(backlogList.name, backlogStarted.runId, 'backlog');
    const backlogSdk = spawnSync('go', ['run', '.'], { cwd: path.resolve(__dirname, 'go-sdk'), env: {
      ...process.env, CADENCE_MIXED_MATCHING_DOMAIN: domain, CADENCE_TCHANNEL_PEER: process.argv[6],
      CADENCE_MIXED_RESTART_WORKFLOW_ID: backlogWorkflowId, CADENCE_MIXED_RESTART_TASK_LIST: backlogList.name,
    }, encoding: 'utf8', timeout: 120000 });
    fs.writeFileSync(path.join(work, 'gc-backlog-sdk.log'), `${backlogSdk.stdout || ''}\n${backlogSdk.stderr || ''}`);
    if (backlogSdk.status !== 0) throw new Error(`Existing SDK did not drain the garbage collection backlog: ${backlogSdk.error || backlogSdk.status}; see ${work}/gc-backlog-sdk.log`);
    const backlogHistory = await call('WorkflowAPI', 'GetWorkflowExecutionHistory', { domain, workflowExecution: { workflowId: backlogWorkflowId, runId: backlogStarted.runId } });
    const backlogCompletions = backlogHistory.history.events.filter(event => event.workflowExecutionCompletedEventAttributes).length;
    assert.equal(backlogCompletions, 1, 'the drained garbage collection backlog must have exactly one visible completion');
    // A task that is never acknowledged must survive the collector.
    const pendingList = { name: `gc-pending-${id}` };
    const pendingStarted = await call('WorkflowAPI', 'StartWorkflowExecution', {
      ...startRequest, workflowId: `gc-pending-${id}`, taskList: pendingList, requestId: randomUUID(),
    });
    const pendingWatermark = await awaitPersistedTask(pendingList.name, 0);
    assert(pendingWatermark && pendingWatermark.maxTaskId !== null, 'Go History did not persist the unacknowledged control task');
    const liveBlockProvider = async (listName, taskType) => {
      const taskListType = taskType === 0 ? 'TASK_LIST_TYPE_DECISION' : 'TASK_LIST_TYPE_ACTIVITY';
      const status = (await call('WorkflowAPI', 'DescribeTaskList', { domain, taskList: { name: listName }, taskListType, includeTaskListStatus: true })).taskListStatus;
      return { startId: Number(status.taskIdBlock.startId), endId: Number(status.taskIdBlock.endId) };
    };
    const mainDecisionMax = Math.max(mainDecisionWatermark?.maxTaskId ?? 0, finalWatermark?.maxTaskId ?? 0);
    const gateExpectations = [
      { label: 'backlogDecision', name: backlogList.name, taskType: 0, observedMaxTaskId: backlogWatermark.maxTaskId },
      { label: 'backlogActivity', name: backlogList.name, taskType: 1, observedMaxTaskId: backlogActivityWatermark.maxTaskId },
      { label: 'mainDecision', name: taskList.name, taskType: 0, observedMaxTaskId: mainDecisionMax },
      { label: 'mainActivity', name: taskList.name, taskType: 1, observedMaxTaskId: mainActivityWatermark?.maxTaskId ?? 1 },
    ];
    if (restartMode && restartState.watermark && restartState.watermark.maxTaskId !== null) {
      gateExpectations.push({ label: 'restartDecision', name: restartState.taskList, taskType: 0, observedMaxTaskId: restartState.watermark.maxTaskId });
      gateExpectations.push({ label: 'restartActivity', name: restartState.taskList, taskType: 1, observedMaxTaskId: restartState.activityWatermark.maxTaskId });
    }
    const taskGarbageCollection = await verifyProductionMySqlTaskGc({
      query: mysqlRows,
      domainId,
      taskLists: gateExpectations,
      pendingListName: pendingList.name,
      liveBlockProvider,
      rangeSize: 100000,
      sleep: milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds)),
    });
    const domainMetadata = observeProductionMySqlDomain({ query: mysqlRows, domain, domainId });
    productionDatabaseEvidence = {
      decoderFixtureCases,
      taskGarbageCollection,
      domainMetadata,
      backlog: { taskList: backlogList.name, workflowId: backlogWorkflowId, watermark: backlogWatermark, completionEvents: backlogCompletions },
      pendingControl: { taskList: pendingList.name, workflowId: pendingStarted.workflowId || `gc-pending-${id}`, watermark: pendingWatermark },
    };
    fs.writeFileSync(path.join(work, 'db-observations.json'), JSON.stringify(productionDatabaseEvidence, null, 2) + '\n');
  }
  if (productionMode && process.env.CADENCE_MATCHING_POSTGRES) {
    const goMatchingWorkflowId = `go-matching-consume-${id}`;
    const goMatchingTaskList = `go-matching-consume-tasks-${id}`;
    const pending = await call('WorkflowAPI', 'StartWorkflowExecution', {
      ...startRequest, workflowId: goMatchingWorkflowId, workflowType: { name: 'main.EchoWorkflow' },
      taskList: { name: goMatchingTaskList }, requestId: randomUUID(),
      input: payload(JSON.stringify('restart-proof')),
    });
    const pendingDeadline = Date.now() + 20000;
    let pendingRows;
    while (Date.now() < pendingDeadline) {
      pendingRows = durableTaskCount(domainId, goMatchingTaskList, 0, undefined);
      if (pendingRows.status === 0 && Number(pendingRows.stdout) > 0) break;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert.equal(pendingRows?.status, 0, `could not inspect Rust-produced PostgreSQL task: ${pendingRows?.stderr}`);
    assert(Number(pendingRows.stdout) > 0, 'Go History did not persist a pending PostgreSQL task through Rust Matching');
    fs.writeFileSync(path.join(work, 'rust-postgres-evidence.json'), JSON.stringify({
      domain, domainId, workflowId: execution.workflowId, runId: execution.runId,
      taskList: taskList.name, completedEvents: events.length,
      goMatchingPendingTask: { workflowId: goMatchingWorkflowId, runId: pending.runId,
        taskList: goMatchingTaskList, taskRowsBeforeGoConsume: Number(pendingRows.stdout) },
    }, null, 2) + '\n');
  }
  console.log(JSON.stringify({ result: productionMode ? (cassandraGate ? 'passed' : 'preliminary-passed') : 'passed', acceptanceEligible: cassandraGate, topology: productionMode ? (cassandraGate ? 'Go Frontend/History/Worker and Rust Matching on shared Go Cassandra schema' : 'Go Frontend/History/Worker with Rust Matching via original Go Ringpop discovery') : 'Go Frontend/History/Worker with Rust Matching RPC', idleUnloadAndRecovery: productionMode ? 'production process idle eviction and recovery' : 'passed', gracefulDrainRecovery: gracefulDrainEvidence || 'not requested', queryDispatch: 'passed', isolationDispatch: 'passed', nativePartitionConfig: 'passed', backlogForward: backlogForwardEvidence || 'not requested', configstore: configstoreEvidence || 'not requested', probeScenarios, processRestart: restartEvidence || 'not requested', parityGate: cassandraGate ? 'Cassandra mixed SDK, domain pagination, task rows, semaphore concurrency, SIGKILL and SIGTERM recovery gates passed; service parity audit still required' : (productionMode ? 'production mixed SDK/restart evidence only; Cassandra acceptance requires --cassandra' : 'adapter compatibility evidence only'), productionDatabaseEvidence: productionDatabaseEvidence || 'not collected', shardDistributor: shardManagerControl ? shardDistributorEvidence : 'not requested', peer, domain, execution, attributes }));
  } catch (error) {
    // Keep the probe-body failure visible; the shutdown checks in finally may throw their own error.
    probeFailure = error;
    console.error('PROBE BODY FAILED:', error);
  } finally {
    if (productionMode) {
      const sigtermSentUnixNs = Date.now() * 1_000_000;
      fs.writeFileSync(path.join(work, 'matching-shutdown.json'), JSON.stringify({
        signal: 'SIGTERM', sigtermSentUnixNs,
      }) + '\n');
      rust.kill('SIGTERM');
    }
    else fs.writeFileSync(stop, 'stop');
    let shutdownTimer;
    let result = await Promise.race([exited, new Promise(resolve => { shutdownTimer = setTimeout(() => resolve(null), 20000); })]);
    clearTimeout(shutdownTimer);
    if (result === null) {
      rust.kill('SIGKILL');
      result = await exited;
      if (probeFailure) console.error('RUST SHUTDOWN TIMEOUT after probe failure; sent SIGKILL.');
      else throw new Error('Rust Matching did not stop');
    } else if (result.code !== 0) {
      const shutdownError = new Error(`Rust Matching exited ${JSON.stringify(result)}; see ${work}/rust-server-after-restart.log`);
      if (probeFailure) console.error('RUST SHUTDOWN FAILURE after probe failure:', shutdownError);
      else throw shutdownError;
    }
    if (productionMode) {
      const shutdownPath = path.join(work, 'matching-shutdown.json');
      const shutdown = JSON.parse(fs.readFileSync(shutdownPath, 'utf8'));
      shutdown.exitCode = result.code;
      shutdown.childSignal = result.signal;
      shutdown.completedAtUnixNs = Date.now() * 1_000_000;
      fs.writeFileSync(shutdownPath, JSON.stringify(shutdown, null, 2) + '\n');
    }
    if (secondaryRust && secondaryRust.exitCode === null && secondaryRust.signalCode === null) {
      secondaryRust.kill('SIGTERM');
      const secondaryExit = await Promise.race([
        new Promise(resolve => secondaryRust.once('exit', (code, signal) => resolve({ code, signal }))),
        new Promise(resolve => setTimeout(() => resolve(null), 20000)),
      ]);
      if (secondaryExit === null) { secondaryRust.kill('SIGKILL'); throw new Error('secondary Rust Matching did not stop'); }
      if (secondaryExit.code !== 0) throw new Error(`secondary Rust Matching exited ${JSON.stringify(secondaryExit)}; see ${work}/rust-server-secondary.log`);
    }
  }
  if (probeFailure) throw probeFailure;
}
run().catch(error => { console.error(error); process.exitCode = 1; }).finally(() => Object.values(clients).forEach(client => client.close()));
