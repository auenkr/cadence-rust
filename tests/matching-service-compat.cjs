// Go Frontend/History/Worker routed to the Rust Matching protobuf listener.
const assert = require('node:assert/strict');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const fs = require('node:fs');
const { spawn, spawnSync } = require('node:child_process');
const { createRequire } = require('node:module');
const { createTaskObserver } = require('./matching-task-observer.cjs');
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
const shardManagerControl = process.env.CADENCE_MATCHING_SHARD_DISTRIBUTOR_CONTROL;
const shardManagerNamespace = process.env.CADENCE_MATCHING_SHARD_DISTRIBUTOR_NAMESPACE || 'matching';
const clients = Object.fromEntries(['DomainAPI', 'WorkflowAPI', 'WorkerAPI', 'SemaphoreAPI'].map(name => [name, new api[name](peer, grpc.credentials.createInsecure())]));
clients.Matching = new schema.matching.v1.MatchingAPI(peer, grpc.credentials.createInsecure());
const cassandraGate = process.env.CADENCE_MATCHING_CASSANDRA_GATE === '1';
function cassandraCommand(args, label) {
  const result = spawnSync('go', ['run', './tests/matching-cassandra-interop', ...args], {
    cwd: path.resolve(__dirname, '..'), encoding: 'utf8', timeout: 120000, env: process.env,
  });
  fs.writeFileSync(path.join(path.resolve(process.argv[5]), `${label}.log`), `${result.stdout || ''}\n${result.stderr || ''}`);
  if (result.status !== 0) throw new Error(`Cassandra ${label} failed (${result.error || result.status}); see ${path.resolve(process.argv[5], `${label}.log`)}`);
  return JSON.parse(result.stdout.trim().split('\n').at(-1));
}
function sqliteTaskState(domainId, taskList, label) {
  const database = productionMode ? process.env.CADENCE_MATCHING_SQLITE_DB : path.join(path.resolve(process.argv[5]), 'matching.db');
  if (!database) throw new Error('SQLite production probe requires CADENCE_MATCHING_SQLITE_DB');
  const result = spawnSync('python3', ['-c',
    'import json,sqlite3,sys,uuid; c=sqlite3.connect(sys.argv[1],timeout=30); n=c.execute("select count(*) from tasks where task_list_name=? and task_type=0",(sys.argv[3],)).fetchone()[0]; r=c.execute("select range_id from task_lists where shard_id=0 and domain_id=? and name=? and task_type=0",(uuid.UUID(sys.argv[2]).bytes,sys.argv[3])).fetchone(); print(json.dumps({"queued_rows":n,"range_id":r[0] if r else 0}))',
    database, domainId, taskList], { encoding: 'utf8' });
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
    const wasManaged = managedShardNames.has(shardName);
    managedShardNames.add(shardName);
    const deadline = Date.now() + 30000;
    let executorId, heartbeat;
    while (Date.now() < deadline) {
      const stats = await shardManagerCommand({ op: 'stats' });
      const entries = Object.entries(stats || {}).sort((a, b) =>
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
function call(service, method, request) {
  const taskListName = request.taskList?.name || request.request?.taskList?.name;
  if (taskListName) return registerShardWithGoManager(taskListName).then(() => callRpc(service, method, request));
  return callRpc(service, method, request);
}
function callRpc(service, method, request) {
  const taskListName = request.taskList?.name || request.request?.taskList?.name;
  const metadata = new grpc.Metadata();
  metadata.set('rpc-service', service === 'Matching' ? 'cadence-matching' : 'cadence-frontend');
  metadata.set('rpc-encoding', 'proto');
  metadata.set('rpc-caller', 'cadence-service-compat');
  return new Promise((resolve, reject) => clients[service][method](request, metadata,
    { deadline: Date.now() + 20000 }, (error, response) => {
      if (error) {
        error.message = `${service}.${method} failed${taskListName ? ` for task list ${taskListName}` : ''}: ${error.message}`;
        return reject(error);
      }
      resolve(response);
    }));
}
const payload = data => ({ data: Buffer.from(data) });
async function run() {
  const id = process.env.CADENCE_MATCHING_TEST_ID || randomUUID();
  const domain = `service-compat-${id}`, taskList = { name: `tasks-${id}` };
  await call('DomainAPI', 'RegisterDomain', { name: domain, workflowExecutionRetentionPeriod: { seconds: '259200' } });
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
  const work = path.resolve(process.argv[5]);
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
    if (!fs.existsSync(productionBinary)) throw new Error(`Rust production server binary does not exist: ${productionBinary}`);
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
      'start', '--config', rustConfig, '--services', 'matching', '--port', '0',
      '--history-grpc-port', historyGrpcPort,
      '--matching-grpc-port', process.env.CADENCE_RUST_MATCHING_SECONDARY_GRPC_PORT,
      '--matching-tchannel-port', process.env.CADENCE_RUST_MATCHING_SECONDARY_TCHANNEL,
      '--data-dir', path.join(path.dirname(rustDataDir), 'rust-matching-secondary-data'),
    ], { cwd: path.resolve(__dirname, '..'), env: process.env, stdio: ['ignore', output, output] });
    fs.closeSync(output);
    return child;
  }
  function directMatchingClient(address) {
    return new matchingApi.MatchingAPI(address, grpc.credentials.createInsecure());
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
  function directPartitionOwner(client, domainName, taskListName) {
    return new Promise((resolve, reject) => client.ListTaskListPartitions({
      domain: domainName, taskList: { name: taskListName },
    }, new grpc.Metadata(), { deadline: Date.now() + 10000 }, (error, response) => {
      if (error) return reject(error);
      const partitions = response.decisionTaskListPartitions || [];
      resolve(partitions.find(partition => partition.key === taskListName)?.ownerHostName || '');
    }));
  }
  async function waitForRustReady(label) {
    const deadline = Date.now() + 180000;
    let lastReadinessError;
    while (Date.now() < deadline) {
      if (productionMode) {
        try {
          // This public Go Frontend call must resolve Matching through Go's
          // real Ringpop PeerProvider; TCP readiness alone is insufficient.
          await call('WorkflowAPI', 'DescribeTaskList', {
            domain, taskList: { name: `discovery-ready-${id}` },
            taskListType: 'TASK_LIST_TYPE_DECISION', includeTaskListStatus: true,
          });
          return;
        } catch (error) {
          lastReadinessError = error;
          // JOIN, SWIM, label publication, or Go membership propagation may
          // still be in progress. The timeout below leaves the cause in logs.
        }
      } else if (fs.existsSync(ready)) {
        return;
      }
      if (rust.exitCode !== null || rust.signalCode !== null) throw new Error(`Rust Matching ${label} exited before readiness; see ${work}/rust-server-${label}.log`);
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    throw new Error(`Rust Matching ${label} did not become discoverable through Go Frontend; last readiness error: ${lastReadinessError?.details || lastReadinessError?.message || 'none'}; see ${work}/rust-server-${label}.log`);
  }
  let exited;
  let rust = startRust('before-restart');
  let secondaryRust;
  let semaphoreEvidence;
  let semaphoreAcquire;
  let semaphoreOwner;
  try {
    await waitForRustReady('before-restart');
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
    const convergenceDeadline = Date.now() + 30000;
    while (Date.now() < convergenceDeadline) {
      if (secondaryRust.exitCode !== null || secondaryRust.signalCode !== null) throw new Error(`secondary Rust Matching exited; see ${work}/rust-server-secondary.log`);
      try {
        await call('WorkflowAPI', 'DescribeTaskList', { domain, taskList: { name: `secondary-ready-${id}` }, taskListType: 'TASK_LIST_TYPE_DECISION', includeTaskListStatus: true });
        break;
      } catch (_) {
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    }
    if (Date.now() >= convergenceDeadline) throw new Error('second Rust Matching instance did not join the original Ringpop cluster');
    if (cassandraGate) {
      const semaphoreName = `durable-${id}`;
      const createdSemaphore = await call('SemaphoreAPI', 'CreateSemaphore', {
        domain, semaphoreName, capacity: 1, bucketCapacity: 1,
      });
      assert.equal(createdSemaphore.semaphore.capacity, 1);
      assert.equal(createdSemaphore.semaphore.bucketCapacity, 1);
      const semaphoreClients = [clients.Matching, directMatchingClient(process.env.CADENCE_RUST_MATCHING_SECONDARY_GRPC_PORT)];
      semaphoreAcquire = (owner, clientIndex = 0) => acquireSemaphore(semaphoreClients[clientIndex], {
        domainId, semaphoreName, bucket: 0, ownerId: owner,
      }).catch(error => {
        if ((error.code === grpc.status.ABORTED || error.code === grpc.status.UNAVAILABLE)
            && clientIndex + 1 < semaphoreClients.length) return semaphoreAcquire(owner, clientIndex + 1);
        throw error;
      });
      const owners = [`${id.length}:${id}:${randomUUID()}:1`, `${id.length}:${id}:${randomUUID()}:2`];
      const grants = await Promise.all(owners.map(owner => semaphoreAcquire(owner)));
      const acquired = grants.map((grant, index) => ({ grant, owner: owners[index] }))
        .filter(({ grant }) => grant.outcome === 'SEMAPHORE_ACQUIRE_OUTCOME_ACQUIRED');
      const unavailable = grants.filter(grant => grant.outcome === 'SEMAPHORE_ACQUIRE_OUTCOME_NO_SLOT');
      assert.equal(acquired.length, 1, 'Cassandra semaphore capacity one must grant exactly one concurrent owner');
      assert.equal(unavailable.length, 1, 'Cassandra semaphore capacity one must reject the competing owner');
      const expectedToken = Number(acquired[0].grant.tokenId);
      const stored = cassandraCommand(['verify-semaphore', domainId, semaphoreName, acquired[0].owner, String(expectedToken)], 'semaphore-before-recovery');
      assert.equal(stored.owner_id, acquired[0].owner, 'Go gocql must observe the exact Rust semaphore owner row');
      assert.equal(stored.token_id, expectedToken, 'Go gocql must observe the positive Rust semaphore token row');
      semaphoreOwner = stored.owner_id;
      semaphoreEvidence = { semaphoreName, createdThroughGoFrontend: true, ownerId: semaphoreOwner, tokenId: stored.token_id, concurrentGrants: acquired.length, noSlotResponses: unavailable.length };
    }
    const primaryMembershipPeer = directMatchingClient(`127.0.0.1:${rustGrpcPort}`);
    const secondaryPeer = directMatchingClient(`127.0.0.1:${process.env.CADENCE_RUST_MATCHING_SECONDARY_GRPC_PORT}`);
    const primaryAddress = `127.0.0.1:${rustTChannel}`;
    const secondaryAddress = `127.0.0.1:${process.env.CADENCE_RUST_MATCHING_SECONDARY_TCHANNEL}`;
    const ownerProbeNames = Array.from({ length: 64 }, (_, index) => `ringpop-convergence-${id}-${index}`);
    const ringpopDeadline = Date.now() + 30000;
    let observedOwners = new Map();
    while (Date.now() < ringpopDeadline) {
      const observations = await Promise.all(ownerProbeNames.map(async name => {
        const [primaryOwner, secondaryOwner] = await Promise.all([
          directPartitionOwner(primaryMembershipPeer, domain, name),
          directPartitionOwner(secondaryPeer, domain, name),
        ]);
        return [name, primaryOwner, secondaryOwner];
      }));
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
    const idleDeadline = Date.now() + 10000;
    for (;;) {
      const info = await call('WorkflowAPI', 'DescribeTaskList', { domain, taskList: idleList, taskListType: 'TASK_LIST_TYPE_DECISION', includeTaskListStatus: true });
      if (info.pollers.some(poller => poller.identity === 'production-idle-probe')) break;
      if (Date.now() > idleDeadline) throw new Error('production idle probe poll was not registered');
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
    const queued = await taskObserver.observe(domainId, crashList, 20, 'task-before-crash');
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
    if (cassandraGate && semaphoreEvidence) {
      const recovered = await semaphoreAcquire(semaphoreOwner);
      assert.equal(Number(recovered.tokenId), semaphoreEvidence.tokenId, 'semaphore hold must survive Matching owner crash and be recovered from Cassandra');
      const afterRestart = cassandraCommand(['verify-semaphore', domainId, semaphoreEvidence.semaphoreName, semaphoreOwner, String(semaphoreEvidence.tokenId)], 'semaphore-after-recovery');
      assert.equal(afterRestart.owner_id, semaphoreOwner);
      assert.equal(afterRestart.token_id, semaphoreEvidence.tokenId);
    }
  }
  let restartEvidence;
  if (restartMode) {
    const restartID = `restart-${id}`, restartTaskList = `restart-tasks-${id}`;
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
    const beforePid = rust.pid;
    rust.kill('SIGKILL');
    const beforeExit = await exited;
    assert.equal(beforeExit.signal, 'SIGKILL', 'first Rust Matching process did not terminate');
    if (goServicesPid) process.kill(goServicesPid, 0);
    rust = startRust('after-restart');
    await waitForRustReady('after-restart');
    const sdk = spawnSync('go', ['run', '.'], { cwd: path.resolve(__dirname, 'go-sdk'), env: { ...process.env, CADENCE_MIXED_MATCHING_DOMAIN: domain, CADENCE_TCHANNEL_PEER: process.argv[6], CADENCE_MIXED_RESTART_WORKFLOW_ID: restartID, CADENCE_MIXED_RESTART_TASK_LIST: restartTaskList }, encoding: 'utf8', timeout: 120000 });
    fs.writeFileSync(path.join(work, 'restart-sdk.log'), `${sdk.stdout || ''}\n${sdk.stderr || ''}`);
    if (sdk.status !== 0) throw new Error(`Existing SDK did not complete pre-restart workflow/activity: ${sdk.error || sdk.status}; see ${work}/restart-sdk.log`);
    const finalHistory = await call('WorkflowAPI', 'GetWorkflowExecutionHistory', { domain, workflowExecution: { workflowId: restartID, runId: restartStarted.runId } });
    const completions = finalHistory.history.events.filter(event => event.workflowExecutionCompletedEventAttributes).length;
    assert.equal(completions, 1, 'restart workflow must have exactly one visible completion');
    restartEvidence = { workflowId: restartID, runId: restartStarted.runId, taskList: restartTaskList, durableTaskRowsBeforeKill: persisted.queued_rows, taskRangeBeforeKill: persisted.range_id, firstRustPid: beforePid, secondRustPid: rust.pid, completionEvents: completions, goServicesPid };
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
  const activity = await call('WorkerAPI', 'PollForActivityTask', { domain, taskList, identity: 'service-probe' });
  assert.equal(activity.input.data.toString(), 'activity');
  assert.equal((await call('WorkerAPI', 'RecordActivityTaskHeartbeat', { taskToken: activity.taskToken, details: payload('heartbeat') })).cancelRequested, false);
  await call('WorkerAPI', 'RespondActivityTaskCompleted', { taskToken: activity.taskToken, result: payload('activity-result'), identity: 'service-probe' });
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
    drainingProcess.kill('SIGTERM');
    const drained = await Promise.race([drainExit, new Promise(resolve => setTimeout(() => resolve(null), 60000))]);
    assert(drained, `Rust Matching did not finish SIGTERM drain; see ${work}/rust-server-before-graceful-restart.log`);
    assert.equal(drained.code, 0, `Rust Matching graceful shutdown failed: ${JSON.stringify(drained)}`);
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
  // cumulative pacing left the last backlog delivery waiting for its token.
  assert.deepEqual(pacedInputs.slice().sort(), ['paced-0', 'paced-1', 'paced-2']);
  assert(pacedLatency.at(-1) >= 500, `paced activity dispatch was not rate limited: ${JSON.stringify(pacedLatency)}`);
  const pacedDescribe = await call('WorkflowAPI', 'DescribeTaskList', { domain, taskList: pacedList, taskListType: 'TASK_LIST_TYPE_ACTIVITY', includeTaskListStatus: true });
  assert.equal(pacedDescribe.taskListStatus.ratePerSecond, 1, 'DescribeTaskList must report the reported backing rate');
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
  clients.Matching = new schema.matching.v1.MatchingAPI(process.argv[4], grpc.credentials.createInsecure());
  const isolatedList = { name: `isolated-${id}` };
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
  assert.equal((await pollB).error?.code, grpc.status.CANCELLED, 'other isolation group must not consume the isolated activity');
  await call('WorkerAPI', 'RespondActivityTaskCompleted', { taskToken: isolatedTask.taskToken, result: payload('isolated-ok') });
  // Empty partition entries must survive Rust -> Go Frontend -> protobufjs.
  // The Go gogo decoder treats an omitted default map value as a nil pointer.
  await call('Matching', 'UpdateTaskListPartitionConfig', {
    domainId, taskList: isolatedList, taskListType: 'TASK_LIST_TYPE_ACTIVITY',
    partitionConfig: { version: '999', numReadPartitions: 2, numWritePartitions: 1,
      readPartitions: { 0: {}, 5: { isolationGroups: ['zone-a'] } }, writePartitions: { 0: {} } },
  });
  const partitionInfo = await call('WorkflowAPI', 'DescribeTaskList', { domain, taskList: isolatedList, taskListType: 'TASK_LIST_TYPE_ACTIVITY', includeTaskListStatus: true });
  assert.equal(partitionInfo.partitionConfig.version, '1');
  assert.deepEqual(partitionInfo.partitionConfig.readPartitions['0'].isolationGroups, []);
  assert.deepEqual(partitionInfo.partitionConfig.readPartitions['5'].isolationGroups, ['zone-a']);
  assert.deepEqual(partitionInfo.partitionConfig.writePartitions['0'].isolationGroups, []);
  const sdk = spawnSync('go', ['run', '.'], { cwd: path.resolve(__dirname, 'go-sdk'), env: { ...process.env, CADENCE_MIXED_MATCHING_DOMAIN: domain, CADENCE_TCHANNEL_PEER: process.argv[6] }, encoding: 'utf8', timeout: 180000 });
  fs.writeFileSync(path.join(work, 'go-sdk.log'), `${sdk.stdout || ''}\n${sdk.stderr || ''}`);
  if (sdk.status !== 0) throw new Error(`Existing SDK against mixed Matching failed: ${sdk.error || sdk.status}; see ${work}/go-sdk.log`);
  console.log(JSON.stringify({ result: productionMode ? (cassandraGate ? 'passed' : 'preliminary-passed') : 'passed', acceptanceEligible: cassandraGate, topology: productionMode ? (cassandraGate ? 'Go Frontend/History/Worker and Rust Matching on shared Go Cassandra schema' : 'Go Frontend/History/Worker with Rust Matching via original Go Ringpop discovery') : 'Go Frontend/History/Worker with Rust Matching RPC', idleUnloadAndRecovery: productionMode ? 'production process idle eviction and recovery' : 'passed', gracefulDrainRecovery: gracefulDrainEvidence || 'not requested', queryDispatch: 'passed', isolationDispatch: 'passed', nativePartitionConfig: 'passed', processRestart: restartEvidence || 'not requested', parityGate: cassandraGate ? 'Cassandra mixed SDK, domain pagination, task rows, semaphore concurrency, SIGKILL and SIGTERM recovery gates passed; service parity audit still required' : (productionMode ? 'production mixed SDK/restart evidence only; Cassandra acceptance requires --cassandra' : 'adapter compatibility evidence only'), peer, domain, execution, attributes }));
  } finally {
    if (productionMode) rust.kill('SIGTERM');
    else fs.writeFileSync(stop, 'stop');
    let shutdownTimer;
    const result = await Promise.race([exited, new Promise(resolve => { shutdownTimer = setTimeout(() => resolve(null), 20000); })]);
    clearTimeout(shutdownTimer);
    if (result === null) { rust.kill('SIGKILL'); await exited; throw new Error('Rust Matching did not stop'); }
    if (result.code !== 0) throw new Error(`Rust Matching exited ${JSON.stringify(result)}; see ${work}/rust-server-after-restart.log`);
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
}
run().catch(error => { console.error(error); process.exitCode = 1; }).finally(() => Object.values(clients).forEach(client => client.close()));
