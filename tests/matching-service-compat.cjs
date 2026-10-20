// Go Frontend/History/Worker routed to the Rust Matching protobuf listener.
const assert = require('node:assert/strict');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const fs = require('node:fs');
const { spawn, spawnSync } = require('node:child_process');
const { createRequire } = require('node:module');
const webRequire = createRequire(path.resolve(__dirname, '../cadence-web/package.json'));
const grpc = webRequire('@grpc/grpc-js');
const loader = webRequire('@grpc/proto-loader');
const root = path.resolve(__dirname, '../go/idls/proto');
const files = ['domain', 'workflow', 'worker'].map(name => path.join(root, `uber/cadence/api/v1/service_${name}.proto`));
const internalRoot = path.resolve(__dirname, '../go/proto/internal');
files.push(path.join(internalRoot, 'uber/cadence/matching/v1/service.proto'));
const schema = grpc.loadPackageDefinition(loader.loadSync(files, {
  includeDirs: [root, internalRoot], enums: String, longs: String, bytes: Buffer, defaults: true, oneofs: true,
})).uber.cadence;
const api = schema.api.v1;
const peer = process.argv[2] || '127.0.0.1:7833';
const restartMode = process.argv[7] === 'restart';
const goServicesPid = Number(process.argv[8] || 0);
const productionMode = process.argv[9] === 'production';
const clients = Object.fromEntries(['DomainAPI', 'WorkflowAPI', 'WorkerAPI'].map(name => [name, new api[name](peer, grpc.credentials.createInsecure())]));
function call(service, method, request) {
  const metadata = new grpc.Metadata();
  metadata.set('rpc-service', service === 'Matching' ? 'cadence-matching' : 'cadence-frontend');
  metadata.set('rpc-encoding', 'proto');
  metadata.set('rpc-caller', 'cadence-service-compat');
  return new Promise((resolve, reject) => clients[service][method](request, metadata,
    { deadline: Date.now() + 20000 }, (error, response) => error ? reject(error) : resolve(response)));
}
const payload = data => ({ data: Buffer.from(data) });
function durableTaskCount(taskList, fallbackDatabase) {
  if (process.env.CADENCE_MATCHING_MYSQL) {
    const query = `SELECT COUNT(*) FROM tasks WHERE task_list_name='${taskList.replaceAll("'", "''")}' AND task_type=0`;
    const container = process.env.CADENCE_MYSQL_DOCKER_CONTAINER;
    const command = container
      ? ['docker', ['exec', container, 'mysql', '--batch', '--skip-column-names', '-uroot',
        `-p${process.env.CADENCE_MYSQL_PASSWORD || ''}`, process.env.CADENCE_MYSQL_DATABASE, '-e', query]]
      : ['mysql', ['--batch', '--skip-column-names', '--host', process.env.CADENCE_MYSQL_HOST,
        '--port', process.env.CADENCE_MYSQL_PORT, '--user', process.env.CADENCE_MYSQL_USER,
        `--password=${process.env.CADENCE_MYSQL_PASSWORD || ''}`, process.env.CADENCE_MYSQL_DATABASE, '-e', query]];
    const result = spawnSync(command[0], command[1], { encoding: 'utf8' });
    if (result.status === 0) result.stdout = result.stdout.trim();
    return result;
  }
  const database = process.env.CADENCE_MATCHING_SQLITE_DB || fallbackDatabase;
  return spawnSync('python3', ['-c', 'import sqlite3,sys; c=sqlite3.connect(sys.argv[1], timeout=30); print(c.execute("select count(*) from tasks where task_list_name=? and task_type=0", (sys.argv[2],)).fetchone()[0])', database, taskList], { encoding: 'utf8' });
}
async function run() {
  const id = randomUUID(), domain = `service-compat-${id}`, taskList = { name: `tasks-${id}` };
  await call('DomainAPI', 'RegisterDomain', { name: domain, workflowExecutionRetentionPeriod: { seconds: '259200' } });
  const domainId = (await call('DomainAPI', 'DescribeDomain', { name: domain })).domain.id;
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
  async function waitForRustReady(label) {
    const deadline = Date.now() + 180000;
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
        } catch (_) {
          // JOIN, SWIM, label publication, or Go membership propagation may
          // still be in progress. The timeout below leaves the cause in logs.
        }
      } else if (fs.existsSync(ready)) {
        return;
      }
      if (rust.exitCode !== null || rust.signalCode !== null) throw new Error(`Rust Matching ${label} exited before readiness; see ${work}/rust-server-${label}.log`);
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    throw new Error(`Rust Matching ${label} did not become discoverable through Go Frontend; see ${work}/rust-server-${label}.log`);
  }
  let exited;
  let rust = startRust('before-restart');
  try {
    await waitForRustReady('before-restart');
  const startRequest = {
    domain, workflowId: id, workflowType: { name: 'ServiceCompatibility' }, taskList,
    requestId: randomUUID(), executionStartToCloseTimeout: { seconds: '300' },
    taskStartToCloseTimeout: { seconds: '10' }, input: payload('start'),
  };
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
    const persistedDeadline = Date.now() + 20000;
    let persisted;
    while (Date.now() < persistedDeadline) {
      persisted = durableTaskCount(restartTaskList, path.join(work, 'matching.db'));
      if (persisted.status === 0 && Number(persisted.stdout.trim()) > 0) break;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert.equal(persisted?.status, 0, `could not inspect Matching durable queue: ${persisted?.stderr}`);
    assert(Number(persisted.stdout.trim()) > 0, 'workflow task was not persisted before Matching restart');
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
    restartEvidence = { workflowId: restartID, runId: restartStarted.runId, taskList: restartTaskList, durableTaskRowsBeforeKill: Number(persisted.stdout.trim()), firstRustPid: beforePid, secondRustPid: rust.pid, completionEvents: completions, goServicesPid };
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
  const decision = await call('WorkerAPI', 'PollForDecisionTask', { domain, taskList, identity: 'service-probe' });
  assert.equal(decision.workflowExecution.runId, started.runId);
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
    if (!process.env.CADENCE_MATCHING_SQLITE_DB && !process.env.CADENCE_MATCHING_MYSQL) throw new Error('production Matching mode requires a configured persistence probe target');
    const taskDeadline = Date.now() + 20000;
    let queued;
    while (Date.now() < taskDeadline) {
      queued = durableTaskCount(drainTaskList, path.join(work, 'matching.db'));
      if (queued.status === 0 && Number(queued.stdout.trim()) > 0) break;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    assert.equal(queued?.status, 0, `could not inspect pre-drain Matching queue: ${queued?.stderr}`);
    assert(Number(queued.stdout.trim()) > 0, 'Go History did not persist the graceful-drain decision task');
    const drainingPid = rust.pid;
    rust.kill('SIGTERM');
    const drained = await Promise.race([exited, new Promise(resolve => setTimeout(() => resolve(null), 60000))]);
    assert(drained, `Rust Matching did not finish SIGTERM drain; see ${work}/rust-server-before-graceful-restart.log`);
    assert.equal(drained.code, 0, `Rust Matching graceful shutdown failed: ${JSON.stringify(drained)}`);
    if (goServicesPid) process.kill(goServicesPid, 0);
    rust = startRust('after-graceful-drain');
    await waitForRustReady('after-graceful-drain');
    const drainSdk = spawnSync('go', ['run', '.'], { cwd: path.resolve(__dirname, 'go-sdk'), env: {
      ...process.env, CADENCE_MIXED_MATCHING_DOMAIN: domain, CADENCE_TCHANNEL_PEER: process.argv[6],
      CADENCE_MIXED_RESTART_WORKFLOW_ID: drainID, CADENCE_MIXED_RESTART_TASK_LIST: drainTaskList,
    }, encoding: 'utf8', timeout: 120000 });
    fs.writeFileSync(path.join(work, 'graceful-drain-sdk.log'), `${drainSdk.stdout || ''}\n${drainSdk.stderr || ''}`);
    if (drainSdk.status !== 0) throw new Error(`Existing SDK could not resume the task preserved across Rust SIGTERM: ${drainSdk.error || drainSdk.status}; see ${work}/graceful-drain-sdk.log`);
    const drainHistory = await call('WorkflowAPI', 'GetWorkflowExecutionHistory', { domain, workflowExecution: { workflowId: drainID, runId: drainStarted.runId } });
    const drainCompletions = drainHistory.history.events.filter(event => event.workflowExecutionCompletedEventAttributes).length;
    assert.equal(drainCompletions, 1, 'gracefully drained workflow must have exactly one visible completion');
    gracefulDrainEvidence = { workflowId: drainID, runId: drainStarted.runId, taskList: drainTaskList, queuedRowsBeforeSignal: Number(queued.stdout.trim()), drainingPid, recoveredByRustPid: rust.pid, completionEvents: drainCompletions, goServicesPid };
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
  console.log(JSON.stringify({ result: productionMode ? 'preliminary-passed' : 'passed', acceptanceEligible: !productionMode, topology: productionMode ? 'Go Frontend/History/Worker with Rust Matching via original Go Ringpop discovery' : 'Go Frontend/History/Worker with Rust Matching RPC', idleUnloadAndRecovery: productionMode ? 'adapter-only fixture behavior; production process uses SIGTERM drain/recovery' : 'passed', gracefulDrainRecovery: gracefulDrainEvidence || 'adapter fixture stop path only', queryDispatch: 'passed', isolationDispatch: 'passed', nativePartitionConfig: 'passed', processRestart: restartEvidence || 'not requested', parityGate: productionMode ? 'production mixed SDK/restart evidence only; Matching acceptance still requires the complete parity audit and remaining failure/recovery cases' : 'adapter compatibility evidence only', peer, domain, execution, attributes }));
  } finally {
    if (productionMode) rust.kill('SIGTERM');
    else fs.writeFileSync(stop, 'stop');
    let shutdownTimer;
    const result = await Promise.race([exited, new Promise(resolve => { shutdownTimer = setTimeout(() => resolve(null), 20000); })]);
    clearTimeout(shutdownTimer);
    if (result === null) { rust.kill('SIGKILL'); await exited; throw new Error('Rust Matching did not stop'); }
    if (result.code !== 0) throw new Error(`Rust Matching exited ${JSON.stringify(result)}; see ${work}/rust-server-after-restart.log`);
  }
}
run().catch(error => { console.error(error); process.exitCode = 1; }).finally(() => Object.values(clients).forEach(client => client.close()));
