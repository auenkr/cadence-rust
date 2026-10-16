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
async function run() {
  const id = randomUUID(), domain = `service-compat-${id}`, taskList = { name: `tasks-${id}` };
  await call('DomainAPI', 'RegisterDomain', { name: domain, workflowExecutionRetentionPeriod: { seconds: '259200' } });
  const domainId = (await call('DomainAPI', 'DescribeDomain', { name: domain })).domain.id;
  const work = path.resolve(process.argv[5]);
  fs.mkdirSync(work, { recursive: true });
  const ready = path.join(work, 'ready'), stop = path.join(work, 'stop'), fixture = path.join(work, 'fixture.json');
   fs.writeFileSync(fixture, JSON.stringify({ domain, domain_id: domainId, workflow_id: '', run_id: '', task_list: taskList.name, history_peer: process.argv[3], kind: 'server', schedule_id: 0, database: path.join(work, 'matching.db'), output: ready, listen: process.argv[4], stop_file: stop, idle_trigger: path.join(work, 'idle-trigger'), idle_done: path.join(work, 'idle-done'), isolation_groups: ['zone-a', 'zone-b'] }));
  const output = fs.openSync(path.join(work, 'rust-server.log'), 'w');
  const rust = spawn('cargo', ['test', '--manifest-path', path.resolve(__dirname, '../rust/Cargo.toml'), '-p', 'cadence-server', '--test', 'matching_delivery', 'matching_delivery_go_history', '--', '--ignored', '--exact'], { cwd: path.resolve(__dirname, '..'), env: { ...process.env, CARGO_BUILD_JOBS: '1', CADENCE_MATCHING_DELIVERY_FIXTURE: fixture }, stdio: ['ignore', output, output] });
  fs.closeSync(output);
  const exited = new Promise(resolve => rust.once('exit', (code, signal) => resolve({ code, signal })));
  try {
    const deadline = Date.now() + 180000;
    while (!fs.existsSync(ready)) {
      if (rust.exitCode !== null || rust.signalCode !== null || Date.now() > deadline) throw new Error(`Rust Matching startup failed; see ${work}/rust-server.log`);
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  const startRequest = {
    domain, workflowId: id, workflowType: { name: 'ServiceCompatibility' }, taskList,
    requestId: randomUUID(), executionStartToCloseTimeout: { seconds: '300' },
    taskStartToCloseTimeout: { seconds: '10' }, input: payload('start'),
  };
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
  console.log(JSON.stringify({ result: 'passed', topology: 'Go Frontend/History/Worker with Rust Matching RPC', idleUnloadAndRecovery: 'passed', queryDispatch: 'passed', isolationDispatch: 'passed', nativePartitionConfig: 'passed', peer, domain, execution, attributes }));
  } finally {
    fs.writeFileSync(stop, 'stop');
    let shutdownTimer;
    const result = await Promise.race([exited, new Promise(resolve => { shutdownTimer = setTimeout(() => resolve(null), 20000); })]);
    clearTimeout(shutdownTimer);
    if (result === null) { rust.kill('SIGKILL'); await exited; throw new Error('Rust Matching did not stop'); }
    if (result.code !== 0) throw new Error(`Rust Matching exited ${JSON.stringify(result)}; see ${work}/rust-server.log`);
  }
}
run().catch(error => { console.error(error); process.exitCode = 1; }).finally(() => Object.values(clients).forEach(client => client.close()));
