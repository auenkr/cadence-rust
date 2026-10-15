// Wire compatibility check using cadence-web's upstream Node gRPC client.
// Prerequisites: npm ci && npm run install-idl in cadence-web; Rust server running.
// Run: node tests/grpc-compat.cjs [host:port]
const assert = require('node:assert/strict');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { createRequire } = require('node:module');
const webRequire = createRequire(path.resolve(__dirname, '../cadence-web/package.json'));
const grpc = webRequire('@grpc/grpc-js');
const loader = webRequire('@grpc/proto-loader');
const protobuf = webRequire('protobufjs');
const idl = path.resolve(__dirname, '../cadence-web/node_modules/cadence-idl/proto');
const files = ['domain', 'meta', 'workflow', 'worker', 'visibility', 'schedule'].map(
  name => path.join(idl, `uber/cadence/api/v1/service_${name}.proto`)
);
// The web client's pinned IDL predates the semaphore service; load it from
// the repo's pinned Go IDL instead (same package, newer revision).
const goIdl = path.resolve(__dirname, '../go/idls/proto');
files.push(path.join(goIdl, 'uber/cadence/api/v1/service_semaphore.proto'));
const api = grpc.loadPackageDefinition(loader.loadSync(files, {
  includeDirs: [idl, goIdl], enums: String, longs: String, bytes: Buffer,
  defaults: true, oneofs: true,
})).uber.cadence.api.v1;
const peer = process.argv[2] || '127.0.0.1:7833';
const clients = {};
for (const name of ['DomainAPI', 'MetaAPI', 'WorkflowAPI', 'WorkerAPI', 'VisibilityAPI', 'ScheduleAPI', 'SemaphoreAPI'])
  clients[name] = new api[name](peer, grpc.credentials.createInsecure());
function call(service, method, request) {
  const metadata = new grpc.Metadata();
  metadata.set('rpc-service', 'cadence-frontend');
  metadata.set('rpc-caller', 'cadence-rust-grpc-compat');
  return new Promise((resolve, reject) => clients[service][method](
    request, metadata, { deadline: Date.now() + 5000 },
    (error, result) => error ? reject(error) : resolve(result)
  ));
}
const uuid = randomUUID();
const domain = `rust-grpc-compat-${uuid}`;
const workflowId = `workflow-${uuid}`;
const taskList = { name: 'rust-grpc-compat', kind: 'TASK_LIST_KIND_NORMAL' };
const payload = text => ({ data: Buffer.from(text) });
async function run() {
  assert.equal((await call('MetaAPI', 'Health', {})).ok, true);
  await call('DomainAPI', 'RegisterDomain', {
    name: domain, description: 'Rust gRPC compatibility workflow',
    workflowExecutionRetentionPeriod: { seconds: '259200' },
    data: { implementation: 'rust' },
  });
  const described = await call('DomainAPI', 'DescribeDomain', { name: domain });
  assert.equal(described.domain.name, domain);
  assert.equal(described.domain.status, 'DOMAIN_STATUS_REGISTERED');
  assert(described.domain.activeClusterName);
  assert(described.domain.clusters.some(c => c.clusterName === described.domain.activeClusterName));
  assert.equal(described.domain.workflowExecutionRetentionPeriod.seconds, '259200');
  const byId = await call('DomainAPI', 'DescribeDomain', { id: described.domain.id });
  const globalDomain = `global-${uuid}`;
  await call('DomainAPI', 'RegisterDomain', {
    name: globalDomain, workflowExecutionRetentionPeriod: { seconds: '259200' },
    isGlobalDomain: true, clusters: [{ clusterName: 'remote' }, { clusterName: 'cluster0' }],
    activeClusterName: 'remote',
  });
  const failed = await call('DomainAPI', 'FailoverDomain', { domainName: globalDomain, domainActiveClusterName: 'cluster0' });
  assert.equal(failed.domain.activeClusterName, 'cluster0');
  const globalId = (await call('DomainAPI', 'DescribeDomain', { name: globalDomain })).domain.id;
  const failoverHistory = await call('DomainAPI', 'ListFailoverHistory', { filters: { domainId: globalId } });
  assert.equal(failoverHistory.failoverEvents.length, 1);
  assert.equal(failoverHistory.failoverEvents[0].clusterFailovers[0].toCluster.activeClusterName, 'cluster0');
  assert.equal(byId.domain.name, domain);
  let token = Buffer.alloc(0), domains = [];
  do {
    const page = await call('DomainAPI', 'ListDomains', { pageSize: 1, nextPageToken: token });
    domains.push(...page.domains);
    token = page.nextPageToken;
  } while (token.length);
  assert(domains.some(d => d.name === domain));
  await assert.rejects(call('DomainAPI', 'ListDomains', {
    pageSize: 1, nextPageToken: Buffer.from('invalid'),
  }), e => e.code === grpc.status.INVALID_ARGUMENT);

  const start = {
    domain, workflowId, workflowType: { name: 'RustGrpcCompatibilityWorkflow' },
    taskList, input: payload('start-input'), requestId: randomUUID(),
    executionStartToCloseTimeout: { seconds: '300' },
    taskStartToCloseTimeout: { seconds: '10' },
    workflowIdReusePolicy: 'WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE',
  };
  const started = await call('WorkflowAPI', 'StartWorkflowExecution', start);
  assert.equal((await call('WorkflowAPI', 'StartWorkflowExecution', start)).runId, started.runId);
  await assert.rejects(call('WorkflowAPI', 'StartWorkflowExecution', {
    ...start, requestId: randomUUID(),
  }), e => {
    assert.equal(e.code, grpc.status.ALREADY_EXISTS);
    const encoded = e.metadata.get('grpc-status-details-bin')[0];
    assert(Buffer.isBuffer(encoded));
    const root = protobuf.parse('syntax="proto3"; message Any { string type_url=1; bytes value=2; } message Status { int32 code=1; string message=2; repeated Any details=3; } message StartedError { string start_request_id=1; string run_id=2; }').root;
    const detail = root.lookupType('Status').decode(encoded).details[0];
    assert.equal(detail.typeUrl, 'type.googleapis.com/uber.cadence.api.v1.WorkflowExecutionAlreadyStartedError');
    assert.equal(root.lookupType('StartedError').decode(detail.value).runId, started.runId);
    return true;
  });
  const execution = { workflowId, runId: started.runId };
  const initial = await call('WorkerAPI', 'PollForDecisionTask', { domain, taskList, identity: 'node-compat' });
  assert(initial.taskToken.length > 0);
  assert.equal(JSON.parse(initial.taskToken.toString()).domainId, described.domain.id);
  assert.equal(initial.history.events[0].workflowExecutionStartedEventAttributes.input.data.toString(), 'start-input');
  assert.equal(initial.history.events[0].workflowExecutionStartedEventAttributes.executionStartToCloseTimeout.seconds, '300');
  await call('WorkerAPI', 'RespondDecisionTaskCompleted', {
    taskToken: initial.taskToken, identity: 'node-compat', decisions: [{
      scheduleActivityTaskDecisionAttributes: {
        activityId: 'activity-1', activityType: { name: 'EchoActivity' }, taskList,
        input: payload('activity-input'),
        scheduleToCloseTimeout: { seconds: '60' },
        scheduleToStartTimeout: { seconds: '30' },
        startToCloseTimeout: { seconds: '30' },
        heartbeatTimeout: { seconds: '10' },
        header: { fields: { trace: payload('opaque-trace') } },
      },
    }],
  });
  const activity = await call('WorkerAPI', 'PollForActivityTask', { domain, taskList, identity: 'node-compat' });
  assert.equal(activity.activityId, 'activity-1');
  assert.equal(activity.input.data.toString(), 'activity-input');
  assert.equal(activity.header.fields.trace.data.toString(), 'opaque-trace');
  assert.equal((await call('WorkerAPI', 'RecordActivityTaskHeartbeat', {
    taskToken: activity.taskToken, details: payload('heartbeat'),
  })).cancelRequested, false);
  await call('WorkerAPI', 'RespondActivityTaskCompleted', {
    taskToken: activity.taskToken, result: payload('activity-output'), identity: 'node-compat',
  });
  const final = await call('WorkerAPI', 'PollForDecisionTask', { domain, taskList, identity: 'node-compat' });
  assert(final.history.events.some(e => e.activityTaskCompletedEventAttributes?.result.data.toString() === 'activity-output'));
  await call('WorkerAPI', 'RespondDecisionTaskCompleted', {
    taskToken: final.taskToken, identity: 'node-compat', decisions: [{
      completeWorkflowExecutionDecisionAttributes: { result: payload('workflow-output') },
    }],
  });
  const result = await call('WorkflowAPI', 'DescribeWorkflowExecution', { domain, workflowExecution: execution });
  assert.equal(result.workflowExecutionInfo.closeStatus, 'WORKFLOW_EXECUTION_CLOSE_STATUS_COMPLETED');
  assert(result.workflowExecutionInfo.startTime.seconds);
  assert.equal(result.workflowExecutionInfo.taskList, taskList.name);
  assert.equal(result.workflowExecutionInfo.executionStatus, 'WORKFLOW_EXECUTION_STATUS_COMPLETED');
  const list = await call('VisibilityAPI', 'ListWorkflowExecutions', { domain, pageSize: 100, query: 'ORDER BY StartTime DESC' });
  assert.equal(list.executions[0].workflowExecution.runId, started.runId);
  assert.equal((await call('VisibilityAPI', 'CountWorkflowExecutions', { domain })).count, '1');
  token = Buffer.alloc(0);
  const events = [];
  do {
    const page = await call('WorkflowAPI', 'GetWorkflowExecutionHistory', {
      domain, workflowExecution: execution, pageSize: 2, nextPageToken: token,
    });
    events.push(...page.history.events);
    token = page.nextPageToken;
  } while (token.length);
  assert.equal(events.length, 11);
  assert.equal(events.at(-1).workflowExecutionCompletedEventAttributes.result.data.toString(), 'workflow-output');
  await assert.rejects(call('WorkflowAPI', 'SignalWorkflowExecution', {
    domain, workflowExecution: execution, signalName: 'too-late',
  }), e => e.code === grpc.status.NOT_FOUND);
  const asyncId = randomUUID();
  await call('WorkflowAPI', 'StartWorkflowExecutionAsync', { request: { domain, workflowId: `async-${asyncId}`, workflowType: { name: 'AsyncCompat' }, taskList, requestId: asyncId, executionStartToCloseTimeout: { seconds: '60' }, taskStartToCloseTimeout: { seconds: '10' } } });
  let asyncHistory;
  for (let i = 0; i < 20; i++) {
    try {
      asyncHistory = await call('WorkflowAPI', 'GetWorkflowExecutionHistory', { domain, workflowExecution: { workflowId: `async-${asyncId}` } });
      break;
    } catch (e) {
      if (e.code !== grpc.status.NOT_FOUND) throw e;
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  }
  assert(asyncHistory && asyncHistory.history.events.length >= 2, 'async start was consumed into a run');
  const decisionEvents = await call('WorkflowAPI', 'GetWorkflowExecutionHistory', { domain, workflowExecution: execution });
  const startedBoundary = decisionEvents.history.events.find(e => e.decisionTaskStartedEventAttributes);
  const resetted = await call('WorkflowAPI', 'ResetWorkflowExecution', { domain, workflowExecution: execution, reason: 'grpc-compat', decisionFinishEventId: startedBoundary.eventId, requestId: randomUUID() });
  assert(resetted.runId && resetted.runId !== started.runId);
  const restarted = await call('WorkflowAPI', 'RestartWorkflowExecution', { domain, workflowExecution: execution, identity: 'grpc-compat' });
  assert(restarted.runId && restarted.runId !== started.runId);
  await call('WorkflowAPI', 'RefreshWorkflowTasks', { domain, workflowExecution: execution });
  try {
    await call('DomainAPI', 'RegisterDomain', { name: 'cadence-system', workflowExecutionRetentionPeriod: { seconds: '259200' } });
  } catch (e) {
    if (e.code !== grpc.status.ALREADY_EXISTS) throw e;
  }
  const diagnosis = await call('WorkflowAPI', 'DiagnoseWorkflowExecution', { domain, workflowExecution: execution, identity: 'grpc-compat' });
  assert.equal(diagnosis.domain, 'cadence-system');
  assert(diagnosis.diagnosticWorkflowExecution.runId);
  await assert.rejects(call('VisibilityAPI', 'ListArchivedWorkflowExecutions', { domain }), e => e.code === grpc.status.INVALID_ARGUMENT && /not configured for visibility archival/.test(e.details));
  try {
    await call('ScheduleAPI', 'CreateSchedule', {
      domain, scheduleId: `compat-${uuid}`,
      spec: { cronExpression: '* * * * *' },
      action: { startWorkflow: { workflowType: { name: 'CompatScheduled' }, taskList, executionStartToCloseTimeout: { seconds: '60' }, taskStartToCloseTimeout: { seconds: '10' } } },
    });
    const schedDesc = await call('ScheduleAPI', 'DescribeSchedule', { domain, scheduleId: `compat-${uuid}` });
    assert.equal(schedDesc.spec.cronExpression, '* * * * *');
    const schedList = await call('ScheduleAPI', 'ListSchedules', { domain });
    assert(schedList.schedules.some(s => s.scheduleId === `compat-${uuid}`));
    await call('ScheduleAPI', 'PauseSchedule', { domain, scheduleId: `compat-${uuid}`, reason: 'compat' });
    await call('ScheduleAPI', 'UnpauseSchedule', { domain, scheduleId: `compat-${uuid}` });
    await call('ScheduleAPI', 'DeleteSchedule', { domain, scheduleId: `compat-${uuid}` });
    console.log(JSON.stringify({ scheduleCheck: 'passed' }));
  } catch (e) {
    // Schedules stay disabled by default like Go (worker.enableScheduler=false).
    assert(/not enabled for domain/.test(e.details), `unexpected schedule error: ${e}`);
    console.log(JSON.stringify({ scheduleCheck: 'disabled' }));
  }
  try {
    const created = await call('SemaphoreAPI', 'CreateSemaphore', { domain, semaphoreName: `compat-${uuid}`, capacity: 4, bucketCapacity: 2 });
    assert.equal(created.semaphore.capacity, 4);
    assert.equal(created.semaphore.bucketCapacity, 2);
    console.log(JSON.stringify({ semaphoreCheck: 'passed' }));
  } catch (e) {
    // Semaphores stay disabled by default like Go (system.enableDistributedSemaphore=false).
    assert(/not enabled for domain/.test(e.details), `unexpected semaphore error: ${e}`);
    console.log(JSON.stringify({ semaphoreCheck: 'disabled' }));
  }
  const listed = await call('WorkflowAPI', 'DescribeTaskList', { domain, taskList, includeTaskListStatus: true });
  assert(listed.taskListStatus);
  const byDomain = await call('WorkflowAPI', 'GetTaskListsByDomain', { domain });
  assert(byDomain.decisionTaskListMap[taskList.name]);
  const partitions = await call('WorkflowAPI', 'ListTaskListPartitions', { domain, taskList });
  assert.equal(partitions.decisionTaskListPartitions[0].key, taskList.name);
  assert.equal(partitions.activityTaskListPartitions[0].key, taskList.name);
  console.log(JSON.stringify({ peer, domain, workflowId, runId: started.runId, historyEvents: events.length, result: 'passed' }));
}
run().catch(error => { console.error(error); process.exitCode = 1; }).finally(() => {
  for (const client of Object.values(clients)) client.close();
});
