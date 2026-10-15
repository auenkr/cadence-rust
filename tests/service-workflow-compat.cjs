// The same public workflow probe runs against all-Go and mixed-service clusters.
const assert = require('node:assert/strict');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
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
const matchingPeer = process.argv[3];
if (matchingPeer) clients.Matching = new schema.matching.v1.MatchingAPI(matchingPeer, grpc.credentials.createInsecure());
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
  if (matchingPeer) {
    const domainId = (await call('DomainAPI', 'DescribeDomain', { name: domain })).domain.id;
    const rootList = { name: `partition-${id}`, kind: 'TASK_LIST_KIND_NORMAL' };
    const config = { version: '900', numReadPartitions: 2, numWritePartitions: 1, readPartitions: { 0: { isolationGroups: ['a'] }, 1: { isolationGroups: ['b'] } }, writePartitions: { 0: { isolationGroups: ['a'] } } };
    const update = partitionConfig => call('Matching', 'UpdateTaskListPartitionConfig', { domainId, taskList: rootList, taskListType: 'TASK_LIST_TYPE_ACTIVITY', partitionConfig });
    // Domain cache notifications propagate between independent Go roles.
    // Retry only that provisioning race, before testing actual config behavior.
    const deadline = Date.now() + 15000;
    for (;;) {
      try { await update(config); break; }
      catch (error) {
        if (!error.details?.includes('DefaultDomainCache encounter case') || Date.now() >= deadline) throw error;
        await new Promise(resolve => setTimeout(resolve, 200));
      }
    }
    await update({ ...config, version: '-10' });
    const info = await call('Matching', 'DescribeTaskList', { domainId, request: { taskList: rootList, taskListType: 'TASK_LIST_TYPE_ACTIVITY' } });
    assert.equal(info.partitionConfig.version, '1');
    await assert.rejects(update({}), error => error.code === grpc.status.INVALID_ARGUMENT && error.details === 'read partitions < 1');
    await assert.rejects(call('Matching', 'RefreshTaskListPartitionConfig', { domainId, taskList: rootList, partitionConfig: config }), error => error.code === grpc.status.INVALID_ARGUMENT && error.details === 'PartitionConfig must be nil for root partition.');
    const partitions = await call('Matching', 'ListTaskListPartitions', { domain, taskList: { name: `/__cadence_sys/${rootList.name}/2` } });
    assert.equal(partitions.decisionTaskListPartitions[0].key, rootList.name);
  }
  const started = await call('WorkflowAPI', 'StartWorkflowExecution', {
    domain, workflowId: id, workflowType: { name: 'ServiceCompatibility' }, taskList,
    requestId: randomUUID(), executionStartToCloseTimeout: { seconds: '300' },
    taskStartToCloseTimeout: { seconds: '10' }, input: payload('start'),
  });
  const execution = { workflowId: id, runId: started.runId };
  const decision = await call('WorkerAPI', 'PollForDecisionTask', { domain, taskList, identity: 'service-probe' });
  assert.equal(decision.workflowExecution.runId, started.runId);
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
  console.log(JSON.stringify({ result: 'passed', peer, domain, execution, attributes }));
}
run().catch(error => { console.error(error); process.exitCode = 1; }).finally(() => Object.values(clients).forEach(client => client.close()));
