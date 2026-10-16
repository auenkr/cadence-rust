// Real Go Frontend/History with Rust Matching protobuf AddTask/Poll RPCs.
// This boundary probe seeds tasks directly; --rust-matching tests Go routing.
const assert = require('node:assert/strict');
const path = require('node:path');
const fs = require('node:fs');
const os = require('node:os');
const { spawnSync } = require('node:child_process');
const { randomUUID } = require('node:crypto');
const { createRequire } = require('node:module');
const webRequire = createRequire(path.resolve(__dirname, '../cadence-web/package.json'));
const grpc = webRequire('@grpc/grpc-js'), loader = webRequire('@grpc/proto-loader');
const root = path.resolve(__dirname, '..'), publicRoot = path.join(root, 'go/idls/proto'), internalRoot = path.join(root, 'go/proto/internal');
const files = ['domain', 'workflow', 'worker'].map(name => path.join(publicRoot, `uber/cadence/api/v1/service_${name}.proto`));
files.push(path.join(internalRoot, 'uber/cadence/matching/v1/service.proto'));
const schema = grpc.loadPackageDefinition(loader.loadSync(files, { includeDirs: [publicRoot, internalRoot], enums: String, longs: String, bytes: Buffer, defaults: true, oneofs: true })).uber.cadence;
const clients = Object.fromEntries(['DomainAPI', 'WorkflowAPI', 'WorkerAPI'].map(name => [name, new schema.api.v1[name](process.argv[2], grpc.credentials.createInsecure())]));
function call(service, method, request) {
  const metadata = new grpc.Metadata();
  metadata.set('rpc-service', 'cadence-frontend'); metadata.set('rpc-caller', 'rust-matching-history-compat'); metadata.set('rpc-encoding', 'proto');
  return new Promise((resolve, reject) => clients[service][method](request, metadata, { deadline: Date.now() + 20000 }, (error, value) => error ? reject(error) : resolve(value)));
}
const payload = text => ({ data: Buffer.from(text) });
async function run() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'cadence-matching-delivery-'));
  try {
    const id = randomUUID(), domain = `matching-delivery-${id}`, taskList = { name: `queue-${id}` };
    await call('DomainAPI', 'RegisterDomain', { name: domain, workflowExecutionRetentionPeriod: { seconds: '259200' } });
    const domainId = (await call('DomainAPI', 'DescribeDomain', { name: domain })).domain.id;
    const startRequest = { domain, workflowId: id, workflowType: { name: 'DurableMatching' }, taskList, requestId: randomUUID(), executionStartToCloseTimeout: { seconds: '300' }, taskStartToCloseTimeout: { seconds: '20' } };
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
    const history = () => call('WorkflowAPI', 'GetWorkflowExecutionHistory', { domain, workflowExecution: execution });
    function deliver(kind, scheduleId) {
      const fixture = path.join(directory, 'fixture.json'), output = path.join(directory, 'response.bin');
      fs.writeFileSync(fixture, JSON.stringify({ domain, domain_id: domainId, workflow_id: id, run_id: started.runId, task_list: taskList.name, history_peer: process.argv[3], kind, schedule_id: Number(scheduleId), database: path.join(directory, 'matching.db'), output }));
      const command = ['test', '--manifest-path', path.join(root, 'rust/Cargo.toml'), '-p', 'cadence-server', '--test', 'matching_delivery', 'matching_delivery_go_history', '--', '--ignored', '--exact'];
      const result = spawnSync('cargo', command, { cwd: root, env: { ...process.env, CARGO_BUILD_JOBS: process.env.CARGO_BUILD_JOBS || '1', CADENCE_MATCHING_DELIVERY_FIXTURE: fixture }, encoding: 'utf8', timeout: 180000 });
      if (result.status !== 0) throw new Error(`Rust matching delivery failed: ${result.error || ''}\n${result.stdout}\n${result.stderr}`);
      const method = Object.values(schema.matching.v1.MatchingAPI.service).find(value => value.path.endsWith(kind === 'activity' ? '/PollForActivityTask' : '/PollForDecisionTask'));
      return method.responseDeserialize(fs.readFileSync(output));
    }
    let events = (await history()).history.events;
    const first = deliver('decision', events.find(event => event.decisionTaskScheduledEventAttributes).eventId);
    assert.equal(JSON.parse(first.taskToken.toString()).domainId, domainId);
    assert(first.branchToken.length > 0);
    await call('WorkerAPI', 'RespondDecisionTaskCompleted', { taskToken: first.taskToken, identity: 'go-frontend-rust-matching', decisions: [{ scheduleActivityTaskDecisionAttributes: { activityId: 'echo', activityType: { name: 'Echo' }, taskList, input: payload('activity-input'), header: { fields: { trace: payload('opaque-trace') } }, scheduleToCloseTimeout: { seconds: '60' }, scheduleToStartTimeout: { seconds: '30' }, startToCloseTimeout: { seconds: '30' }, heartbeatTimeout: { seconds: '10' } } }] });
    events = (await history()).history.events;
    const activity = deliver('activity', events.find(event => event.activityTaskScheduledEventAttributes).eventId);
    assert.equal(activity.input.data.toString(), 'activity-input'); assert.equal(activity.header.fields.trace.data.toString(), 'opaque-trace');
    assert.equal(activity.workflowType.name, 'DurableMatching'); assert.equal(activity.workflowDomain, domain);
    assert.equal((await call('WorkerAPI', 'RecordActivityTaskHeartbeat', { taskToken: activity.taskToken, details: payload('heartbeat') })).cancelRequested, false);
    await call('WorkerAPI', 'RespondActivityTaskCompleted', { taskToken: activity.taskToken, result: payload('activity-result'), identity: 'go-frontend-rust-matching' });
    events = (await history()).history.events;
    const final = deliver('decision', events.filter(event => event.decisionTaskScheduledEventAttributes).at(-1).eventId);
    await call('WorkerAPI', 'RespondDecisionTaskCompleted', { taskToken: final.taskToken, identity: 'go-frontend-rust-matching', decisions: [{ completeWorkflowExecutionDecisionAttributes: { result: payload('done') } }] });
    events = (await history()).history.events;
    assert.equal(events.length, 11); assert.equal(events.at(-1).workflowExecutionCompletedEventAttributes.result.data.toString(), 'done');
    console.log(JSON.stringify({ result: 'passed', topology: 'Go services with Rust queue/History delivery boundary', domain, execution, historyEvents: events.length }));
  } finally { fs.rmSync(directory, { recursive: true, force: true }); }
}
run().catch(error => { console.error(error); process.exitCode = 1; }).finally(() => Object.values(clients).forEach(client => client.close()));
