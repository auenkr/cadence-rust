// Verify Go can read Rust Matching's durable task-list row and the shared
// workflow history after the Rust Matching process has exited.
const assert = require('node:assert/strict');
const path = require('node:path');
const { createRequire } = require('node:module');
const webRequire = createRequire(path.resolve(__dirname, '../cadence-web/package.json'));
const grpc = webRequire('@grpc/grpc-js');
const loader = webRequire('@grpc/proto-loader');
const root = path.resolve(__dirname, '../go/idls/proto');
const files = ['domain', 'workflow'].map(name => path.join(root, `uber/cadence/api/v1/service_${name}.proto`));
const api = grpc.loadPackageDefinition(loader.loadSync(files, {
  includeDirs: [root], enums: String, longs: String, bytes: Buffer, defaults: true, oneofs: true,
})).uber.cadence.api.v1;
const [peer, domain, workflowId, runId, taskList] = process.argv.slice(2);
if (![peer, domain, workflowId, runId, taskList].every(Boolean)) {
  throw new Error('usage: matching-postgres-go-verify.cjs FRONTEND_GRPC DOMAIN WORKFLOW_ID RUN_ID TASK_LIST');
}
const clients = Object.fromEntries(['DomainAPI', 'WorkflowAPI'].map(name => [name,
  new api[name](peer, grpc.credentials.createInsecure())]));
function call(service, method, request) {
  const metadata = new grpc.Metadata();
  metadata.set('rpc-service', 'cadence-frontend');
  metadata.set('rpc-encoding', 'proto');
  metadata.set('rpc-caller', 'cadence-postgres-go-verify');
  return new Promise((resolve, reject) => clients[service][method](request, metadata,
    { deadline: Date.now() + 20000 }, (error, response) => error ? reject(error) : resolve(response)));
}
async function verify() {
  const described = await call('DomainAPI', 'DescribeDomain', { name: domain });
  const domainId = described.domain.id;
  const taskListInfo = await call('WorkflowAPI', 'DescribeTaskList', {
    domain, taskList: { name: taskList }, taskListType: 'TASK_LIST_TYPE_DECISION',
    includeTaskListStatus: true,
  });
  assert.equal(taskListInfo.taskListStatus.taskIdBlock.startId, '1');
  assert.equal(taskListInfo.taskListStatus.taskIdBlock.endId, '100000');
  const history = await call('WorkflowAPI', 'GetWorkflowExecutionHistory', {
    domain, workflowExecution: { workflowId, runId },
  });
  const completed = history.history.events.filter(event => event.workflowExecutionCompletedEventAttributes);
  assert.equal(completed.length, 1, 'Go History must see one completed workflow execution');
  console.log(JSON.stringify({ result: 'passed', domain, domainId, workflowId, runId, taskList,
    goReadTaskListMetadata: true, goReadCompletedWorkflowHistory: true }));
}
verify().catch(error => { console.error(error); process.exitCode = 1; })
  .finally(() => Object.values(clients).forEach(client => client.close()));
