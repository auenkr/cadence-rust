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
const [peer, domain, workflowId, runId, taskList, consumedWorkflowId, consumedRunId, consumedTaskList] = process.argv.slice(2);
if (![peer, domain, workflowId, runId, taskList].every(Boolean)
    || [consumedWorkflowId, consumedRunId, consumedTaskList].some(Boolean)
      && ![consumedWorkflowId, consumedRunId, consumedTaskList].every(Boolean)) {
  throw new Error('usage: matching-postgres-go-verify.cjs FRONTEND_GRPC DOMAIN WORKFLOW_ID RUN_ID TASK_LIST');
}
// Go's DescribeTaskList can lease a list and persist a new range, so the block
// is recorded here and checked by the runner against the post-call range row.
const taskIdBlock = status => ({ startId: status.taskIdBlock.startId, endId: status.taskIdBlock.endId });
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
  const taskBlocks = { taskList: taskIdBlock(taskListInfo.taskListStatus) };
  const history = await call('WorkflowAPI', 'GetWorkflowExecutionHistory', {
    domain, workflowExecution: { workflowId, runId },
  });
  const completed = history.history.events.filter(event => event.workflowExecutionCompletedEventAttributes);
  assert.equal(completed.length, 1, 'Go History must see one completed workflow execution');
  let goConsumedRustTask;
  if (consumedWorkflowId) {
    const consumed = await call('WorkflowAPI', 'GetWorkflowExecutionHistory', {
      domain, workflowExecution: { workflowId: consumedWorkflowId, runId: consumedRunId },
    });
    const completionEvents = consumed.history.events.filter(event => event.workflowExecutionCompletedEventAttributes);
    assert.equal(completionEvents.length, 1, 'Go Matching must consume the Rust-produced task and complete it once');
    // The Go SDK stores the workflow result as JSON with a trailing newline.
    assert.equal(JSON.parse(completionEvents[0].workflowExecutionCompletedEventAttributes.result.data.toString()), 'restart-proof');
    const consumedTaskList = await call('WorkflowAPI', 'DescribeTaskList', {
      domain, taskList: { name: consumedTaskList }, taskListType: 'TASK_LIST_TYPE_DECISION',
      includeTaskListStatus: true,
    });
    taskBlocks.consumedTaskList = taskIdBlock(consumedTaskList.taskListStatus);
    goConsumedRustTask = true;
  }
  console.log(JSON.stringify({ result: 'passed', domain, domainId, workflowId, runId, taskList, taskBlocks,
    goReadTaskListMetadata: true, goReadCompletedWorkflowHistory: true,
    goMatchingConsumedRustProducedPendingTask: goConsumedRustTask || false,
    consumedWorkflowId: consumedWorkflowId || null }));
}
verify().catch(error => { console.error(error); process.exitCode = 1; })
  .finally(() => Object.values(clients).forEach(client => client.close()));
