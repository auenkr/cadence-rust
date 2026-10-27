// Diagnostic control for the unchanged adaptive scaler scenario on the original Go cluster.
const assert = require('node:assert/strict');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { createRequire } = require('node:module');
const { runProbeScenarios } = require('./matching-probe-scenarios.cjs');
const webRequire = createRequire(path.resolve(__dirname, '../cadence-web/package.json'));
const grpc = webRequire('@grpc/grpc-js');
const loader = webRequire('@grpc/proto-loader');
const root = path.resolve(__dirname, '../go/idls/proto');
const internalRoot = path.resolve(__dirname, '../go/proto/internal');
const files = ['domain', 'workflow', 'worker'].map(name => path.join(root, `uber/cadence/api/v1/service_${name}.proto`));
files.push(path.join(internalRoot, 'uber/cadence/matching/v1/service.proto'));
const schema = grpc.loadPackageDefinition(loader.loadSync(files, {
  includeDirs: [root, internalRoot], enums: String, longs: String, bytes: Buffer, defaults: true, oneofs: true,
})).uber.cadence;
const api = schema.api.v1;
const frontendPeer = process.argv[2];
const matchingPeer = process.argv[3];
const tchannelPeer = process.argv[4];
const work = process.argv[5];
const clients = Object.fromEntries(['DomainAPI', 'WorkflowAPI', 'WorkerAPI'].map(name => [name, new api[name](frontendPeer, grpc.credentials.createInsecure())]));
const matching = new schema.matching.v1.MatchingAPI(matchingPeer, grpc.credentials.createInsecure());
function call(service, method, request) {
  const metadata = new grpc.Metadata();
  metadata.set('rpc-service', service === 'Matching' ? 'cadence-matching' : 'cadence-frontend');
  metadata.set('rpc-encoding', 'proto'); metadata.set('rpc-caller', 'cadence-adaptive-go-control');
  return new Promise((resolve, reject) => (service === 'Matching' ? matching : clients[service])[method](request, metadata,
    { deadline: Date.now() + 20000 }, (error, value) => error ? reject(error) : resolve(value)));
}
async function main() {
  const id = randomUUID();
  const domain = `adaptive-go-control-${id}`;
  await call('DomainAPI', 'RegisterDomain', { name: domain, workflowExecutionRetentionPeriod: { seconds: '259200' } });
  const domainId = (await call('DomainAPI', 'DescribeDomain', { name: domain })).domain.id;
  // Independent Go roles refresh domain metadata separately. Wait only for
  // this provisioning race before invoking the unchanged adaptive assertions.
  const provisionDeadline = Date.now() + 15000;
  for (;;) {
    try {
      await call('Matching', 'DescribeTaskList', { domainId,
        request: { taskList: { name: 'production-adaptive-scaler-probe' },
          taskListType: 'TASK_LIST_TYPE_DECISION', includeTaskListStatus: true } });
      break;
    } catch (error) {
      if (error.code !== grpc.status.INTERNAL
        || !error.details?.includes('DefaultDomainCache encounter case')
        || Date.now() >= provisionDeadline) throw error;
      await new Promise(resolve => setTimeout(resolve, 200));
    }
  }
  const startRequest = { domain, workflowType: { name: 'main.EchoWorkflow' }, taskList: { name: 'unused' },
    executionStartToCloseTimeout: { seconds: '300' }, taskStartToCloseTimeout: { seconds: '10' },
    input: { data: Buffer.from(JSON.stringify('adaptive-load')) } };
  const directClients = [{ client: matching, tchannel: tchannelPeer }];
  // The shared probe's SDK child uses process.argv[6] for its Matching TChannel endpoint.
  process.argv[6] = tchannelPeer;
  process.env.CADENCE_PROBE_ADAPTIVE_SCALER = '1';
  const probeScenarios = await runProbeScenarios({ grpc, call, directClients, domain, domainId, id, startRequest, work,
    matchingImplementation: 'original Go Matching',
  });
  assert.equal(probeScenarios.adaptiveScaler.result, 'passed');
  const result = { result: 'passed', acceptanceEligible: false,
    topology: 'original Go Frontend/History/Matching/Worker; adaptive control only', probeScenarios };
  require('node:fs').writeFileSync(path.join(work, 'adaptive-go-control.json'), `${JSON.stringify(result, null, 2)}\n`);
  console.log(JSON.stringify(result));
}
main().catch(error => { console.error(error); process.exitCode = 1; })
  .finally(() => { matching.close(); Object.values(clients).forEach(client => client.close()); });
