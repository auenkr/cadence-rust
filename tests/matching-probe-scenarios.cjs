// Opt-in probe scenarios for the mixed-cluster Matching gate. Each one is enabled by its own
// environment flag (set by tests/run-go-service-baseline.py) and is called from
// tests/matching-service-compat.cjs with the probe's live helpers. See
// memory/matching-probe-scenarios-20261023.md for the flags and what a pass looks like.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { spawnSync } = require('node:child_process');
const { observeProductionMySqlDomain, verifyProductionMySqlTaskGc } = require('./matching-mysql-gate.cjs');

const sleep = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds));
const payload = data => ({ data: Buffer.from(data) });
const partitionName = (root, index) => (index === 0 ? root : `/__cadence_sys/${root}/${index}`);

// ctx: { grpc, call, directClients: [{ client, tchannel }], domain, domainId, id, startRequest, work, durableTaskCount }
function ownerRpc(ctx, method, request) {
  const attempt = ({ client }) => new Promise((resolve, reject) => {
    const metadata = new ctx.grpc.Metadata();
    metadata.set('rpc-service', 'cadence-matching');
    metadata.set('rpc-encoding', 'proto');
    metadata.set('rpc-caller', 'cadence-probe-scenarios');
    client[method](request, metadata, { deadline: Date.now() + 25000 }, (error, response) => (error ? reject(error) : resolve(response)));
  });
  return (async () => {
    let last;
    for (const node of ctx.directClients) {
      try { return await attempt(node); } catch (error) {
        if (error.code !== ctx.grpc.status.ABORTED) throw error;
        last = error;
      }
    }
    throw last;
  })();
}

// Starts a workflow, schedules one activity on a list nobody polls, returns the identifiers Matching needs.
async function scheduleActivity(ctx, label, activityId) {
  const workflowId = `${label}-${ctx.id}`;
  const decisionList = { name: `${label}-decision-${ctx.id}` };
  const started = await ctx.call('WorkflowAPI', 'StartWorkflowExecution', { ...ctx.startRequest, workflowId, taskList: decisionList, requestId: randomUUID() });
  const decision = await ctx.call('WorkerAPI', 'PollForDecisionTask', { domain: ctx.domain, taskList: decisionList, identity: `${label}-scheduler` });
  await ctx.call('WorkerAPI', 'RespondDecisionTaskCompleted', { taskToken: decision.taskToken, decisions: [{ scheduleActivityTaskDecisionAttributes: {
    activityId, activityType: { name: 'Echo' }, taskList: { name: `${label}-unpolled-${ctx.id}` },
    scheduleToCloseTimeout: { seconds: '60' }, scheduleToStartTimeout: { seconds: '60' }, startToCloseTimeout: { seconds: '30' },
  } }] });
  const deadline = Date.now() + 10000;
  for (;;) {
    const history = await ctx.call('WorkflowAPI', 'GetWorkflowExecutionHistory', { domain: ctx.domain, workflowExecution: { workflowId, runId: started.runId } });
    const scheduled = history.history.events.find(event => event.activityTaskScheduledEventAttributes);
    if (scheduled) return { workflowId, runId: started.runId, scheduleId: scheduled.eventId };
    if (Date.now() > deadline) throw new Error(`${label} activity was not scheduled`);
    await sleep(20);
  }
}

async function waitForRows(ctx, listName, taskType, label, atLeast = 1) {
  const deadline = Date.now() + 20000;
  for (;;) {
    const rows = ctx.durableTaskCount(ctx.domainId, listName, taskType, undefined);
    assert.equal(rows.status, 0, `could not count ${label} rows: ${rows.stderr}`);
    if (Number(String(rows.stdout).trim()) >= atLeast) return Number(String(rows.stdout).trim());
    if (Date.now() > deadline) throw new Error(`${label} rows were not persisted under ${listName}`);
    await sleep(100);
  }
}

// CADENCE_PROBE_PG_GC_DOMAIN: the MySQL lane's GC convergence and domain metadata checks, run on PostgreSQL.
// The shared MySQL verifier is reused through a query adapter that rewrites its MySQL-only SQL.
function postgresQuery(sql) {
  const translated = sql
    .replace(/UNHEX\('([0-9a-fA-F]*)'\)/g, "decode('$1','hex')")
    .replace(/HEX\((data|id)\)/g, "encode($1,'hex')")
    .replace(/, is_global,/, ', is_global::int,');
  const env = { ...process.env, PGHOST: process.env.CADENCE_POSTGRES_HOST, PGPORT: process.env.CADENCE_POSTGRES_PORT,
    PGUSER: process.env.CADENCE_POSTGRES_USER, PGDATABASE: process.env.CADENCE_POSTGRES_DATABASE,
    PGPASSWORD: process.env.CADENCE_POSTGRES_PASSWORD || '', PGSSLMODE: process.env.CADENCE_POSTGRES_SSLMODE || 'disable' };
  for (const [name, value] of [['PGSSLROOTCERT', process.env.CADENCE_POSTGRES_CA_FILE], ['PGSSLCERT', process.env.CADENCE_POSTGRES_CERT_FILE], ['PGSSLKEY', process.env.CADENCE_POSTGRES_KEY_FILE]]) {
    if (value) env[name] = value;
  }
  const result = spawnSync('psql', ['-X', '-A', '-t', '-w', '-v', 'ON_ERROR_STOP=1', '-F', '\t', '-c', translated], { encoding: 'utf8', env });
  if (result.status !== 0) throw new Error(`PostgreSQL query failed: ${translated}: ${result.stderr}`);
  return result.stdout.split('\n').map(line => line.trim()).filter(line => line.length > 0).map(line => line.split('\t'));
}

async function postgresGcAndDomain(ctx) {
  const ackedList = { name: `pg-gc-acked-${ctx.id}` };
  const pendingList = { name: `pg-gc-pending-${ctx.id}` };
  const acked = await ctx.call('WorkflowAPI', 'StartWorkflowExecution', { ...ctx.startRequest, workflowId: ackedList.name, taskList: ackedList, requestId: randomUUID() });
  await waitForRows(ctx, ackedList.name, 0, 'acked decision');
  const watermark = Number(postgresQuery(`SELECT MAX(task_id) FROM tasks WHERE domain_id=UNHEX('${ctx.domainId.replace(/-/g, '')}') AND task_list_name='${ackedList.name}' AND task_type=0`)[0][0]);
  assert(Number.isFinite(watermark), 'PostgreSQL decision task row has no task id');
  // Poll and complete the decision so Matching acknowledges the task; the GC must then remove the row.
  const decision = await ctx.call('WorkerAPI', 'PollForDecisionTask', { domain: ctx.domain, taskList: ackedList, identity: 'pg-gc-probe' });
  assert.equal(decision.workflowExecution.runId, acked.runId);
  await ctx.call('WorkerAPI', 'RespondDecisionTaskCompleted', { taskToken: decision.taskToken, identity: 'pg-gc-probe', decisions: [] });
  // A task nobody acknowledges has to survive the collector.
  await ctx.call('WorkflowAPI', 'StartWorkflowExecution', { ...ctx.startRequest, workflowId: pendingList.name, taskList: pendingList, requestId: randomUUID() });
  await waitForRows(ctx, pendingList.name, 0, 'pending control');
  const liveBlockProvider = async (listName, taskType) => {
    const status = (await ctx.call('WorkflowAPI', 'DescribeTaskList', { domain: ctx.domain, taskList: { name: listName },
      taskListType: taskType === 0 ? 'TASK_LIST_TYPE_DECISION' : 'TASK_LIST_TYPE_ACTIVITY', includeTaskListStatus: true })).taskListStatus;
    return { startId: Number(status.taskIdBlock.startId), endId: Number(status.taskIdBlock.endId) };
  };
  const taskGarbageCollection = await verifyProductionMySqlTaskGc({
    query: postgresQuery, domainId: ctx.domainId,
    taskLists: [{ label: 'pgDecision', name: ackedList.name, taskType: 0, observedMaxTaskId: watermark }],
    pendingListName: pendingList.name, liveBlockProvider, rangeSize: 100000, sleep,
  });
  const domainMetadata = observeProductionMySqlDomain({ query: postgresQuery, domain: ctx.domain, domainId: ctx.domainId });
  return { result: 'passed', ackedList: ackedList.name, pendingList: pendingList.name, watermark, taskGarbageCollection, domainMetadata };
}

// CADENCE_PROBE_QUERY_STANDBY: the only poller polls the root (parent) partition of a two-partition list.
// Queries are sent to the child partition directly (Matching must forward to the parent) and through Go Frontend.
async function queryStandby(ctx) {
  const rootName = `query-standby-${ctx.id}`;
  const root = { name: rootName }, child = { name: partitionName(rootName, 1) };
  await ownerRpc(ctx, 'UpdateTaskListPartitionConfig', { domainId: ctx.domainId, taskList: root, taskListType: 'TASK_LIST_TYPE_DECISION',
    partitionConfig: { version: '999', numReadPartitions: 2, numWritePartitions: 2, readPartitions: { 0: {}, 1: {} }, writePartitions: { 0: {}, 1: {} } } });
  const started = await ctx.call('WorkflowAPI', 'StartWorkflowExecution', { ...ctx.startRequest, workflowId: rootName, taskList: root, requestId: randomUUID() });
  const rootPoll = () => ownerRpc(ctx, 'PollForDecisionTask', { domainId: ctx.domainId, pollerId: randomUUID(), request: { domain: ctx.domain, taskList: root, identity: 'query-standby-root-poller' } });
  let first;
  for (const deadline = Date.now() + 30000; !first?.taskToken?.length; ) {
    first = await rootPoll();
    if (Date.now() > deadline) throw new Error('root-only poller never received the workflow decision task');
  }
  await ctx.call('WorkerAPI', 'RespondDecisionTaskCompleted', { taskToken: first.taskToken, identity: 'query-standby-root-poller', decisions: [] });
  const answerOne = async (label, send) => {
    const pending = rootPoll();
    const deadline = Date.now() + 10000;
    for (;;) {
      const info = await ownerRpc(ctx, 'DescribeTaskList', { domainId: ctx.domainId, request: { taskList: root, taskListType: 'TASK_LIST_TYPE_DECISION' } });
      if (info.pollers.some(poller => poller.identity === 'query-standby-root-poller')) break;
      if (Date.now() > deadline) throw new Error(`${label}: root poller not registered`);
      await sleep(20);
    }
    const answer = send();
    const task = await pending;
    assert.equal(task.query?.queryType, 'standby', `${label}: root-only poller must receive the query`);
    await ctx.call('WorkerAPI', 'RespondQueryTaskCompleted', { taskToken: task.taskToken, result: { resultType: 'QUERY_RESULT_TYPE_ANSWERED', answer: payload(`pong-${label}`) } });
    const response = await answer;
    assert.equal((response.queryResult || response).data.toString(), `pong-${label}`, `${label}: query not answered`);
  };
  const execution = { workflowId: rootName, runId: started.runId };
  const query = { queryType: 'standby' };
  await answerOne('direct-child', () => ownerRpc(ctx, 'QueryWorkflow', { domainId: ctx.domainId, taskList: child,
    request: { domain: ctx.domain, workflowExecution: execution, query, queryConsistencyLevel: 'QUERY_CONSISTENCY_LEVEL_EVENTUAL' } }));
  // Go Frontend picks a partition by its own load balancer, so repeat to reach the child at least once.
  const viaFrontend = 6;
  for (let i = 0; i < viaFrontend; i++) {
    await answerOne(`frontend-${i}`, () => ctx.call('WorkflowAPI', 'QueryWorkflow', { domain: ctx.domain, workflowExecution: execution, query, queryConsistencyLevel: 'QUERY_CONSISTENCY_LEVEL_EVENTUAL' }));
  }
  return { result: 'passed', rootPartition: root.name, childPartition: child.name, directChildAnswered: true, frontendAnswered: viaFrontend };
}

// CADENCE_PROBE_LIST_PARTITIONS: Go engine.go getAllPartitions returns [root, /__cadence_sys/<root>/i for i in 1..n-1]
// with n = matching.numTasklistWritePartitions (the harness sets 3), for both task list types, each with
// OwnerHostName = ring owner of that partition key. Every Rust node and the Go Frontend must agree.
async function listPartitions(ctx) {
  const n = 3;
  const results = [];
  // These isolated-database fixture names match the harness's filtered config.
  for (const listName of ['probe-list-partitions-root', partitionName('probe-list-partitions-child', 2)]) {
    const rootName = listName.startsWith('/__cadence_sys/') ? listName.split('/')[2] : listName;
    const expectedKeys = Array.from({ length: n }, (_, i) => partitionName(rootName, i));
    const views = [];
    for (const node of ctx.directClients) {
      const response = await new Promise((resolve, reject) => node.client.ListTaskListPartitions({ domain: ctx.domain, taskList: { name: listName } },
        new ctx.grpc.Metadata(), { deadline: Date.now() + 10000 }, (error, value) => (error ? reject(error) : resolve(value))));
      views.push({ source: node.tchannel, response });
    }
    views.push({ source: 'go-frontend', response: await ctx.call('WorkflowAPI', 'ListTaskListPartitions', { domain: ctx.domain, taskList: { name: listName } }) });
    const ringAddresses = ctx.directClients.map(node => node.tchannel);
    for (const { source, response } of views) {
      for (const field of ['decisionTaskListPartitions', 'activityTaskListPartitions']) {
        assert.deepEqual(response[field].map(p => p.key), expectedKeys, `${source} ${field} keys/order differ from Go getAllPartitions for ${listName}`);
        for (const partition of response[field]) assert(ringAddresses.includes(partition.ownerHostName), `${source} ${field} owner ${partition.ownerHostName} is not a ring member`);
      }
    }
    const [reference, ...rest] = views;
    for (const view of rest) assert.deepEqual(view.response, reference.response, `${view.source} answer differs from ${reference.source} for ${listName}`);
    // The reported owner must be the node that serves the partition: the others answer typed ABORTED.
    const describes = [];
    for (const partition of reference.response.decisionTaskListPartitions) {
      const servedBy = [];
      for (const node of ctx.directClients) {
        try {
          const described = await new Promise((resolve, reject) => node.client.DescribeTaskList({ domainId: ctx.domainId,
            request: { taskList: { name: partition.key }, taskListType: 'TASK_LIST_TYPE_DECISION', includeTaskListStatus: true } },
          new ctx.grpc.Metadata(), { deadline: Date.now() + 10000 }, (error, value) => (error ? reject(error) : resolve(value))));
          assert(described.taskListStatus, `DescribeTaskList on ${partition.key} returned no status`);
          servedBy.push(node.tchannel);
        } catch (error) {
          if (error.code !== ctx.grpc.status.ABORTED) throw error;
        }
      }
      assert.deepEqual(servedBy, [partition.ownerHostName], `DescribeTaskList for ${partition.key} must be served only by its ListTaskListPartitions owner`);
      describes.push({ key: partition.key, servedBy: servedBy[0] });
    }
    const frontendDescribe = await ctx.call('WorkflowAPI', 'DescribeTaskList', { domain: ctx.domain, taskList: { name: rootName }, taskListType: 'TASK_LIST_TYPE_DECISION', includeTaskListStatus: true });
    assert(frontendDescribe.taskListStatus, 'Go Frontend DescribeTaskList returned no status');
    results.push({ requested: listName, keys: expectedKeys, owners: reference.response.decisionTaskListPartitions.map(p => p.ownerHostName), describes, sources: views.map(v => v.source) });
  }
  return { result: 'passed', transport: process.env.CADENCE_MATCHING_TRANSPORT || 'grpc', nodes: ctx.directClients.length, lists: results };
}

// CADENCE_PROBE_ADAPTIVE_SCALER exercises the production registry/config/scaler path.
// The harness enables adaptive scaling only for this root decision task list and
// lowers its QPS threshold/update interval. Workflows are scheduled by Go
// Frontend/History, so Matching observes genuine production task-add traffic.
async function adaptiveScaler(ctx) {
  const taskList = 'production-adaptive-scaler-probe';
  const request = { ...ctx.startRequest, taskList: { name: taskList },
    workflowType: { name: 'main.EchoWorkflow' }, input: payload(JSON.stringify('adaptive-load')) };
  const initial = await ownerRpc(ctx, 'DescribeTaskList', { domainId: ctx.domainId,
    request: { taskList: { name: taskList }, taskListType: 'TASK_LIST_TYPE_DECISION', includeTaskListStatus: true } });
  const initialConfig = initial.partitionConfig || {};
  const initialVersion = BigInt(initialConfig.version || '0');
  const initialWrites = Object.keys(initialConfig.writePartitions || {}).length || 1;
  const load = 24;
  const loadExecutions = [];
  for (let index = 0; index < load; index++) {
    const workflowId = `${taskList}-${ctx.id}-${index}`;
    const started = await ctx.call('WorkflowAPI', 'StartWorkflowExecution', {
      ...request, workflowId, requestId: randomUUID(),
    });
    loadExecutions.push({ workflowId, runId: started.runId });
  }
  const deadline = Date.now() + 45000;
  let updated;
  while (Date.now() < deadline) {
    updated = await ownerRpc(ctx, 'DescribeTaskList', { domainId: ctx.domainId,
      request: { taskList: { name: taskList }, taskListType: 'TASK_LIST_TYPE_DECISION', includeTaskListStatus: true } });
    const config = updated.partitionConfig || {};
    if (Object.keys(config.writePartitions || {}).length > initialWrites
      && BigInt(config.version || '0') > initialVersion) break;
    await sleep(250);
  }
  const config = updated?.partitionConfig || {};
  const writeCount = Object.keys(config.writePartitions || {}).length;
  assert(writeCount > initialWrites, `adaptive scaler did not increase write partitions (${initialWrites} -> ${writeCount})`);
  assert(BigInt(config.version || '0') > initialVersion,
    `adaptive scaler did not advance partition config version (${initialVersion} -> ${config.version || '0'})`);
  // Go Matching getAllPartitions uses the filtered static numTasklistWritePartitions
  // setting for this queue (the default is one). Adaptive partition config is
  // persisted separately and is verified above; it does not change this listing.
  const listRequest = { domain: ctx.domain, taskList: { name: taskList } };
  const rustListings = [];
  for (const node of ctx.directClients) {
    const response = await new Promise((resolve, reject) => node.client.ListTaskListPartitions(listRequest,
      new ctx.grpc.Metadata(), { deadline: Date.now() + 10000 }, (error, value) => (error ? reject(error) : resolve(value))));
    rustListings.push({ source: node.tchannel, response });
  }
  const goListing = await ctx.call('WorkflowAPI', 'ListTaskListPartitions', listRequest);
  for (const { source, response } of rustListings) {
    assert.deepEqual(response.decisionTaskListPartitions.map(partition => partition.key), [taskList],
      `${source} Go-shaped listing must follow static matching.numTasklistWritePartitions independently of adaptive config`);
    assert.deepEqual(response, goListing, `${source} listing differs from Go Frontend`);
  }
  assert.deepEqual(goListing.decisionTaskListPartitions.map(partition => partition.key), [taskList],
    'Go Frontend listing must follow static matching.numTasklistWritePartitions, independently of adaptive config');

  // Use the existing Go SDK worker/client on the changed queue. Its normal
  // workflow and activity loop proves tasks scheduled after the update still
  // traverse Go Frontend/History and Rust Matching successfully.
  const sdk = spawnSync('go', ['run', '.'], { cwd: path.resolve(__dirname, 'go-sdk'),
    env: { ...process.env, CADENCE_MIXED_MATCHING_DOMAIN: ctx.domain,
      CADENCE_TCHANNEL_PEER: process.argv[6], CADENCE_MIXED_MATCHING_TASK_LIST: taskList,
      CADENCE_MIXED_MATCHING_LOAD_IDS: JSON.stringify(loadExecutions.map(execution => execution.workflowId)) },
    encoding: 'utf8', timeout: 180000 });
  fs.writeFileSync(path.join(ctx.work, 'adaptive-scaler-sdk.log'), `${sdk.stdout || ''}\n${sdk.stderr || ''}`);
  assert.equal(sdk.status, 0, `Go SDK did not complete work on the scaled task list: ${sdk.error || sdk.status}; see adaptive-scaler-sdk.log`);
  let completedLoad = 0;
  for (const workflowExecution of loadExecutions) {
    const history = await ctx.call('WorkflowAPI', 'GetWorkflowExecutionHistory', { domain: ctx.domain, workflowExecution });
    assert.equal(history.history.events.filter(event => event.workflowExecutionCompletedEventAttributes).length, 1,
      `adaptive load workflow ${workflowExecution.workflowId} did not complete exactly once`);
    completedLoad++;
  }
  assert.equal(completedLoad, load, 'not all 24 adaptive load workflows completed');
  return { result: 'passed', taskList, enableAdaptiveScaler: true, cacheBackedPartitionCount: true,
    scheduledLoad: load, completedLoad, initial: { version: initialVersion.toString(), writePartitions: initialWrites },
    updated: { version: String(config.version), writePartitions: writeCount,
      readPartitions: Object.keys(config.readPartitions || {}).length },
    staticListing: goListing.decisionTaskListPartitions.map(partition => partition.key),
    sdk: 'existing Go SDK worker and client completed workflow plus activity after scaler update' };
}

// CADENCE_PROBE_DURABLE_RESTART: backlog rows for a three-partition list survive a Rust Matching restart and
// every partition drains afterwards. ctx.restartRust() kills and restarts the primary Rust process.
async function durableRestart(ctx) {
  const rootName = `durable-restart-${ctx.id}`;
  const root = { name: rootName };
  await ownerRpc(ctx, 'UpdateTaskListPartitionConfig', { domainId: ctx.domainId, taskList: root, taskListType: 'TASK_LIST_TYPE_ACTIVITY',
    partitionConfig: { version: '999', numReadPartitions: 3, numWritePartitions: 3, readPartitions: { 0: {}, 1: {}, 2: {} }, writePartitions: { 0: {}, 1: {}, 2: {} } } });
  const queued = [];
  for (let index = 0; index < 3; index++) {
    const activityId = `durable-${index}`;
    const scheduled = await scheduleActivity(ctx, `durable-${index}`, activityId);
    const partition = partitionName(rootName, index);
    await ownerRpc(ctx, 'AddActivityTask', { domainId: ctx.domainId, sourceDomainId: ctx.domainId, workflowExecution: { workflowId: scheduled.workflowId, runId: scheduled.runId }, taskList: { name: partition }, scheduleId: scheduled.scheduleId });
    queued.push({ partition, activityId, rows: await waitForRows(ctx, partition, 1, `partition ${index} backlog`) });
  }
  const restart = await ctx.restartRust();
  const drained = [];
  // A cold partition forwards its poll to its parent after the 10ms local wait (Go
  // LocalPollWaitTime parity) while its own backlog is still loading, and a loaded child
  // may hand its task to a waiting parent poller, so which partition's task a given poll
  // returns is timing dependent in Go too. Assert every backlog task survives the restart
  // and is delivered exactly once across the three polls.
  const remaining = new Set(queued.map(entry => entry.activityId));
  for (const entry of queued) {
    const task = await ownerRpc(ctx, 'PollForActivityTask', { domainId: ctx.domainId, pollerId: randomUUID(), request: { taskList: { name: entry.partition }, identity: 'durable-restart-poller' } });
    assert(remaining.delete(task.activityId), `partition ${entry.partition} returned unexpected or duplicate task ${task.activityId} after restart`);
    await ctx.call('WorkerAPI', 'RespondActivityTaskCompleted', { taskToken: task.taskToken, result: payload('durable-ok') });
    drained.push({ partition: entry.partition, activityId: task.activityId });
  }
  assert.equal(remaining.size, 0, `backlog tasks not delivered after restart: ${[...remaining]}`);
  return { result: 'passed', rootPartition: rootName, queuedBeforeRestart: queued, restart, drainedAfterRestart: drained };
}

// CADENCE_PROBE_AUTO_CONFIG_HINT: the poll responses that Go Frontend forwards from Rust Matching carry autoConfigHint.
// The harness sets matching.enableClientAutoConfig=false for this flag, so enableAutoConfig must be false.
// The field is checked for presence on the message: with proto-loader defaults:true an unset message is null,
// while an unset bool would read as false and hide a missing hint.
function autoConfigHintOf(response, label) {
  assert(response.autoConfigHint, `${label} poll response has no autoConfigHint`);
  assert.equal(response.autoConfigHint.enableAutoConfig, false, `${label} poll response autoConfigHint.enableAutoConfig must be false`);
  return { enableAutoConfig: response.autoConfigHint.enableAutoConfig };
}

async function autoConfigHint(ctx) {
  const decisionList = { name: `auto-config-decision-${ctx.id}` };
  const activityList = { name: `auto-config-activity-${ctx.id}` };
  const started = await ctx.call('WorkflowAPI', 'StartWorkflowExecution', { ...ctx.startRequest, workflowId: `auto-config-${ctx.id}`, taskList: decisionList, requestId: randomUUID() });
  const decision = await ctx.call('WorkerAPI', 'PollForDecisionTask', { domain: ctx.domain, taskList: decisionList, identity: 'auto-config-decision-poller' });
  assert.equal(decision.workflowExecution.runId, started.runId, 'decision poll returned a different run');
  const decisionHint = autoConfigHintOf(decision, 'decision');
  await ctx.call('WorkerAPI', 'RespondDecisionTaskCompleted', { taskToken: decision.taskToken, identity: 'auto-config-decision-poller', decisions: [{ scheduleActivityTaskDecisionAttributes: {
    activityId: 'auto-config-activity', activityType: { name: 'Echo' }, taskList: activityList,
    scheduleToCloseTimeout: { seconds: '60' }, scheduleToStartTimeout: { seconds: '60' }, startToCloseTimeout: { seconds: '30' },
  } }] });
  const activity = await ctx.call('WorkerAPI', 'PollForActivityTask', { domain: ctx.domain, taskList: activityList, identity: 'auto-config-activity-poller' });
  assert.equal(activity.activityId, 'auto-config-activity', 'activity poll did not return the scheduled activity');
  const activityHint = autoConfigHintOf(activity, 'activity');
  await ctx.call('WorkerAPI', 'RespondActivityTaskCompleted', { taskToken: activity.taskToken, result: payload('auto-config-ok'), identity: 'auto-config-activity-poller' });
  return { result: 'passed', decisionHint, activityHint };
}

// CADENCE_PROBE_ACTIVE_OFFER: register a real Rust Matching decision poller first,
// then ask Go Frontend/History to create a workflow task on that list. Delivery
// with no task row proves the active offer reached the waiter instead of falling
// back to the durable queue.
async function activeOffer(ctx) {
  const taskList = { name: `active-offer-${ctx.id}` };
  const identity = `active-offer-poller-${ctx.id}`;
  const pendingPoll = ownerRpc(ctx, 'PollForDecisionTask', {
    domainId: ctx.domainId, pollerId: randomUUID(),
    request: { domain: ctx.domain, taskList, identity },
  });
  const deadline = Date.now() + 30000;
  let poller;
  do {
    const described = await ownerRpc(ctx, 'DescribeTaskList', {
      domainId: ctx.domainId,
      request: { taskList, taskListType: 'TASK_LIST_TYPE_DECISION' },
    });
    poller = described.pollers.find(entry => entry.identity === identity);
    if (!poller && Date.now() > deadline) throw new Error('active-offer decision poller was not registered');
    if (!poller) await sleep(20);
  } while (!poller);

  // An after-delivery row count alone cannot distinguish direct offer from a
  // row that was persisted and consumed quickly. Audit inserts for this exact
  // task list while Go History schedules the task.
  const suffix = ctx.id.replace(/[^a-z0-9]/gi, '').toLowerCase();
  const auditTable = `active_offer_audit_${suffix}`;
  const auditTrigger = `active_offer_insert_${suffix}`;
  const auditSql = sql => {
    const container = process.env.CADENCE_MYSQL_DOCKER_CONTAINER;
    const password = process.env.CADENCE_MYSQL_PASSWORD || '';
    const database = process.env.CADENCE_MYSQL_DATABASE || 'cadence';
    const command = container
      ? ['docker', ['exec', container, 'mysql', '--batch', '--skip-column-names', '-uroot', `-p${password}`, database, '-e', sql]]
      : ['mysql', ['--batch', '--skip-column-names', '--host', process.env.CADENCE_MYSQL_HOST || '127.0.0.1', '--port', process.env.CADENCE_MYSQL_PORT || '3306',
        '--user', process.env.CADENCE_MYSQL_USER || 'root', `--password=${password}`, database, '-e', sql]];
    const result = spawnSync(command[0], command[1], { encoding: 'utf8', timeout: 30000 });
    if (result.status !== 0) throw new Error(`active-offer MySQL audit failed: ${result.stderr || result.error}`);
    return result.stdout.trim();
  };
  auditSql(`CREATE TABLE ${auditTable} (task_id BIGINT NOT NULL) ENGINE=InnoDB`);
  try {
    auditSql(`CREATE TRIGGER ${auditTrigger} BEFORE INSERT ON tasks FOR EACH ROW INSERT INTO ${auditTable}(task_id) SELECT NEW.task_id WHERE NEW.domain_id=UNHEX('${ctx.domainId.replace(/-/g, '')}') AND NEW.task_list_name='${taskList.name}' AND NEW.task_type=0`);
    const workflowId = `active-offer-workflow-${ctx.id}`;
    const started = await ctx.call('WorkflowAPI', 'StartWorkflowExecution', {
      ...ctx.startRequest, workflowId, taskList, requestId: randomUUID(),
    });
    assert(started.runId, 'Go History did not return a workflow run id');
    const task = await pendingPoll;
    assert(task.taskToken?.length, 'Go History decision task was not delivered to the waiting poller');
    assert.equal(task.workflowExecution.workflowId, workflowId, 'poller received a different workflow task');
    assert.equal(task.workflowExecution.runId, started.runId, 'poller received a different workflow run');
    const persistedRows = ctx.durableTaskCount(ctx.domainId, taskList.name, 0, undefined);
    assert.equal(persistedRows.status, 0, `could not check active-offer task rows: ${persistedRows.stderr}`);
    assert.equal(Number(String(persistedRows.stdout).trim()), 0, 'active-offered task unexpectedly remains in the durable queue');
    const insertAttempts = Number(auditSql(`SELECT COUNT(*) FROM ${auditTable}`));
    assert.equal(insertAttempts, 0, 'active-offered task was inserted into the durable queue before delivery');
    await ctx.call('WorkerAPI', 'RespondDecisionTaskCompleted', { taskToken: task.taskToken, decisions: [] });
    return { result: 'passed', taskList: taskList.name, pollerIdentity: identity, workflowId, runId: started.runId,
      historyScheduledTaskDelivered: true, taskTokenDelivered: true, persistedTaskRows: 0, persistedTaskInsertAttempts: insertAttempts };
  } finally {
    auditSql(`DROP TRIGGER IF EXISTS ${auditTrigger}`);
    auditSql(`DROP TABLE IF EXISTS ${auditTable}`);
  }
}

async function runScenario(name, flag, fn, ctx, results) {
  if (process.env[flag] !== '1') { results[name] = 'not requested'; return; }
  results[name] = await fn(ctx);
  fs.writeFileSync(path.join(ctx.work, `${name}.json`), JSON.stringify(results[name], null, 2) + '\n');
}

async function runProbeScenarios(ctx) {
  const results = {};
  await runScenario('postgresGcDomain', 'CADENCE_PROBE_PG_GC_DOMAIN', postgresGcAndDomain, ctx, results);
  await runScenario('queryStandbyForwarding', 'CADENCE_PROBE_QUERY_STANDBY', queryStandby, ctx, results);
  await runScenario('listTaskListPartitions', 'CADENCE_PROBE_LIST_PARTITIONS', listPartitions, ctx, results);
  await runScenario('adaptiveScaler', 'CADENCE_PROBE_ADAPTIVE_SCALER', adaptiveScaler, ctx, results);
  await runScenario('durableRestartDrain', 'CADENCE_PROBE_DURABLE_RESTART', durableRestart, ctx, results);
  await runScenario('autoConfigHint', 'CADENCE_PROBE_AUTO_CONFIG_HINT', autoConfigHint, ctx, results);
  await runScenario('activeOffer', 'CADENCE_PROBE_ACTIVE_OFFER', activeOffer, ctx, results);
  return results;
}

module.exports = { runProbeScenarios, postgresQuery };
