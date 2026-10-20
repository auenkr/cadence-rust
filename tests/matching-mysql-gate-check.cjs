// Offline self-check for the production MySQL gate.
//
// The gate decides pass/fail for the production mixed run, so its decision
// logic is exercised here against a fake database: a converged run must pass,
// and every missing guarantee must fail.
const assert = require('node:assert/strict');
const { observeProductionMySqlDomain, verifyProductionMySqlTaskGc, verifyTaskListBlobDecoder } = require('./matching-mysql-gate.cjs');

const fixtures = require('./fixtures/matching-blobs.json');
const taskListFixtures = fixtures.filter(entry => entry.kind === 'task_list');
const blobFor = ackLevel => {
  const fixture = taskListFixtures.find(entry => entry.data.ack_level === ackLevel);
  assert(fixture, `no task-list blob fixture with ack level ${ackLevel}`);
  return fixture.thrift_hex;
};
// Patch the persisted acknowledgement of a fixture blob so the gate can be
// exercised with acknowledgements the fixtures do not contain.
const blobWithAck = ackLevel => {
  const hex = blobFor(2);
  const marker = '0a000c';
  const start = hex.indexOf(marker);
  assert(start >= 0, 'the fixture blob must carry an acknowledgement field');
  const value = BigInt(ackLevel).toString(16).padStart(16, '0');
  return `${hex.slice(0, start + marker.length)}${value}${hex.slice(start + marker.length + 16)}`;
};

const DOMAIN_ID = '2f8b2c1d-4e5f-4a6b-8c9d-0e1f2a3b4c5d';
const DOMAIN_HEX = DOMAIN_ID.replace(/-/g, '').toLowerCase();
const TASK_LIST = 'gate-tasks';
const PENDING_LIST = 'gate-pending';
const RANGE_SIZE = 100000;

function fakeQuery(state) {
  return sql => {
    if (sql.includes('FROM task_lists')) {
      const name = /name='([^']+)'/.exec(sql)[1];
      return (state.taskLists.get(name) || []).map(row => [String(row.taskType), String(row.rangeId), row.hex, row.encoding]);
    }
    if (sql.includes('FROM tasks')) {
      const name = /task_list_name='([^']+)'/.exec(sql)[1];
      const type = /task_type=(\d+)/.exec(sql);
      const rows = state.tasks.get(name) || [];
      const filtered = type ? rows.filter(row => row.taskType === Number(type[1])) : rows;
      return [[String(filtered.length)]];
    }
    if (sql.includes('FROM domains')) {
      return state.domains.map(row => [row.name, row.hex, String(row.isGlobal), row.encoding, String(row.length)]);
    }
    if (sql.includes('FROM domain_metadata')) {
      return state.notificationVersions.map(version => [String(version)]);
    }
    throw new Error(`unexpected query: ${sql}`);
  };
}

function convergedState() {
  return {
    // The collector removed every acknowledged row; the control task that was
    // never acknowledged is still stored.
    tasks: new Map([[TASK_LIST, []], [PENDING_LIST, [{ taskType: 0 }]]]),
    taskLists: new Map([[TASK_LIST, [
      { taskType: 0, rangeId: 1, hex: blobFor(2), encoding: 'thriftrw' },
      { taskType: 1, rangeId: 1, hex: blobFor(1), encoding: 'thriftrw' },
    ]]]),
    domains: [{ name: 'gate-domain', hex: DOMAIN_HEX, isGlobal: 0, encoding: 'thriftrw', length: 512 }],
    notificationVersions: [3],
  };
}

const taskLists = [
  { label: 'decision', name: TASK_LIST, taskType: 0, observedMaxTaskId: 2 },
  { label: 'activity', name: TASK_LIST, taskType: 1, minAck: 1 },
];
const liveBlockProvider = async () => ({ startId: 1, endId: RANGE_SIZE });
const sleep = () => Promise.resolve();

async function expectFailure(label, state, expected) {
  let failed = false;
  try {
    await verifyProductionMySqlTaskGc({
      query: fakeQuery(state), domainId: DOMAIN_ID, taskLists, pendingListName: PENDING_LIST,
      liveBlockProvider, rangeSize: RANGE_SIZE, deadlineMs: 0, sleep,
    });
  } catch (error) {
    failed = true;
    assert.match(error.message, expected, `${label} failed with an unexpected message`);
  }
  assert(failed, `${label} must fail`);
}

async function main() {
  const fixtureCases = verifyTaskListBlobDecoder();
  assert(fixtureCases >= 1, 'the blob decoder self-check must cover at least one fixture');

  const evidence = await verifyProductionMySqlTaskGc({
    query: fakeQuery(convergedState()), domainId: DOMAIN_ID, taskLists, pendingListName: PENDING_LIST,
    liveBlockProvider, rangeSize: RANGE_SIZE, deadlineMs: 0, sleep,
  });
  assert.equal(evidence.pendingTaskRows, 1, 'a converged run must report the surviving control task');
  assert.equal(evidence.queues.decision.remainingTaskRows, 0, 'a converged run must report no remaining decision rows');
  assert.equal(evidence.queues.decision.ackLevel, 2, 'a converged run must report the persisted acknowledgement');
  assert.equal(evidence.queues.activity.ackLevel, 1, 'a converged run must report the persisted activity acknowledgement');
  assert.equal(evidence.snapshots.length, 1, 'a converged run must report its snapshot history');
  assert.equal(evidence.snapshots[0].converged, true, 'a converged run must report a consistent snapshot');

  const domain = observeProductionMySqlDomain({ query: fakeQuery(convergedState()), domain: 'gate-domain', domainId: DOMAIN_ID });
  assert.equal(domain.domainId, DOMAIN_HEX, 'the domain observation must report Go\'s domain row');
  assert.deepEqual(domain.notificationVersions, [3], 'the domain observation must report Go\'s notification version');

  // The acknowledgement was never persisted, so the collector cannot have run.
  const unpersisted = convergedState();
  unpersisted.taskLists.set(TASK_LIST, [
    { taskType: 0, rangeId: 1, hex: blobFor(-1), encoding: 'thriftrw' },
    { taskType: 1, rangeId: 1, hex: blobFor(-1), encoding: 'thriftrw' },
  ]);
  await expectFailure('an unpersisted acknowledgement', unpersisted, /did not converge/);

  // The collector removed the control task that was never acknowledged.
  const lostControl = convergedState();
  lostControl.tasks.set(PENDING_LIST, []);
  await expectFailure('a removed control task', lostControl, /never acknowledged must survive/);

  // The persisted range no longer matches the live task-list block.
  const wrongRange = convergedState();
  wrongRange.taskLists.set(TASK_LIST, [
    { taskType: 0, rangeId: 3, hex: blobFor(2), encoding: 'thriftrw' },
    { taskType: 1, rangeId: 3, hex: blobFor(1), encoding: 'thriftrw' },
  ]);
  await expectFailure('a mismatched range', wrongRange, /did not converge/);

  // The task-list row disappeared instead of being collected.
  const missingRow = convergedState();
  missingRow.taskLists.set(TASK_LIST, []);
  await expectFailure('a missing task-list row', missingRow, /did not converge/);

  // The acknowledgement is ahead of the removed task. A Matching queue may
  // skip task ids that were allocated but never completed, so this must pass.
  const aheadAck = convergedState();
  aheadAck.taskLists.set(TASK_LIST, [
    { taskType: 0, rangeId: 1, hex: blobWithAck(9), encoding: 'thriftrw' },
    { taskType: 1, rangeId: 1, hex: blobWithAck(9), encoding: 'thriftrw' },
  ]);
  const aheadEvidence = await verifyProductionMySqlTaskGc({
    query: fakeQuery(aheadAck), domainId: DOMAIN_ID, taskLists, pendingListName: PENDING_LIST,
    liveBlockProvider, rangeSize: RANGE_SIZE, deadlineMs: 0, sleep,
  });
  assert.equal(aheadEvidence.queues.decision.ackLevel, 9, 'an acknowledgement ahead of the removed task is allowed');

  console.log(`Matching MySQL gate self-check passed (${fixtureCases} blob fixtures, 4 failure cases, 1 allowed high acknowledgement)`);
}

main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
