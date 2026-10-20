// Production MySQL gate for the Go/Rust mixed Matching run.
//
// The gate verifies Matching's task garbage collection and its domain
// metadata against Go's real MySQL schema:
//
//   * every acknowledged task row of the completed workflow is removed,
//   * a task that was never acknowledged survives the collector,
//   * the persisted acknowledgement covers the removed rows,
//   * the persisted range stays consistent with the live task-list block,
//   * the cached domain metadata is the row Go's `domains` table holds.
//
// The queries are injected so this gate can be exercised without a database.
const assert = require('node:assert/strict');
const { decodeTaskListBlob, verifyTaskListBlobDecoder } = require('./matching-thrift-blob.cjs');

// Read the task-list metadata rows Go's schema defines for one task list.
function readTaskListRows(query, domainHex, taskListName) {
  return query(
    `SELECT task_type, range_id, HEX(data), data_encoding FROM task_lists WHERE shard_id=54321 AND domain_id=UNHEX('${domainHex}') AND name='${String(taskListName).replaceAll("'", "''")}' ORDER BY task_type`
  ).map(([taskType, rangeId, hex, encoding]) => {
    const blob = decodeTaskListBlob(hex);
    return {
      taskType: Number(taskType),
      rangeId: Number(rangeId),
      dataEncoding: encoding,
      kind: blob.kind,
      ackLevel: blob.ackLevel,
      expiryNanos: blob.expiryNanos,
      lastUpdatedNanos: blob.lastUpdatedNanos,
      hasAdaptivePartitionConfig: blob.hasAdaptivePartitionConfig,
    };
  });
}

function countTaskRows(query, domainHex, taskListName) {
  return Number(query(
    `SELECT COUNT(*) FROM tasks WHERE domain_id=UNHEX('${domainHex}') AND task_list_name='${String(taskListName).replaceAll("'", "''")}'`)[0][0]);
}

// Bounded production GC verification against Go's real MySQL schema.
async function verifyProductionMySqlTaskGc({ query, domainId, taskListName, pendingListName, observedMaxTaskIds, liveBlocks, rangeSize, deadlineMs = 90000, sleep }) {
  const domainHex = domainId.replace(/-/g, '').toLowerCase();
  assert(observedMaxTaskIds.length > 0, "no Matching task ids were observed in Go's tasks table");
  for (const taskType of [0, 1]) {
    const observed = observedMaxTaskIds.find(([observedType]) => observedType === taskType);
    assert(observed, `no ${taskType === 0 ? 'decision' : 'activity'} task id was observed in Go's tasks table`);
  }
  const deadline = Date.now() + deadlineMs;
  let observation = null;
  for (;;) {
    const lists = readTaskListRows(query, domainHex, taskListName);
    const remainingTasks = countTaskRows(query, domainHex, taskListName);
    const pendingTasks = countTaskRows(query, domainHex, pendingListName);
    observation = { remainingTasks, pendingTasks, lists, observedMaxTaskIds };
    const ackCoversRemovedRows = observedMaxTaskIds.every(([taskType, observed]) => {
      const row = lists.find(entry => entry.taskType === taskType);
      return Boolean(row) && row.ackLevel !== null && row.ackLevel >= observed.maxTaskId;
    });
    if (remainingTasks === 0 && ackCoversRemovedRows) break;
    if (Date.now() > deadline) {
      throw new Error(`Matching task garbage collection did not converge for ${taskListName}: ${JSON.stringify(observation)}`);
    }
    await sleep(500);
  }
  assert.equal(observation.remainingTasks, 0, "acknowledged Matching tasks must be removed from Go's tasks table");
  assert(observation.pendingTasks >= 1, 'a task that was never acknowledged must survive task garbage collection');
  for (const [taskType, observed] of observedMaxTaskIds) {
    const row = observation.lists.find(entry => entry.taskType === taskType);
    assert(row, `Go's task_lists table is missing the ${taskType} row for ${taskListName}`);
    assert(row.dataEncoding === 'thriftrw', `unexpected task-list blob encoding ${row.dataEncoding}`);
    assert(row.ackLevel >= observed.maxTaskId, `persisted ack level ${row.ackLevel} does not cover removed task ${observed.maxTaskId}`);
    assert(row.ackLevel <= observed.maxTaskId + 1, `persisted ack level ${row.ackLevel} is ahead of the removed task ${observed.maxTaskId}`);
    assert(row.rangeId >= 1, `persisted range id ${row.rangeId} is not a leased range`);
    const liveBlock = liveBlocks[row.taskType];
    if (!liveBlock) continue;
    // A lease renewal may move the range by one between the persisted row and
    // the live task-list block, so both must describe the same range block.
    const liveRange = Math.floor((liveBlock.startId - 1) / rangeSize) + 1;
    assert(Math.abs(liveRange - row.rangeId) <= 1,
      `persisted range ${row.rangeId} does not match the live task-list block ${JSON.stringify(liveBlock)}`);
    assert(liveBlock.endId === liveRange * rangeSize,
      `the live task-list block ${JSON.stringify(liveBlock)} does not describe a complete range block`);
  }
  return {
    taskList: taskListName,
    remainingTaskRows: observation.remainingTasks,
    pendingTaskRows: observation.pendingTasks,
    taskLists: observation.lists,
    observedMaxTaskIds: observedMaxTaskIds.map(([taskType, observed]) => ({ taskType, ...observed })),
    liveBlocks,
    rangeSize,
  };
}

// Go's own domain metadata tables, as the Matching domain cache reads them.
function observeProductionMySqlDomain({ query, domain, domainId }) {
  const domainHex = domainId.replace(/-/g, '').toLowerCase();
  const rows = query(`SELECT name, HEX(id), is_global, data_encoding, LENGTH(data) FROM domains WHERE name='${String(domain).replaceAll("'", "''")}'`);
  const metadata = query('SELECT notification_version FROM domain_metadata');
  assert.equal(rows.length, 1, `Go's domains table must hold exactly one row for ${domain}`);
  const [name, id, isGlobal, encoding, length] = rows[0];
  assert.equal(name, domain, 'the cached domain row must carry the probed domain name');
  assert.equal(id.toLowerCase(), domainHex, 'the cached domain row must carry the UUID Go Frontend reported');
  assert.equal(encoding, 'thriftrw', `unexpected domain blob encoding ${encoding}`);
  assert(Number(length) > 0, "the cached domain row must carry Go's domain metadata payload");
  assert(metadata.length >= 1, "Go's domain_metadata table must hold its notification version row");
  const notificationVersions = metadata.map(row => Number(row[0]));
  assert(notificationVersions.every(version => version >= 1), 'Go domain notification versions must be positive');
  return {
    domain: name,
    domainId: id.toLowerCase(),
    isGlobal: Number(isGlobal),
    dataEncoding: encoding,
    dataLength: Number(length),
    notificationVersions,
  };
}

module.exports = { countTaskRows, observeProductionMySqlDomain, readTaskListRows, verifyProductionMySqlTaskGc, verifyTaskListBlobDecoder };
