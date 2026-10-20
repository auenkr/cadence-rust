// Production MySQL gate for the Go/Rust mixed Matching run.
//
// The gate verifies Matching's task garbage collection and its domain
// metadata against Go's real MySQL schema:
//
//   * every acknowledged task row of a drained task list is removed,
//   * a task that was never acknowledged survives the collector,
//   * the persisted acknowledgement covers the removed rows,
//   * the persisted range and the live task-list block describe the same
//     range, read as one stable snapshot,
//   * the cached domain metadata is the row Go's `domains` table holds.
//
// The queries and the live block provider are injected, so the decision
// logic is exercised offline.
const assert = require('node:assert/strict');
const { decodeTaskListBlob, verifyTaskListBlobDecoder } = require('./matching-thrift-blob.cjs');

function sqlText(value) { return String(value).replaceAll("'", "''"); }

// Read the task-list metadata rows Go's schema defines for one task list.
function readTaskListRows(query, domainHex, taskListName) {
  return query(
    `SELECT task_type, range_id, HEX(data), data_encoding FROM task_lists WHERE shard_id=54321 AND domain_id=UNHEX('${domainHex}') AND name='${sqlText(taskListName)}' ORDER BY task_type`
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

// Count the persisted task rows of one task list and task type.
function countTaskRows(query, domainHex, taskListName, taskType) {
  const type = taskType === undefined ? '' : ` AND task_type=${taskType}`;
  return Number(query(
    `SELECT COUNT(*) FROM tasks WHERE domain_id=UNHEX('${domainHex}') AND task_list_name='${sqlText(taskListName)}'${type}`)[0][0]);
}

// Bounded production GC verification against Go's real MySQL schema.
//
// Every task list is described by one expectation:
//
//   name              the task list name
//   taskType          0 for a decision queue, 1 for an activity queue
//   observedMaxTaskId the highest task id that was observed in Go's tasks
//                     table while the task was persisted and undelivered
//   minAck            the lowest acknowledgement the queue must reach
//   label             the name used in the evidence
//
// The acknowledgement of a Matching queue is the highest continuously
// completed task id, so it may skip task ids that were allocated but never
// used. The gate therefore requires the persisted acknowledgement to cover
// the observed watermark and never assumes an upper bound.
//
// The persisted range and the live task-list block are read as one snapshot.
// A lease renewal between the two reads moves the range by one, so the gate
// reads both again inside the bounded retry instead of tolerating a
// difference, and it records the temporal history of every snapshot.
async function verifyProductionMySqlTaskGc({ query, domainId, taskLists, pendingListName, liveBlockProvider, rangeSize, deadlineMs = 90000, sleep }) {
  const domainHex = domainId.replace(/-/g, '').toLowerCase();
  assert(Array.isArray(taskLists) && taskLists.length > 0, 'the gate needs at least one task list to verify');
  for (const expectation of taskLists) {
    assert(expectation.name && expectation.label, 'every gate expectation needs a task list name and a label');
    assert(expectation.observedMaxTaskId !== undefined || expectation.minAck !== undefined,
      `the gate expectation ${expectation.label} needs an observed watermark or a minimum acknowledgement`);
  }
  const deadline = Date.now() + deadlineMs;
  const snapshots = [];
  let observation = null;
  for (;;) {
    const state = {};
    for (const expectation of taskLists) {
      const rows = readTaskListRows(query, domainHex, expectation.name)
        .filter(row => row.taskType === expectation.taskType);
      const taskRows = countTaskRows(query, domainHex, expectation.name, expectation.taskType);
      const live = await liveBlockProvider(expectation.name, expectation.taskType);
      state[expectation.label] = { rows, taskRows, live };
    }
    const pendingTasks = countTaskRows(query, domainHex, pendingListName);
    const converged = Object.entries(state).every(([, entry]) => {
      const row = entry.rows[0];
      if (entry.taskRows !== 0) return false;
      if (!row) return false;
      if (row.ackLevel === null) return false;
      const liveRange = Math.floor((entry.live.startId - 1) / rangeSize) + 1;
      return liveRange === row.rangeId;
    });
    const acksCoverRemovedRows = Object.entries(state).every(([label, entry]) => {
      const row = entry.rows[0];
      if (!row || row.ackLevel === null) return false;
      const expectation = taskLists.find(candidate => candidate.label === label);
      if (expectation.observedMaxTaskId !== undefined) {
        return row.ackLevel >= expectation.observedMaxTaskId;
      }
      return row.ackLevel >= expectation.minAck;
    });
    snapshots.push({
      at: new Date().toISOString(),
      pendingTasks,
      converged,
      acksCoverRemovedRows,
      queues: Object.fromEntries(Object.entries(state).map(([label, entry]) => [label, {
        taskRows: entry.taskRows,
        ackLevel: entry.rows[0] ? entry.rows[0].ackLevel : null,
        persistedRange: entry.rows[0] ? entry.rows[0].rangeId : null,
        liveRange: Math.floor((entry.live.startId - 1) / rangeSize) + 1,
      }])),
    });
    observation = { state, pendingTasks, snapshots };
    if (converged && acksCoverRemovedRows) break;
    if (Date.now() > deadline) {
      throw new Error(`Matching task garbage collection did not converge: ${JSON.stringify(observation)}`);
    }
    await sleep(500);
  }
  assert(observation.pendingTasks >= 1, 'a task that was never acknowledged must survive task garbage collection');
  const evidence = { pendingTaskRows: observation.pendingTasks, snapshots: observation.snapshots, queues: {} };
  for (const expectation of taskLists) {
    const entry = observation.state[expectation.label];
    const row = entry.rows[0];
    assert(entry.taskRows === 0, `the acknowledged task rows of ${expectation.label} must be removed from Go's tasks table`);
    assert(row, `Go's task_lists table is missing the ${expectation.taskType} row for ${expectation.name}`);
    assert(row.dataEncoding === 'thriftrw', `unexpected task-list blob encoding ${row.dataEncoding}`);
    if (expectation.observedMaxTaskId !== undefined) {
      assert(row.ackLevel >= expectation.observedMaxTaskId,
        `the persisted ack level ${row.ackLevel} of ${expectation.label} does not cover removed task ${expectation.observedMaxTaskId}`);
    } else {
      assert(row.ackLevel >= expectation.minAck,
        `the persisted ack level ${row.ackLevel} of ${expectation.label} is below the required acknowledgement ${expectation.minAck}`);
    }
    assert(row.rangeId >= 1, `the persisted range id ${row.rangeId} of ${expectation.label} is not a leased range`);
    const liveRange = Math.floor((entry.live.startId - 1) / rangeSize) + 1;
    assert(liveRange === row.rangeId,
      `the persisted range ${row.rangeId} of ${expectation.label} does not match the live task-list block ${JSON.stringify(entry.live)}`);
    assert(entry.live.endId === liveRange * rangeSize,
      `the live task-list block ${JSON.stringify(entry.live)} of ${expectation.label} does not describe a complete range block`);
    evidence.queues[expectation.label] = {
      taskList: expectation.name,
      taskType: expectation.taskType,
      remainingTaskRows: entry.taskRows,
      ackLevel: row.ackLevel,
      observedMaxTaskId: expectation.observedMaxTaskId ?? null,
      minAck: expectation.minAck ?? null,
      persistedRange: row.rangeId,
      liveBlock: entry.live,
      dataEncoding: row.dataEncoding,
    };
  }
  evidence.rangeSize = rangeSize;
  return evidence;
}

// Go's own domain metadata tables, as the Matching domain cache reads them.
function observeProductionMySqlDomain({ query, domain, domainId }) {
  const domainHex = domainId.replace(/-/g, '').toLowerCase();
  const rows = query(`SELECT name, HEX(id), is_global, data_encoding, LENGTH(data) FROM domains WHERE name='${sqlText(domain)}'`);
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
