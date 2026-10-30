'use strict';

const assert = require('node:assert/strict');
const { createTaskObserver } = require('./matching-task-observer.cjs');

async function run() {
  const cassandraCalls = [];
  const sqliteCalls = [];
  const cassandraStates = [
    { queued_rows: 0, range_id: 10 },
    { queued_rows: 2, range_id: 11 },
    { queued_rows: 1, range_id: 11 },
    { queued_rows: 0, range_id: 12 },
    { queued_rows: 1, range_id: 13 },
    { queued_rows: 0, range_id: 14 },
  ];
  const cassandra = createTaskObserver({
    backend: 'cassandra',
    cassandraCommand(args, label) {
      cassandraCalls.push({ args, label });
      return cassandraStates.shift();
    },
    sqliteTaskState() {
      sqliteCalls.push(true);
      throw new Error('Cassandra observation fell through to SQLite');
    },
    wait: async () => {},
  });
  assert.deepEqual(await cassandra.observe('domain', 'queue', 5, 'before-kill'),
    { queued_rows: 2, range_id: 11 });
  assert.deepEqual(await cassandra.waitEmpty('domain', 'queue', 5, 'after-recovery'),
    { queued_rows: 0, range_id: 12 });
  assert.deepEqual(await cassandra.observe('domain', 'drain-queue', 5, 'before-sigterm'),
    { queued_rows: 1, range_id: 13 });
  assert.deepEqual(await cassandra.waitEmpty('domain', 'drain-queue', 5, 'after-sigterm'),
    { queued_rows: 0, range_id: 14 });
  assert.deepEqual(cassandraCalls.map(call => call.args[0]),
    ['observe-task', 'observe-task', 'wait-empty', 'wait-empty', 'observe-task', 'wait-empty']);
  assert.deepEqual(cassandraCalls.map(call => call.label),
    ['before-kill', 'before-kill', 'after-recovery', 'after-recovery', 'before-sigterm', 'after-sigterm']);
  assert.deepEqual(cassandraCalls.map(call => call.args[4]), ['0', '0', '0', '0', '0', '0'],
    'decision task observations must retain the default Go persistence type');
  assert.equal(sqliteCalls.length, 0);

  const activityCassandra = createTaskObserver({
    backend: 'cassandra',
    cassandraCommand(args) {
      cassandraCalls.push({ args });
      return { queued_rows: 1, range_id: 15 };
    },
    sqliteTaskState() { throw new Error('Cassandra observation fell through to SQLite'); },
    wait: async () => {},
  });
  await activityCassandra.observe('domain', 'activity-queue', 5, 'activity-before-crash', 1);
  assert.deepEqual(cassandraCalls.at(-1).args,
    ['observe-task', 'domain', 'activity-queue', '5', '1'],
    'activity observations must use Go persistence.TaskListTypeActivity (1)');

  const sqliteStates = [
    { queued_rows: 0, range_id: 20 },
    { queued_rows: 1, range_id: 21 },
    { queued_rows: 1, range_id: 21 },
    { queued_rows: 0, range_id: 22 },
    { queued_rows: 1, range_id: 23 },
    { queued_rows: 0, range_id: 24 },
  ];
  const sqliteTaskTypes = [];
  const sqlite = createTaskObserver({
    backend: 'sqlite',
    cassandraCommand() {
      throw new Error('SQLite observation fell through to Cassandra');
    },
    sqliteTaskState(_domain, _taskList, _label, taskType) {
      sqliteTaskTypes.push(taskType);
      return sqliteStates.shift();
    },
    wait: async () => {},
  });
  assert.deepEqual(await sqlite.observe('domain', 'queue', 5, 'before-kill'),
    { queued_rows: 1, range_id: 21 });
  assert.deepEqual(await sqlite.waitEmpty('domain', 'queue', 5, 'after-recovery'),
    { queued_rows: 0, range_id: 22 });
  assert.deepEqual(await sqlite.observe('domain', 'drain-queue', 5, 'before-sigterm'),
    { queued_rows: 1, range_id: 23 });
  assert.deepEqual(await sqlite.waitEmpty('domain', 'drain-queue', 5, 'after-sigterm'),
    { queued_rows: 0, range_id: 24 });
  assert.deepEqual(sqliteTaskTypes, [0, 0, 0, 0, 0, 0],
    'decision task observations must reach SQL backends with the default task type');
  let sqliteActivityType;
  const sqliteActivity = createTaskObserver({
    backend: 'sqlite',
    cassandraCommand() { throw new Error('SQL observation fell through to Cassandra'); },
    sqliteTaskState(_domain, _taskList, _label, taskType) {
      sqliteActivityType = taskType;
      return { queued_rows: 1, range_id: 25 };
    },
    wait: async () => {},
  });
  await sqliteActivity.observe('domain', 'activity-queue', 5, 'activity-before-crash', 1);
  assert.equal(sqliteActivityType, 1,
    'activity observations must reach SQL backends with Go persistence task type 1');

  console.log('task observer backend dispatch passed (injected Cassandra and SQLite states)');
}

run().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
