'use strict';

function pause(milliseconds) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, milliseconds);
}

function createTaskObserver({ backend, cassandraCommand, sqliteTaskState, mysqlTaskState, wait = pause }) {
  if (!['cassandra', 'sqlite', 'mysql'].includes(backend)) throw new Error('unsupported task observer backend: ' + backend);

  async function state(domainId, taskList, label, condition, timeoutSeconds) {
    const deadline = Date.now() + timeoutSeconds * 1000;
    let current;
    do {
      if (backend === 'cassandra') {
        current = await cassandraCommand([condition === 'queued' ? 'observe-task' : 'wait-empty', domainId, taskList, String(timeoutSeconds)], label);
      } else {
        const read = { sqlite: sqliteTaskState, mysql: mysqlTaskState }[backend];
        if (typeof read !== 'function') throw new Error('task observer backend callback is missing: ' + backend);
        current = await read(domainId, taskList, label);
      }
      if (condition === 'queued' ? current.queued_rows > 0 : current.queued_rows === 0) return current;
      await wait(250);
    } while (Date.now() < deadline);
    throw new Error('Matching task persistence did not reach ' + condition + ' state in ' + timeoutSeconds + 's: ' + JSON.stringify(current));
  }

  return {
    observe: (domainId, taskList, timeoutSeconds, label) =>
      state(domainId, taskList, label, 'queued', timeoutSeconds),
    waitEmpty: (domainId, taskList, timeoutSeconds, label) =>
      state(domainId, taskList, label, 'empty', timeoutSeconds),
  };
}

module.exports = { createTaskObserver };
