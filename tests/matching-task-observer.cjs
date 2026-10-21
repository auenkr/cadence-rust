'use strict';

function pause(milliseconds) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, milliseconds);
}

function createTaskObserver({ backend, cassandraCommand, sqliteTaskState, wait = pause }) {
  if (!['cassandra', 'sqlite'].includes(backend)) throw new Error('task observer backend must be cassandra or sqlite');

  async function state(domainId, taskList, label, condition, timeoutSeconds) {
    const deadline = Date.now() + timeoutSeconds * 1000;
    let current;
    do {
      current = backend === 'cassandra'
        ? cassandraCommand([condition === 'queued' ? 'observe-task' : 'wait-empty', domainId, taskList, String(timeoutSeconds)], label)
        : sqliteTaskState(domainId, taskList, label);
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
