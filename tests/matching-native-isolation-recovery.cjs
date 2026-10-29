// Mixed production probe: Go History owns the schedules; native Matching owns
// the isolated queue and its live worker-reported dispatch limiter.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const listName = 'native-concurrent-isolation-recovery';

async function runNativeIsolationRecovery({ call, domain, domainId, id, startRequest, payload, work }) {
  const taskList = { name: listName };
  const workflowId = `isolation-recovery-${id}`;
  const started = await call('WorkflowAPI', 'StartWorkflowExecution', {
    ...startRequest, workflowId, taskList, requestId: randomUUID(),
  });
  const decision = await call('WorkerAPI', 'PollForDecisionTask', { domain, taskList, identity: 'isolation-recovery-scheduler' });
  assert.equal(decision.workflowExecution.runId, started.runId);
  await call('WorkerAPI', 'RespondDecisionTaskCompleted', {
    taskToken: decision.taskToken, decisions: Array.from({ length: 8 }, (_, i) => ({
      scheduleActivityTaskDecisionAttributes: {
        activityId: `recovery-${i}`, activityType: { name: 'Echo' },
        taskList: { name: `native-recovery-unpolled-${id}` },
        scheduleToCloseTimeout: { seconds: '240' }, scheduleToStartTimeout: { seconds: '210' },
        startToCloseTimeout: { seconds: '30' },
      },
    })),
  });
  let schedules;
  const scheduleDeadline = Date.now() + 10000;
  do {
    const history = await call('WorkflowAPI', 'GetWorkflowExecutionHistory', { domain, workflowExecution: { workflowId, runId: started.runId } });
    schedules = history.history.events.filter(e => e.activityTaskScheduledEventAttributes);
    if (schedules.length === 8) break;
    await sleep(100);
  } while (Date.now() < scheduleDeadline);
  assert.equal(schedules.length, 8, 'Go History must schedule all activities');
  const expected = new Map(schedules.map(e => [e.activityTaskScheduledEventAttributes.activityId, e]));
  const groups = ['zone-a', 'zone-b'];
  const seen = new Set();
  const poll = (group, rate) => {
    const pollerId = randomUUID();
    const promise = call('Matching', 'PollForActivityTask', {
      domainId, pollerId, isolationGroup: group,
      request: { taskList, identity: `recovery-${group}-${pollerId}`, taskListMetadata: { maxTasksPerSecond: { value: rate } } },
    });
    return { group, pollerId, promise };
  };
  async function cancel(p) {
    await call('Matching', 'CancelOutstandingPoll', { domainId, taskList, taskListType: 'TASK_LIST_TYPE_ACTIVITY', pollerId: p.pollerId });
    return p.promise;
  }
  async function registered(count, rate) {
    const deadline = Date.now() + 10000;
    do {
      const info = await call('Matching', 'DescribeTaskList', { domainId, request: { taskList, taskListType: 'TASK_LIST_TYPE_ACTIVITY', includeTaskListStatus: true } });
      const metrics = info.taskListStatus.isolationGroupMetrics || {};
      if (groups.every(g => Number(metrics[g]?.pollerCount || 0) >= count) && (rate === undefined || Number(info.taskListStatus.ratePerSecond) === rate)) return;
      await sleep(100);
    } while (Date.now() < deadline);
    throw new Error(`isolated pollers/rate were not registered: count=${count} rate=${rate}`);
  }
  async function add(i) {
    const event = expected.get(`recovery-${i}`);
    await call('Matching', 'AddActivityTask', {
      domainId, sourceDomainId: domainId, workflowExecution: { workflowId, runId: started.runId },
      taskList, scheduleId: event.eventId, partitionConfig: { 'isolation-group': groups[i % 2] },
    });
  }
  async function accept(group, task) {
    assert(task.taskToken?.length, `empty poll for ${group}`);
    const match = /^recovery-(\d+)$/.exec(task.activityId);
    assert(match && expected.has(task.activityId), `unexpected activity ${task.activityId}`);
    assert.equal(groups[Number(match[1]) % 2], group, `activity ${task.activityId} crossed isolation groups`);
    assert.equal(task.workflowExecution.runId, started.runId);
    assert(!seen.has(task.activityId), `duplicate activity ${task.activityId}`);
    seen.add(task.activityId);
    await call('WorkerAPI', 'RespondActivityTaskCompleted', { taskToken: task.taskToken, result: payload('ok') });
  }

  // Pollers are registered before concurrently issuing the adds, so this also
  // exercises sync matching rather than merely draining a pre-existing queue.
  const initial = groups.flatMap(g => [poll(g, 100), poll(g, 100)]);
  await registered(2);
  await Promise.all([0, 1, 2, 3].map(add));
  await Promise.all(initial.map(async p => accept(p.group, await p.promise)));

  const zeroAt = Date.now();
  const zero = groups.map(g => poll(g, 0));
  await registered(1, 0);
  await Promise.all([4, 5, 6, 7].map(add));
  // A zero burst must leave every task undispatched, including sync matching.
  const early = await Promise.race([Promise.race(zero.map(p => p.promise)).then(() => true), sleep(2000).then(() => false)]);
  assert(!early, 'zero-rate pollers unexpectedly received work');
  for (const p of zero) {
    const response = await cancel(p);
    assert(!response.taskToken?.length && !response.activityId, 'zero dispatch delivered an activity');
  }

  // Positive reports cannot replace the minimum until the production 60s TTL.
  // Keep both groups polling, cancelling before the 20s RPC deadline.
  const recoveryDeadline = zeroAt + 95000;
  while (seen.size < 8 && Date.now() < recoveryDeadline) {
    const active = groups.map(g => poll(g, 100));
    const outcomes = active.map(p => p.promise.then(task => ({ p, task })));
    const completed = new Set();
    const until = Math.min(Date.now() + 10000, recoveryDeadline);
    while (completed.size < active.length && Date.now() < until) {
      const result = await Promise.race([...outcomes.filter((_, i) => !completed.has(i)), sleep(until - Date.now()).then(() => null)]);
      if (!result) break;
      completed.add(active.indexOf(result.p));
      if (result.task.taskToken?.length) await accept(result.p.group, result.task);
    }
    for (const [i, p] of active.entries()) {
      if (!completed.has(i)) {
        const task = await cancel(p);
        if (task.taskToken?.length) await accept(p.group, task);
      }
    }
  }
  assert.equal(seen.size, 8, 'all activities must recover without loss or duplicate delivery');
  assert(Date.now() - zeroAt >= 60000, 'positive report must not bypass zero-rate TTL');
  const history = await call('WorkflowAPI', 'GetWorkflowExecutionHistory', { domain, workflowExecution: { workflowId, runId: started.runId } });
  assert.equal(history.history.events.filter(e => e.activityTaskCompletedEventAttributes).length, 8);
  fs.writeFileSync(path.join(work, 'native-concurrent-isolation-recovery.json'), JSON.stringify({
    topology: 'configured Rust Matching with Go Frontend and History', taskList: listName,
    workflowId, delivered: [...seen], zeroRateHeldMs: Date.now() - zeroAt, activityCompletions: 8,
  }, null, 2) + '\n');
}
module.exports = { runNativeIsolationRecovery, listName };
