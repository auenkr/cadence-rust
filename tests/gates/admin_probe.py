#!/usr/bin/env python3
"""Admin gRPC probe for ReapplyEvents / GetCrossClusterTasks through the Rust Frontend.

Usage: admin_probe.py <grpc-address> <domain> <idl-proto-dir> <work-dir>
Prints one JSON object per check: {"name", "ok", "detail"}; exit 0 when all ok.
Expected values mirror Go (go/service/frontend/adminHandler.go):
  GetCrossClusterTasks("")      -> INVALID_ARGUMENT "Cluster name is not set."
  GetCrossClusterTasks("c")     -> INTERNAL "The cross-cluster feature has been deprecated." (Go History quirk)
  ReapplyEvents validation     -> INVALID_ARGUMENT "Domain not set on request." /
                                  "Execution is not set on request." / "WorkflowId is not set on request."
  ReapplyEvents unknown domain -> NOT_FOUND
  ReapplyEvents bad batch      -> INTERNAL DeserializeBatchEvents (Go History, forwarded unchanged)
"""
import importlib, json, os, subprocess, sys, uuid
from pathlib import Path

addr, domain, idl, work = sys.argv[1], sys.argv[2], Path(sys.argv[3]), Path(sys.argv[4])
gen = work / 'admin-probe-gen'
gen.mkdir(parents=True, exist_ok=True)
protos = [str(p) for p in (idl / 'uber/cadence/admin/v1').glob('*.proto')] + \
         [str(p) for p in (idl / 'uber/cadence/api/v1').glob('*.proto')]
subprocess.run([sys.executable, '-m', 'grpc_tools.protoc', f'-I{idl}', f'--python_out={gen}', f'--grpc_python_out={gen}', *protos], check=True)
sys.path.insert(0, str(gen))
import grpc  # noqa: E402
svc = importlib.import_module('uber.cadence.admin.v1.service_pb2')
svc_grpc = importlib.import_module('uber.cadence.admin.v1.service_pb2_grpc')
api = importlib.import_module('uber.cadence.api.v1.common_pb2')

chan = grpc.insecure_channel(addr)
stub = svc_grpc.AdminAPIStub(chan)
md = (('rpc-caller', 'admin-probe'), ('rpc-service', 'cadence-frontend'), ('rpc-encoding', 'proto'))
results = []

def check(name, call, code, text=None):
    try:
        call(metadata=md, timeout=30)
        got, msg = 'OK', ''
    except grpc.RpcError as e:
        got, msg = e.code().name, e.details() or ''
    ok = got == code and (text is None or text in msg)
    results.append({'name': name, 'ok': ok, 'detail': f'want={code} {text or ""} got={got} {msg}'})

req = svc.GetCrossClusterTasksRequest
check('get-cross-cluster-tasks-empty-cluster', lambda **k: stub.GetCrossClusterTasks(req(), **k),
      'INVALID_ARGUMENT', 'Cluster name is not set.')
check('get-cross-cluster-tasks-deprecated', lambda **k: stub.GetCrossClusterTasks(req(target_cluster='cluster0'), **k),
      'INTERNAL', 'The cross-cluster feature has been deprecated')  # Go History returns a value-typed BadRequestError, which its gRPC mapper does not convert -> INTERNAL

blob = api.DataBlob(encoding_type=api.ENCODING_TYPE_JSON, data=b'[]')
rq = svc.ReapplyEventsRequest
exe = api.WorkflowExecution
check('reapply-events-no-domain', lambda **k: stub.ReapplyEvents(rq(workflow_execution=exe(workflow_id='w'), events=blob), **k),
      'INVALID_ARGUMENT', 'Domain not set on request.')
check('reapply-events-no-execution', lambda **k: stub.ReapplyEvents(rq(domain=domain, events=blob), **k),
      'INVALID_ARGUMENT', 'Execution is not set on request.')
check('reapply-events-no-workflow-id', lambda **k: stub.ReapplyEvents(rq(domain=domain, workflow_execution=exe(), events=blob), **k),
      'INVALID_ARGUMENT', 'WorkflowId is not set on request.')
check('reapply-events-unknown-domain', lambda **k: stub.ReapplyEvents(
    rq(domain='no-such-domain-probe', workflow_execution=exe(workflow_id='w'), events=blob), **k), 'NOT_FOUND')
check('reapply-events-unknown-workflow', lambda **k: stub.ReapplyEvents(
    rq(domain=domain, workflow_execution=exe(workflow_id='no-such-wf-probe'), events=blob), **k),
      'INTERNAL', 'DeserializeBatchEvents')  # Go History deserializes the batch as ThriftRW before the workflow lookup

rti = importlib.import_module('uber.cadence.admin.v1.replication_pb2').ReplicationTaskInfo
dq = svc.GetDLQReplicationMessagesRequest
check('get-dlq-replication-messages-empty', lambda **k: stub.GetDLQReplicationMessages(dq(), **k),
      'INVALID_ARGUMENT', 'Replication task info is not set.')
# Go hydrator returns nil task (no error) when the workflow does not exist
check('get-dlq-replication-messages-unknown-workflow', lambda **k: stub.GetDLQReplicationMessages(
    dq(task_infos=[rti(domain_id='22222222-2222-2222-2222-222222222222', workflow_execution=exe(workflow_id='w', run_id='11111111-1111-1111-1111-111111111111'),
                       task_type=0, task_id=1, version=1, first_event_id=1, next_event_id=2)]), **k), 'OK')
# Go History logs conversion errors per workflow and returns an empty successful
# response (handler/handler.go GetDLQReplicationMessages), rather than forwarding
# the engine's unsupported-task-type error to Frontend.
check('get-dlq-replication-messages-bad-type', lambda **k: stub.GetDLQReplicationMessages(
    dq(task_infos=[rti(domain_id='d', workflow_execution=exe(workflow_id='w', run_id='r'), task_type=9)]), **k),
      'OK')

delete_req = svc.DeleteWorkflowRequest
maintain_req = svc.MaintainCorruptWorkflowRequest
check('delete-workflow-no-execution', lambda **k: stub.DeleteWorkflow(delete_req(domain=domain), **k),
      'INVALID_ARGUMENT', 'Request is nil.')
check('delete-workflow-unknown-domain', lambda **k: stub.DeleteWorkflow(
    delete_req(domain='no-such-domain-probe', workflow_execution=exe(workflow_id='w')), **k), 'NOT_FOUND')
check('maintain-corrupt-no-execution', lambda **k: stub.MaintainCorruptWorkflow(maintain_req(domain=domain), **k),
      'UNKNOWN', 'Execution is missing')

try:
    from google.protobuf.duration_pb2 import Duration
    tasklist = importlib.import_module('uber.cadence.api.v1.tasklist_pb2')
    workflow = importlib.import_module('uber.cadence.api.v1.service_workflow_pb2')
    workflow_grpc = importlib.import_module('uber.cadence.api.v1.service_workflow_pb2_grpc')
    workflow_stub = workflow_grpc.WorkflowAPIStub(chan)
    workflow_id = 'admin-delete-probe-' + str(uuid.uuid4())
    started = workflow_stub.StartWorkflowExecution(workflow.StartWorkflowExecutionRequest(
        domain=domain, workflow_id=workflow_id, request_id=str(uuid.uuid4()),
        workflow_type=api.WorkflowType(name='AdminDeleteProbe'),
        task_list=tasklist.TaskList(name='admin-delete-probe'),
        execution_start_to_close_timeout=Duration(seconds=600),
        task_start_to_close_timeout=Duration(seconds=10),
    ), metadata=md, timeout=30)
    execution = exe(workflow_id=workflow_id, run_id=started.run_id)
    maintained = stub.MaintainCorruptWorkflow(
        maintain_req(domain=domain, workflow_execution=execution), metadata=md, timeout=30)
    results.append({'name': 'maintain-healthy-workflow', 'ok': not maintained.ListFields(),
                    'detail': str(maintained)})
    deleted = stub.DeleteWorkflow(delete_req(domain=domain, workflow_execution=execution),
                                  metadata=md, timeout=30)
    results.append({'name': 'delete-persisted-workflow',
                    'ok': deleted.history_deleted and deleted.executions_deleted and deleted.visibility_deleted,
                    'detail': str(deleted)})
    check('deleted-workflow-mutable-state-missing', lambda **k: stub.DescribeWorkflowExecution(
        svc.DescribeWorkflowExecutionRequest(domain=domain, workflow_execution=execution), **k), 'NOT_FOUND')
except Exception as e:
    results.append({'name': 'delete-persisted-workflow', 'ok': False, 'detail': repr(e)})

# Corrupt mutable state: remove a middle history_node batch row directly in the database
# (Go's "eventID is not continuous" corruption), then MaintainCorruptWorkflow must delete the
# run like Go. Needs DB access via the harness env; Cassandra lanes are skipped (see below).
def db_exec(stmt):
    plugin = os.environ.get('ADMIN_PROBE_DB', '')
    prefix = 'CADENCE_' + plugin.upper()
    container = os.environ.get(prefix + '_DOCKER_CONTAINER')
    if plugin == 'mysql':
        cmd = ['docker', 'exec', container, 'mysql', '-uroot', '-p' + os.environ.get(prefix + '_PASSWORD', 'cadence'),
               '-h127.0.0.1', '-N', '-B', '-e', stmt]
    elif plugin == 'postgres':
        cmd = ['docker', 'exec', container, 'psql', '-U', os.environ.get(prefix + '_USER', 'postgres'), '-d', 'cadence', '-At', '-c', stmt]
    else:
        raise RuntimeError('no SQL database')
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if r.returncode != 0:
        raise RuntimeError(r.stdout + r.stderr)
    return r.stdout.strip()

if os.environ.get('ADMIN_PROBE_DB') in ('mysql', 'postgres'):
    try:
        from google.protobuf.duration_pb2 import Duration
        tasklist = importlib.import_module('uber.cadence.api.v1.tasklist_pb2')
        workflow = importlib.import_module('uber.cadence.api.v1.service_workflow_pb2')
        workflow_grpc = importlib.import_module('uber.cadence.api.v1.service_workflow_pb2_grpc')
        workflow_stub = workflow_grpc.WorkflowAPIStub(chan)
        wid = 'admin-maintain-corrupt-' + str(uuid.uuid4())
        started = workflow_stub.StartWorkflowExecution(workflow.StartWorkflowExecutionRequest(
            domain=domain, workflow_id=wid, request_id=str(uuid.uuid4()),
            workflow_type=api.WorkflowType(name='AdminMaintainCorrupt'),
            task_list=tasklist.TaskList(name='admin-maintain-corrupt'),
            execution_start_to_close_timeout=Duration(seconds=600),
            task_start_to_close_timeout=Duration(seconds=10),
        ), metadata=md, timeout=30)
        execution = exe(workflow_id=wid, run_id=started.run_id)
        for n in range(2):  # no worker polls this task list: each signal is its own history batch (events 3, 4)
            workflow_stub.SignalWorkflowExecution(workflow.SignalWorkflowExecutionRequest(
                domain=domain, workflow_execution=execution, identity='admin-probe',
                request_id=str(uuid.uuid4()), signal_name='corrupt-' + str(n)), metadata=md, timeout=30)
        tree = "'" + started.run_id.replace('-', '') + "'"
        tree = f"UNHEX({tree})" if os.environ['ADMIN_PROBE_DB'] == 'mysql' else f"decode({tree},'hex')"
        table = 'cadence.history_node' if os.environ['ADMIN_PROBE_DB'] == 'mysql' else 'history_node'
        before = db_exec(f'SELECT COUNT(*) FROM {table} WHERE tree_id = {tree}')
        db_exec(f'DELETE FROM {table} WHERE tree_id = {tree} AND node_id = 3')
        after = db_exec(f'SELECT COUNT(*) FROM {table} WHERE tree_id = {tree}')
        results.append({'name': 'maintain-corrupt-inject', 'ok': before != after and int(after) > 0,
                        'detail': f'history_node rows before={before} after={after}'})
        for label, call in (
            ('describe', lambda: workflow_stub.DescribeWorkflowExecution(workflow.DescribeWorkflowExecutionRequest(
                domain=domain, workflow_execution=execution), metadata=md, timeout=30)),
            ('history', lambda: workflow_stub.GetWorkflowExecutionHistory(workflow.GetWorkflowExecutionHistoryRequest(
                domain=domain, workflow_execution=execution), metadata=md, timeout=30)),
        ):
            try:
                call(); got = 'OK'
            except grpc.RpcError as e:
                got = f'{e.code().name} {e.details()}'
            results.append({'name': 'maintain-corrupt-observed-' + label, 'ok': True, 'detail': got})
        maintained = stub.MaintainCorruptWorkflow(
            maintain_req(domain=domain, workflow_execution=execution), metadata=md, timeout=60)
        results.append({'name': 'maintain-corrupt-deletes-workflow',
                        'ok': maintained.history_deleted and maintained.executions_deleted and maintained.visibility_deleted,
                        'detail': str(maintained)})
        check('maintain-corrupt-workflow-gone', lambda **k: stub.DescribeWorkflowExecution(
            svc.DescribeWorkflowExecutionRequest(domain=domain, workflow_execution=execution), **k), 'NOT_FOUND')
        left = db_exec(f'SELECT COUNT(*) FROM {table} WHERE tree_id = {tree}')
        results.append({'name': 'maintain-corrupt-history-rows-removed', 'ok': left == '0', 'detail': f'rows left={left}'})
    except Exception as e:
        results.append({'name': 'maintain-corrupt-deletes-workflow', 'ok': False, 'detail': repr(e)})

for r in results:
    print(json.dumps(r))
sys.exit(0 if all(r['ok'] for r in results) else 1)
