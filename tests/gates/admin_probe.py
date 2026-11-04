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
import importlib, json, subprocess, sys
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

for r in results:
    print(json.dumps(r))
sys.exit(0 if all(r['ok'] for r in results) else 1)
