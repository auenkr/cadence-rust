#!/usr/bin/env python3
"""Watch one benchmark directly and save compact results locally after CI ends."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile
from compact_evidence import export_compact

p=argparse.ArgumentParser()
p.add_argument('run_id',type=int)
p.add_argument('--service',choices=('history','frontend','matching'),required=True)
a=p.parse_args()
root=Path(__file__).resolve().parent
repository=root.parent
out=root/'evidence'/f'{a.service}-benchmark-{a.run_id}'
out.mkdir(parents=True,exist_ok=True)

def gh(*args):
    return subprocess.check_output(['gh',*map(str,args),'--repo','auenkr/cadence-rust'],cwd=repository,text=True)

def save(result):
    (out/'status.json').write_text(json.dumps(result,indent=2)+'\n')

result=json.loads(gh('run','view',a.run_id,'--json','status,conclusion,headSha,url'))
result['service']=a.service
save(result)
watched=subprocess.run(['gh','run','watch',str(a.run_id),'--repo','auenkr/cadence-rust','--interval','60'],cwd=repository)
result.update(json.loads(gh('run','view',a.run_id,'--json','status,conclusion,headSha,url')))
result['watch_exit_code']=watched.returncode
save(result)
if result['status']!='completed':raise SystemExit('CI watch ended before completion; status saved')
try:
    with tempfile.TemporaryDirectory(prefix='cadence-benchmark-evidence-') as tmp:
        gh('run','download',a.run_id,'--dir',tmp)
        export_compact(tmp,out)
except subprocess.CalledProcessError as error:
    result['artifact_error']=str(error)
save(result)
print(json.dumps(result,indent=2),flush=True)
raise SystemExit(0 if result['conclusion']=='success' and 'artifact_error' not in result else 1)
