"""Direct CI watcher: retain compact summaries, retry failed lanes once, publish reports."""
import argparse
from collections import Counter
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import zipfile

ROOT=Path(__file__).resolve().parent
REPO='auenkr/cadence-rust'
BRANCH='t3/matching-benchmark-harness'
SOURCE='77dbaa379eaa88bc7b62b48fc3d711059e2dd93e'

def gh(*args):return subprocess.check_output(['gh',*map(str,args),'--repo',REPO],text=True)

def diagnose(summary,status,build_tails):
    reasons=[]
    if not summary:
        reasons.append('No suite summary: build/setup did not reach the workload suite. '+build_tails[-2500:])
    else:
        for s in summary['stages']:
            if s['mode']=='capacity':continue # capacity misses are benchmark outcomes
            if s['exit_code']:
                reasons.append(f"{s['mode']} exited {s['exit_code']}.")
                for key,r in s['runs'].items():
                    if r['errors']:reasons.append(f"{key}: measured={r['measured']}; warmup_correct={r['warmup_correct']}; errors={r['errors']}")
                if s.get('failures'):reasons.append('Harness failures: '+json.dumps(s['failures']))
                if s.get('runner_tail'):reasons.append(s['runner_tail'][-1500:])
        if not reasons:reasons.append('CI failed/cancelled outside fixed/recovery correctness; inspect CI steps. '+build_tails[-1500:])
    return ' '.join(reasons)[:5000]

def download(run_id,folder):
    artifacts=json.loads(gh('api',f'repos/{REPO}/actions/runs/{run_id}/artifacts'))['artifacts']
    summary=None;tails=''
    for a in artifacts:
        if a['expired']:continue
        if a['size_in_bytes']>150_000_000:raise RuntimeError('Artifact exceeds compact download budget')
        with tempfile.TemporaryDirectory(prefix='cadence-compact-revision-') as temp:
            archive=Path(temp)/'artifact.zip'
            with archive.open('wb') as output:
                subprocess.run(['gh','api',f'repos/{REPO}/actions/artifacts/{a["id"]}/zip'],stdout=output,check=True)
            with zipfile.ZipFile(archive) as z:
                for name in z.namelist():
                    if name.endswith('suite-summary.json'):
                        summary=json.loads(z.read(name))
                        (folder/'suite-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
                    elif name.endswith('-build.log'):
                        with z.open(name) as stream:
                            tail=b''
                            for block in iter(lambda:stream.read(65536),b''):tail=(tail+block)[-2000:]
                        tails+=name+': '+tail.decode(errors='replace')+'\n'
    return summary,tails

def main():
    p=argparse.ArgumentParser();p.add_argument('--state',type=Path,required=True);p.add_argument('--previous',type=Path,required=True);a=p.parse_args()
    state=json.loads(a.state.read_text());out=a.state.parent
    def save():a.state.write_text(json.dumps(state,indent=2)+'\n')
    for service,attempts in state.items():
        # The initial four lanes are already running in parallel. Each retry is
        # dispatched only after its own first result and diagnosis are recorded.
        for index in range(2):
            if index>=len(attempts):break
            attempt=attempts[index];run=attempt['run_id']
            if attempt.get('status')!='completed':
                try:subprocess.run(['gh','run','watch',str(run),'--repo',REPO,'--interval','60'],timeout=5700,check=False)
                except subprocess.TimeoutExpired:pass
                attempt.update(json.loads(gh('run','view',run,'--json','status,conclusion,headSha,url')));save()
            if attempt['status']!='completed':raise RuntimeError('Watch ended while CI is pending; resume this state later')
            folder=out/f'{service}-attempt-{index+1}';folder.mkdir(exist_ok=True)
            summary,tails=download(run,folder)
            if attempt['conclusion']!='success':
                attempt['diagnosis']=diagnose(summary,attempt,tails);save()
                print(service,'failed:',attempt['diagnosis'],flush=True)
            # Latest attempt is selected, even if it also failed; first attempt
            # and diagnosis remain preserved in the committed report.
            selected=out/service;selected.mkdir(exist_ok=True)
            if summary:(selected/'suite-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
            elif (selected/'suite-summary.json').exists():(selected/'suite-summary.json').unlink()
            if attempt['conclusion']=='success' or index==1:break
            if len(attempts)>1:continue
            if attempt.get('retry_dispatched'):
                raise RuntimeError('Retry already dispatched; recover its ID before resuming')
            known={r['databaseId'] for r in json.loads(gh('run','list','--workflow','frontend-matching-benchmark.yml','--branch',BRANCH,'--limit','30','--json','databaseId'))}
            gh('workflow','run','frontend-matching-benchmark.yml','--ref',BRANCH,'-f','sdk_openloop=true','-f','sdk_suite=true','-f',f'service={service}','-f','sdk_repetitions=3','-f',f'source_ref={SOURCE}')
            attempt['retry_dispatched']=True;save()
            time.sleep(60) # single indexing delay, not a polling loop
            new=[r for r in json.loads(gh('run','list','--workflow','frontend-matching-benchmark.yml','--branch',BRANCH,'--limit','30','--json','databaseId,displayTitle,headSha,url')) if r['databaseId'] not in known and r['displayTitle'].startswith(f'Benchmark {service} source {SOURCE}')]
            if len(new)!=1:raise RuntimeError('Retry dispatch ambiguous; state prevents duplicate dispatch')
            attempts.append({'run_id':new[0]['databaseId'],'url':new[0]['url'],'headSha':new[0]['headSha'],'status':'queued'});save()
    report=ROOT/'reports/final-master-20261115'
    subprocess.run([sys.executable,str(ROOT/'revision_report.py'),'--previous',str(a.previous),'--current',str(out),'--attempts',str(a.state),'--output',str(report)],check=True)
    subprocess.run(['git','add',str(report.with_suffix('.md')),str(report.with_suffix('.html'))],check=True)
    if subprocess.run(['git','diff','--cached','--quiet']).returncode:
        subprocess.run(['git','commit','-m','Report final-code benchmark versus 3c2bf3aaa, including retries and charts'],check=True)
        subprocess.run(['git','push','origin','HEAD'],check=True)
    print(report.with_suffix('.md').read_text(),flush=True)
    raise SystemExit(0 if all(v[-1]['conclusion']=='success' for v in state.values()) else 1)

if __name__=='__main__':main()
