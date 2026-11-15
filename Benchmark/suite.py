"""Actions-only smoke suite. Retain a small machine-readable result, never raw output in Git."""
import argparse
from collections import Counter
from datetime import datetime
import json
import os
from pathlib import Path
import subprocess
import time
from report import percentiles

ROOT=Path(__file__).resolve().parent
OUT=ROOT/'evidence/run'

def stamp(value):return datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()

def recovery_result(rows,fault,rate):
    restored=fault.get('restored_unix_seconds')
    if restored is None:return {'recovered':False,'reason':'fault not restored'}
    end=max((stamp(r['scheduled']) for r in rows),default=restored)
    # Five consecutive seconds of arrivals after restoration must all complete
    # correctly within the same 1s latency target. Recovery time is the END of
    # the first qualifying window, not just the first successful RPC.
    for offset in range(max(0,int(end-restored)-4)):
        begin=restored+offset
        window=[r for r in rows if begin<=stamp(r['scheduled'])<begin+5]
        if len(window)>=int(5*rate) and all(r['correct'] and r['completion_ms']<=1000 for r in window):
            return {'recovered':True,'seconds_after_restore':offset+5,'stable_window_seconds':5}
    return {'recovered':False,'seconds_after_restore':None,'reason':'no full healthy 5s window before arrivals ended'}

def summarize_stage(path,mode,rate,status):
    manifest=json.loads((path/'manifest.json').read_text()) if (path/'manifest.json').exists() else {}
    result={'mode':mode,'rate':rate,'exit_code':status,'manifest':manifest,'runs':{},
            'failures':{f.name:json.loads(f.read_text()) for f in path.glob('*-failure.json')}}
    if status and (path/'runner.log').exists():
        with (path/'runner.log').open('rb') as log:
            log.seek(max(0,log.seek(0,2)-3000))
            result['runner_tail']=log.read().decode(errors='replace')
    for file in sorted(path.glob('*-samples.jsonl')):
        key=file.name.removesuffix('-samples.jsonl')
        all_rows=[json.loads(x) for x in file.read_text().splitlines()]
        rows=[r for r in all_rows if r['phase']=='measure']
        errors=Counter(r.get('error','incorrect') for r in all_rows if not r['correct'])
        item={'measured':len(rows),'correct':sum(r['correct'] for r in rows),'errors':dict(errors),
              'warmup_correct':sum(r['correct'] for r in all_rows if r['phase']=='warmup')}
        if rows:
            begin=min(stamp(r['scheduled']) for r in rows);end=begin+30
            item['completions_per_second']=sum(r['correct'] and stamp(r['completed'])<=end for r in rows)/30
            item['successful_starts_per_second']=sum(r['start_ok'] and stamp(r['started'])+r['start_rpc_ms']/1000<=end for r in rows)/30
            item['outstanding_at_arrival_end']=sum(not r['correct'] or stamp(r['completed'])>end for r in rows)
            item['latency_ms']={field:percentiles([v for r in rows for v in (r.get(field) if isinstance(r.get(field),list) else [r.get(field)]) if v is not None]) for field in ('completion_ms','start_rpc_ms','activity_schedule_to_start_ms','decision_schedule_to_start_ms','generator_lag_ms')}
            item['generator_valid']=item['latency_ms']['generator_lag_ms']['p99']<=max(10,1000/rate) and not any(r['generator_dropped'] for r in all_rows)
            item['target_met']=len(rows)==int(rate*30) and not errors and item['generator_valid'] and item['latency_ms']['completion_ms']['p99']<=1000 and item['outstanding_at_arrival_end']<=max(2,.01*len(rows))
            f=path/(key+'-fault.json')
            if f.exists():
                item['fault']=json.loads(f.read_text());item['recovery']=recovery_result(rows,item['fault'],rate)
        resources=path/(key+'-resources.json')
        if resources.exists():
            data=json.loads(resources.read_text());item['resources']={k:v for k,v in data.items() if k not in ('timeline','mysql_samples')}
            item['resources']['rss_trend_note']='30s plus drain; diagnostic only, not a soak slope'
        result['runs'][key]=item
    # Keep one exact resolved deployment per arm; no container inventories/logs.
    result['resolved_compose']={arm:next(path.glob('*-'+arm+'-compose.yml')).read_text() for arm in ('go','rust') if list(path.glob('*-'+arm+'-compose.yml'))}
    return result

def main():
    p=argparse.ArgumentParser();p.add_argument('--service',choices=('matching','frontend','history','all'),required=True);a=p.parse_args()
    stages=[('fixed',5),('capacity',25),('capacity',100),('recovery',5)]
    summary={'service':a.service,'source_head':os.environ.get('SOURCE_REF'),
             'protocol':{'workload':'one-activity','warmup_seconds':10,'measurement_seconds':30,'repetitions':3,'payload_bytes':1024,'activity_ms':10,'capacity_rates':[5,25,100],'completion_p99_target_ms':1000,'error_target':0,'max_end_outstanding':'max(2, 1% of arrivals)','recovery':'3s MySQL TCP INPUT DROP, 10s after collector ready'},
             'stages':[],'limitations':['Smoke protocol, not 10min/30min production protocol.','Capacity is highest TESTED passing rate; a passing highest step is a lower bound.','Recovery API retries are included in latency; server retry counters unavailable.','Short RSS peaks cannot establish memory stability; this benchmark uses musl System, not glibc.']}
    began=time.monotonic()
    for mode,rate in stages:
        # Leave room for artifacts/cleanup within the 90-minute job, including builds.
        path=OUT/f'{mode}-{rate}';path.mkdir(parents=True,exist_ok=True)
        env=dict(os.environ,BENCHMARK_OUTPUT_DIR=str(path))
        cmd=['python3',str(ROOT/'run.py'),'--service',a.service,'--rate',str(rate),'--warmup','10','--duration','30','--repetitions','3']
        if mode=='recovery':cmd+=['--recovery']
        with (path/'runner.log').open('w') as log:
            try:status=subprocess.run(cmd,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=1200).returncode
            except subprocess.TimeoutExpired:status=124
        summary['stages'].append(summarize_stage(path,mode,rate,status))
        summary['elapsed_runtime_seconds']=time.monotonic()-began
        (OUT/'suite-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    # Capacity target misses are results. Fixed/recovery correctness failures fail CI.
    failed=any(s['exit_code'] for s in summary['stages'] if s['mode'] in ('fixed','recovery'))
    raise SystemExit(int(failed))

if __name__=='__main__':main()
