"""CI-only, bounded MySQL TCP interruption; local socket metrics stay available."""
import argparse
import json
import signal
import subprocess
import time
from pathlib import Path

p=argparse.ArgumentParser()
p.add_argument('--ready',required=True)
p.add_argument('--output',required=True)
p.add_argument('--mysql-pid',required=True)
a=p.parse_args()
rule=['sudo','nsenter','-t',a.mysql_pid,'-n','iptables','--wait']
match=['INPUT','-p','tcp','--dport','3306','-j','DROP']
result={'kind':'mysql TCP INPUT DROP','planned_offset_seconds':10,'planned_duration_seconds':3}
installed=False

def stop(*_):
    raise SystemExit('fault injector interrupted')
signal.signal(signal.SIGTERM,stop)
signal.signal(signal.SIGINT,stop)
try:
    deadline=time.monotonic()+240
    while not Path(a.ready).exists():
        if time.monotonic()>deadline:raise RuntimeError('measurement marker timeout')
        time.sleep(.1)
    time.sleep(10)
    subprocess.run(rule+['-I',*match],check=True)
    installed=True
    result['blocked_unix_seconds']=time.time()
    time.sleep(3)
finally:
    if installed:
        subprocess.run(rule+['-D',*match],check=True)
        result['restored_unix_seconds']=time.time()
    Path(a.output).write_text(json.dumps(result,indent=2)+'\n')
