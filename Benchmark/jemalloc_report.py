"""Generate jemalloc comparisons without changing historical report generation."""
import argparse
import json
from pathlib import Path
from revision_report import snapshot, report, visual

def main():
    p=argparse.ArgumentParser()
    for key in ('previous','current','attempts','output'):p.add_argument('--'+key,type=Path,required=True)
    a=p.parse_args();old=snapshot(a.previous);new=snapshot(a.current)
    text=report(old,new,json.loads(a.attempts.read_text()))
    text=text.replace('77dbaa379 versus 3c2bf3aaa','571fb2a08 versus 77dbaa379').replace('77dbaa379eaa88bc7b62b48fc3d711059e2dd93e','571fb2a0862bca59bb799f7baae9518d26c0e7b4').replace('3c2bf3aaabf160518afde04f75097d2dc2c374a6','77dbaa379eaa88bc7b62b48fc3d711059e2dd93e')
    start=text.index('No allocator feature is enabled:');end=text.index('\n',start)
    text=text[:start]+'New Alpine/musl binaries enable jemalloc-default in both launchers; previous binaries used musl System. The GNU-only glibc arena cap is not exercised here. Allocator and Matching writer changes are measured together.'+text[end:]
    text+='\n## Current Rust versus its paired Go baseline\n\n| Lane | Go / Rust p50 ms | Go / Rust p99 ms | Go / Rust CPU cores | Go / Rust memory MiB | Go / Rust max passing starts/s |\n|---|---:|---:|---:|---:|---:|\n'
    from revision_report import LABELS
    from compare_suite import fmt
    for lane,label in LABELS.items():
        g=new['paired_go'].get(lane);r=new['configurations'].get(label)
        if not g or not r:continue
        vals=[f"{fmt(g['latency'][k])} / {fmt(r['latency'][k])}" for k in ('p50','p99')]
        vals += [f"{fmt(g[k])} / {fmt(r[k])}" for k in ('cpu','memory')]
        vals += [f"{g['capacity'] or 'none'} / {r['capacity'] or 'none'}"]
        text+='| '+lane+' | '+' | '.join(vals)+' |\n'
    earlier=Path('Benchmark/evidence/updated-master-20261114')
    if earlier.exists():
        extra=report(snapshot(earlier),new,{})
        extra=extra[extra.index('## Fixed-load results'):extra.index('## Rust binary provenance')]
        text+='\n## Earlier 3c2bf3aaa → current 571fb2a08\n\n'+extra
    page=visual(old,new).replace('3c2bf3aaa','77dbaa379').replace('Updated 77dbaa379','Updated 571fb2a08').replace('<code>77dbaa379</code>.','<code>571fb2a08</code>.')
    page=page.replace('Historical benchmark builds use musl System, not glibc.','New build uses jemalloc on musl; previous build used musl System. GNU arena caps are not exercised.')
    a.output.parent.mkdir(parents=True,exist_ok=True)
    a.output.with_suffix('.md').write_text(text);a.output.with_suffix('.html').write_text(page)
if __name__=='__main__':main()
