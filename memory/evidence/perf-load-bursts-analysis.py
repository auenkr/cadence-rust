import re,sys,collections
def load(f,arm):
    d=collections.defaultdict(list)
    for l in open(f):
        if 'queuev2 loaded' not in l: continue
        if arm=='go':
            if 'transfer-queue' not in l: continue
            k=re.search(r'"shard-id":(\d+)',l).group(1)
            t=float(re.search(r'"t_ms":([\d.]+)',l).group(1)); n=int(re.search(r'"tasks":(\d+)',l).group(1)); hm=re.search(r'"has_more":(\w+)',l).group(1)=='true'; tr=0
        else:
            if 'transfer-queue' not in l: continue
            k=re.search(r'limiter=(\w+)',l).group(1)
            t=float(re.search(r't_ms=([\d.]+)',l).group(1)); n=int(re.search(r'tasks=(\d+)',l).group(1)); hm=re.search(r'has_more=(\w+)',l).group(1)=='true'; tr=int(re.search(r'trig=(\d+)',l).group(1))
        d[k].append((t,n,hm,tr))
    return d
for arm in ['go','rust']:
    d=load(sys.argv[1].replace('ARM',arm),arm)
    tot=0;nz=0;gap=0;hm=0;trg=collections.Counter();gtrg=collections.Counter();pt=0
    for k,v in d.items():
        v.sort(); prev=None
        for t,n,h,tr in v:
            tot+=1; nz+= n>0; hm+=h; trg[tr]+=1; pt+=n
            if n>0 and prev is not None and t-prev<5: gap+=1; gtrg[tr]+=1
            if n>0: prev=t
    print(arm,'loads',tot,'nonempty',nz,'tasks',pt,'has_more',hm,'gap<5ms(nonempty)',gap,'trig',dict(trg),'gap trig',dict(gtrg))
