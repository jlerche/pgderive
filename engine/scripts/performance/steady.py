"""Compare raw observations over declared uninterrupted qualification intervals."""
import gzip
import json

from common import distribution, growth_intervals
from request_metrics import object_requests


def read_work(selected):
    if len(selected)<2:return None
    changes=sum(row['changes'] for row in selected[1:])
    result={}
    for origin in ['foreground','scan']:
        result[origin]={}
        for key in ['requested_bytes','gets','fetched_bytes']:
            values=[row.get('cache',{}).get('io',{}).get(origin,{}).get(key) for row in selected]
            if any(type(value) is not int or value<0 for value in values):return None
            if any(a>b for a,b in zip(values,values[1:])):return None
            result[origin][key+'_per_change']=(values[-1]-values[0])/max(1,changes)
    return result


def compactions(events,begin,end):
    prior=None;witness=[]
    for row in events:
        if row['event']!='maintenance' or not begin<=row['_observed_at']<end:continue
        counts=row.get('merges')
        if (not isinstance(counts,list) or len(counts)!=64 or
            any(type(value) is not int or value<0 for value in counts)):return None
        if prior is None:
            prior=counts;continue
        if any(value<old for value,old in zip(counts,prior)):return None
        if sum(counts[16:])>sum(prior[16:]):
            witness.append({'time':row['time'],'at':row['_observed_at'],
                            'merges_at_least_65536_rows':sum(counts[16:])-sum(prior[16:])})
        prior=counts
    return witness


def interval(events, commits, visible, samples, requests, begin, end):
    selected = [row for row in events if row['event']=='published' and begin<=row['_observed_at']<end]
    resource = [row for row in samples if begin<=row['at']<end and row['activities']==['steady']]
    accepted = [row for row in commits if begin<=row['commit']<end]
    changes = sum(row['source_changes'] for row in accepted)
    emitted = sum(row['changes'] for row in selected)
    sink = [max(0,(row['observed']-row['commit'])*1000) for row in visible if begin<=row['commit']<end]
    phases = {}
    for key in ['prepare_including_object_io','publication']:
        values = [row.get('phases_us',{}).get(key) for row in selected]
        phases[key] = (sum(values)/max(1,emitted)
                       if values and all(type(value) is int and value>=0 for value in values) else None)
    cpu = ((resource[-1]['cpu_seconds']-resource[0]['cpu_seconds'])*1000 /
           max(1,resource[-1]['source_changes']-resource[0]['source_changes'])) if len(resource)>1 else None
    return {'interval':[begin,end],'observations':{'epochs':len(selected),'resources':len(resource),'markers':len(sink),'checkpoints':sum(row['event']=='maintenance' and begin<=row['_observed_at']<end for row in events)},
            'changes_per_second':changes/(end-begin),'source_changes':changes,'range_io_per_change':read_work(selected),
            'processing_ms':distribution([row['elapsed_ms'] for row in selected]),
            'sink_visibility_ms':distribution(sink),'phase_us_per_change':phases,
            'cpu_ms_per_change':cpu,'peak_rss_bytes':max((row['rss_bytes'] for row in resource),default=None),
            'source_rows':[row['source_rows'] for row in resource],
            'groups':[row['groups'] for row in resource],
            'max_pending_changes':max((row['pending_changes_after_marker'] for row in resource),default=None),
            'requests':requests,'request_classes_per_change':{key:requests['estimated_r2_classes'].get(key,0)/max(1,changes) for key in ['class_a','class_b']}}


def assess(directory, samples, args, started):
    duration=int(args.duration); width=30
    if args.warmup_seconds<width or args.warmup_seconds%width:
        raise ValueError('Warmup must be a positive multiple of the window width')
    ranges=growth_intervals(duration,width,args.warmup_seconds//width)
    events=[json.loads(line) for line in (directory/'events-0.jsonl').read_text().splitlines()]
    def records(name):
        with gzip.open(directory/name,'rt') as stream: return [json.loads(line) for line in stream]
    commits,visible=records('transactions.jsonl.gz'),records('observations.jsonl.gz')
    resources=[{**row,'at':started+row['seconds']} for row in samples]
    parts=[interval(events,commits,visible,resources,object_requests(directory,(started+a,started+b)),started+a,started+b) for a,b in ranges]
    issues=[]
    if any(not row['ack_safe'] for row in samples): issues.append('Unsafe source acknowledgement ordering')
    if any(row['worker_generation']!=0 for row in samples): issues.append('Uninterrupted qualification restarted the worker')
    for number,part in enumerate(parts):
        if any(part['observations'][key]<minimum for key,minimum in [('epochs',20),('resources',3),('markers',20),('checkpoints',3)]):
            issues.append(f'Interval {number} lacks observations')
        if abs(part['changes_per_second']/args.rate-1)>.05: issues.append(f'Interval {number} did not sustain the offered rate')
        if part['requests']['methods'].get('PUT',0)==0: issues.append(f'Interval {number} lacks checkpoint upload evidence')
        if part['requests']['transport_errors'] or part['requests']['unknown_request_body_sizes']:
            issues.append(f'Interval {number} has incomplete or failed storage attempts')
        if part['range_io_per_change'] is None:issues.append(f'Interval {number} lacks valid read-origin accounting')
    cycles=compactions(events,started,started+args.duration)
    if args.qualification=='growth' and (cycles is None or len({row['time'] for row in cycles})<2):
        issues.append('Fewer than two checkpoints witnessed completed merges of at least 65536 input rows')
    early,late=parts
    checks=[]
    def bound(name,before,after,ratio,absolute):
        limit=None if before is None else before*ratio+absolute
        passed=limit is not None and after is not None and after<=limit
        checks.append({'metric':name,'early':before,'late':after,'limit':limit,'passed':passed})
        if not passed: issues.append(name+' exceeded its growth bound or lacked data')
    def percentile(part,key,p): return part[key][p] if part[key] else None
    for key in ['processing_ms','sink_visibility_ms']:
        bound(key+'_p50',percentile(early,key,'p50'),percentile(late,key,'p50'),1.25,1)
        bound(key+'_p95',percentile(early,key,'p95'),percentile(late,key,'p95'),1.5,1)
    bound('cpu_ms_per_change',early['cpu_ms_per_change'],late['cpu_ms_per_change'],1.25,.0001)
    bound('max_pending_changes',early['max_pending_changes'],late['max_pending_changes'],1.25,1000)
    for key in ['prepare_including_object_io','publication']:
        bound(key+'_us_per_change',early['phase_us_per_change'][key],late['phase_us_per_change'][key],1.25,.1)
    bound('peak_rss_bytes',early['peak_rss_bytes'],late['peak_rss_bytes'],1.1,16*1024**2)
    for key in ['class_a','class_b']:
        bound(key+'_per_change',early['request_classes_per_change'][key],late['request_classes_per_change'][key],1.25,1/max(1,late['source_changes']))
    for field in ['attempts','request_body_bytes','response_body_bytes']:
        bound(field+'_per_change',early['requests'][field]/max(1,early['source_changes']),late['requests'][field]/max(1,late['source_changes']),1.5,1/max(1,late['source_changes']))
    first=early['source_rows'][-1] if early['source_rows'] else None
    last=late['source_rows'][-1] if late['source_rows'] else None
    growth=last/first if first and last is not None else None
    if args.qualification=='growth' and (growth is None or growth<2): issues.append('Source rows did not at least double between qualified intervals')
    if args.qualification=='churn' and first!=last: issues.append('Constant-state control changed source cardinality')
    return {'passed':not issues,'issues':issues,'qualification':args.qualification,'warmup_seconds':args.warmup_seconds,
            'source_row_growth':growth,'compaction_witnesses':cycles,'parts':parts,'checks':checks,
            'scope':'Uninterrupted wall-clock qualification against local PostgreSQL and latency-injected object storage. Marker freshness is sampled; exact final native equality and WAL fence drain are checked separately. Finite covered range, not arbitrary-size proof.'}
