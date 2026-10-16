#!/usr/bin/env python3
"""Build uninstrumented release workers and retain a local portfolio baseline."""
import argparse
import hashlib
import json
import math
import os
import pathlib
import subprocess
import sys


def percentile(values, fraction):
    return sorted(values)[max(0,math.ceil(len(values)*fraction)-1)] if values else None


def summarize(live):
    results = [json.loads(path.read_text()) for path in sorted(live.glob('*/result.json'))]
    rows = []
    for result in results:
        ticks = [tick for tick in result['ticks'] if 'phases_us' in tick]
        latencies = [tick['client_to_published_ms'] for tick in ticks]
        rows.append({'id':result['id'],'seed':result['seed'],'source_rows':result['source_rows'],
                     'initial_key_distribution':result['initial_key_distribution'],
                     'publication_samples':len(ticks),
                     'client_to_published_ms':{'p50':percentile(latencies,.5),'p95':percentile(latencies,.95),'max':max(latencies,default=None)},
                     'phase_p50_us':{phase:percentile([tick['phases_us'][phase] for tick in ticks],.5) for phase in ticks[0]['phases_us']} if ticks else {},
                     'publication_subphase_p50_us':{phase:percentile([tick['publication_us'][phase] for tick in ticks],.5) for phase in ticks[0]['publication_us']} if ticks else {},
                     'worker_cpu_seconds':sum(item['cpu_seconds'] for item in result['process_generations']),
                     'worker_peak_rss_bytes':max(item['peak_rss_bytes'] for item in result['process_generations']),
                     'initial_client_startup_ms':result['initial_client_startup_ms'],
                     'cold_client_startup_ms':[tick['client_startup_ms'] for tick in result['ticks'] if tick.get('restart')],
                     'object_requests':result['object_requests'],
                     'maintenance_samples':len(result['maintenance']),
                     'maintenance_p50_us':percentile([event['elapsed_us'] for event in result['maintenance']],.5),
                     'expected_insert_occurrences':sum(tick['expected_insert_occurrences'] for tick in ticks),
                     'expected_delete_occurrences':sum(tick['expected_delete_occurrences'] for tick in ticks)})
    return {'version':1,'scope':'local serial release baseline, not throughput capacity or a production SLO',
            'percentiles':'nearest rank; medians of subphases are not additive',
            'cpu':'sum of sampled process cumulative user+system CPU before termination, excludes PG/proxy and shutdown',
            'timing':'client invocation includes psql startup; oracle verification excluded; prepare includes object I/O; subphases nest inside publication',
            'cases':rows}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('output',type=pathlib.Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True,exist_ok=False)
    environment = os.environ.copy()
    for key in list(environment):
        if key in ('RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','RUSTC_WRAPPER','RUSTC_WORKSPACE_WRAPPER','LLVM_PROFILE_FILE','CARGO_BUILD_RUSTFLAGS') or key.startswith('CARGO_PROFILE_RELEASE_'):
            del environment[key]
    target = pathlib.Path('target/application-baseline').resolve()
    environment['CARGO_TARGET_DIR']=str(target)
    with (args.output/'build.log').open('w') as log:
        subprocess.run(['cargo','build','--release','--locked','--bin','pgderive_worker','--bin','pgderive'],env=environment,stdout=log,stderr=log,check=True)
    worker = target/'release/pgderive_worker'
    manifest = {'profile':'release','rustc':subprocess.check_output(['rustc','-Vv'],text=True),
                'binary_sha256':{path.name:hashlib.sha256(path.read_bytes()).hexdigest() for path in (worker,worker.with_name('pgderive'))},
                'isolation':'dedicated target; inherited Rust flags/wrappers/coverage/release overrides cleared'}
    (args.output/'build.json').write_text(json.dumps(manifest,indent=2)+'\n')
    live = args.output/'live'; live.mkdir()
    environment['PGDERIVE_PORTFOLIO_BUILD_PROFILE']='release'
    environment['PGDERIVE_S3_LATENCY_SCALE']='0'
    environment.setdefault('PGDERIVE_PORTFOLIO_ROUNDS','2')
    environment.setdefault('PGDERIVE_PORTFOLIO_ROWS','128')
    environment.setdefault('PGDERIVE_PORTFOLIO_ACCOUNTS','16')
    environment.setdefault('PGDERIVE_PORTFOLIO_TRANSACTIONS','20')
    subprocess.run([sys.executable,'scripts/run_with_s3_proxy.py',str(live),sys.executable,
                    'scripts/application_portfolio_harness.py',str(live),'--',str(worker)],env=environment,check=True)
    summary = summarize(live)
    (args.output/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps(summary,indent=2))


if __name__=='__main__':
    main()
