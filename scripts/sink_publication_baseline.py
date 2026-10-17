#!/usr/bin/env python3
"""Build pinned-before/current release workers, then compare serial local publication."""
import argparse
import hashlib
import json
import os
import pathlib
import platform
import shutil
import subprocess
import sys
import tarfile
import tempfile


def build(source,output,environment):
    with (output/'build.log').open('w') as log:
        subprocess.run(['cargo','build','--release','--locked','--bin','pgderive_worker','--bin','pgderive'],cwd=source,env=environment,stdout=log,stderr=log,check=True)
    result={}
    for name in ('pgderive_worker','pgderive'):
        target=output/name
        shutil.copy2(pathlib.Path(environment['CARGO_TARGET_DIR'])/'release'/name,target)
        result[name]=hashlib.sha256(target.read_bytes()).hexdigest()
    return result


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('output',type=pathlib.Path)
    parser.add_argument('--before-ref',default='6d52093')
    args=parser.parse_args()
    root=pathlib.Path.cwd(); output=args.output.resolve(); output.mkdir(parents=True,exist_ok=False)
    environment=os.environ.copy()
    for key in list(environment):
        if key in ('RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','RUSTC_WRAPPER','RUSTC_WORKSPACE_WRAPPER','LLVM_PROFILE_FILE','CARGO_BUILD_RUSTFLAGS') or key.startswith('CARGO_PROFILE_RELEASE_'):
            del environment[key]
    build_root=root/'target/sink-publication-baseline'
    before=subprocess.check_output(['git','rev-parse',args.before_ref],text=True).strip()
    manifest={'before_ref':before,'head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
              'dirty':bool(subprocess.check_output(['git','status','--porcelain'],text=True).strip()),
              'rustc':subprocess.check_output(['rustc','-Vv'],text=True),'platform':platform.platform(),'logical_cpus':os.cpu_count(),
              'profile':'isolated locked release builds; inherited flags, wrappers, coverage and release overrides cleared'}
    with tempfile.TemporaryDirectory(prefix='pgderive-sink-before-') as directory:
        snapshot=pathlib.Path(directory)
        with (output/'before.tar').open('wb') as archive:
            subprocess.run(['git','archive',before],stdout=archive,check=True)
        with tarfile.open(output/'before.tar') as archive: archive.extractall(snapshot,filter='data')
        for label,source in [('before',snapshot),('after',root)]:
            destination=output/label; destination.mkdir()
            environment['CARGO_TARGET_DIR']=str(build_root/label)
            manifest[label]=build(source,destination,environment)
    sources=[root/'Cargo.toml',root/'Cargo.lock',*sorted((root/'src').rglob('*.rs')),*sorted((root/'scripts').glob('*sink*.py'))]
    manifest['source_sha256']={str(path.relative_to(root)):hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}
    (output/'build.json').write_text(json.dumps(manifest,indent=2)+'\n')
    live=output/'live'; live.mkdir()
    environment['PGDERIVE_S3_LATENCY_SCALE']='0'
    subprocess.run([sys.executable,'scripts/run_with_s3_proxy.py',str(live),sys.executable,'scripts/sink_publication_measure.py',str(live),str(output/'before/pgderive_worker'),str(output/'after/pgderive_worker')],env=environment,check=True)
    summary={'build':manifest,'scope':'local serial before/after release samples; no SLO, throughput, or remote-provider claim',
             'cases':{label:json.loads((live/label/'measurement.json').read_text()) for label in ('before','after')}}
    (output/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps({label:case['ticks'] for label,case in summary['cases'].items()}))


if __name__=='__main__': main()
