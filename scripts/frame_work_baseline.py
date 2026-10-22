#!/usr/bin/env python3
"""Isolated release-only frame evaluator measurement, distinct from worker capacity."""
import argparse
import hashlib
import json
import os
import pathlib
import platform
import subprocess


def main(target, test, marker, dataset):
    parser=argparse.ArgumentParser()
    parser.add_argument('output',type=pathlib.Path)
    args=parser.parse_args()
    args.output.mkdir(parents=True,exist_ok=False)
    environment=os.environ.copy()
    for key in list(environment):
        if key in ('RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','RUSTC_WRAPPER','RUSTC_WORKSPACE_WRAPPER','LLVM_PROFILE_FILE','CARGO_BUILD_RUSTFLAGS') or key.startswith('CARGO_PROFILE_RELEASE_'):
            del environment[key]
    environment['CARGO_TARGET_DIR']=str(pathlib.Path(target).resolve())
    command=['cargo','test','--release','--locked','--lib',test,'--','--ignored','--nocapture']
    samples=[]
    for repeat in range(3):
        result=subprocess.run(command,env=environment,capture_output=True,text=True)
        (args.output/f'run-{repeat}.log').write_text(result.stdout+result.stderr)
        result.check_returncode()
        rows=[json.loads(line.split(marker,1)[1]) for line in result.stderr.splitlines() if line.startswith(marker)]
        assert len(rows)==2,rows
        samples.append(rows)
    files=[pathlib.Path('Cargo.toml'),pathlib.Path('Cargo.lock'),*sorted(pathlib.Path('src').rglob('*.rs'))]
    binaries=[path for path in pathlib.Path(environment['CARGO_TARGET_DIR']).glob('release/deps/pgderive-*') if path.is_file() and os.access(path,os.X_OK)]
    evidence={'scope':'serial pure evaluator wall time; release test binary; no coverage or end-to-end capacity claim',
        'dataset':dataset,
        'order':'scan then sliding; three repeats, no statistical significance claim',
        'git_head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
        'dirty':bool(subprocess.check_output(['git','status','--porcelain'],text=True).strip()),
        'rustc':subprocess.check_output(['rustc','-Vv'],text=True),'platform':platform.platform(),'logical_cpus':os.cpu_count(),
        'command':command,'source_sha256':{str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in files},
        'binary_sha256':{str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in binaries},'samples':samples}
    (args.output/'summary.json').write_text(json.dumps(evidence,indent=2)+'\n')
    print(json.dumps(samples))

