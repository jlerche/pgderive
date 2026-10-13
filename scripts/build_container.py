#!/usr/bin/env python3
"""Build a bounded-input runtime image from the uninstrumented release package.

The executable and ELF closure are host-native. The manifest binds exact runtime
bytes; this does not promise bit-identical Rust builds across arbitrary hosts.
"""
import argparse
import datetime
import hashlib
import json
import os
import pathlib
import re
import shutil
import subprocess


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build(package, output, tag):
    package = package.resolve()
    output.mkdir(mode=0o700)
    manifest = json.loads((package/'manifest.json').read_text())
    assert not manifest['build']['instrumented']
    for name, expected in manifest['package_sha256'].items():
        assert digest(package/name) == expected, f'package drift: {name}'
    for name in ['deploy/Dockerfile','.dockerignore','scripts/build_container.py']:
        assert digest(pathlib.Path(name))==manifest['source_sha256'][name],f'packaged image definition drift: {name}'
    root = output/'rootfs'
    root.mkdir()
    binary = package/'bin/pgderive'
    executable = root/'usr/local/bin/pgderive'
    executable.parent.mkdir(parents=True)
    shutil.copyfile(binary, executable)
    executable.chmod(0o755)
    linked = subprocess.check_output(['ldd',str(binary)],text=True)
    (output/'runtime-libraries.log').write_text(linked)
    assert 'not found' not in linked
    libraries = sorted(set(re.findall(r'(/[^\s()]+)', linked)))
    assert libraries and any('ld-linux' in name for name in libraries)
    for name in libraries:
        source = pathlib.Path(name)
        destination = root/source.relative_to('/')
        destination.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(source,destination)
        destination.chmod(0o755)
    ca = root/'etc/ssl/certs/ca-certificates.crt'
    ca.parent.mkdir(parents=True)
    shutil.copyfile('/etc/ssl/certs/ca-certificates.crt',ca)
    for name, content in {
        'passwd':'pgderive:x:10001:10001:Pgderive:/nonexistent:/usr/sbin/nologin\n',
        'group':'pgderive:x:10001:\n',
        'nsswitch.conf':'hosts: files dns\n',
    }.items():
        (root/'etc'/name).write_text(content)
    (root/'var/lib/pgderive/scratch').mkdir(parents=True)
    (root/'var/lib/pgderive/scratch').chmod(0o700)
    shutil.copyfile('deploy/Dockerfile',output/'Dockerfile')
    shutil.copyfile('.dockerignore',output/'.dockerignore')
    epoch=int(subprocess.check_output(['git','show','-s','--format=%ct',manifest['git_head']],text=True).strip())
    for path in sorted(output.rglob('*')):
        os.utime(path,(epoch,epoch),follow_symlinks=False)
    hashes = {str(path.relative_to(output)):digest(path) for path in sorted(output.rglob('*')) if path.is_file()}
    build_environment={**os.environ,'DOCKER_BUILDKIT':'1'}
    image_ids=[]
    for ordinal in range(2):
        iid=output/f'image-{ordinal}.id'
        with (output/f'build-{ordinal}.log').open('w') as log:
            subprocess.run(['docker','build','--network=none','--pull=false','--no-cache',
                '--build-arg',f'SOURCE_DATE_EPOCH={epoch}','--iidfile',str(iid),
                '--tag',tag if ordinal==0 else tag+'-reproducibility',str(output)],
                env=build_environment,check=True,stdout=log,stderr=log)
        image_ids.append(iid.read_text().strip())
    assert image_ids[0]==image_ids[1], f'clean image builds differ: {image_ids}'
    for name, expected in hashes.items():
        assert digest(output/name)==expected, f'image input drift: {name}'
    image = json.loads(subprocess.check_output(['docker','image','inspect',image_ids[0]]))[0]
    assert int(datetime.datetime.fromisoformat(image['Created'].replace('Z','+00:00')).timestamp())==epoch, 'image creation timestamp was not normalized'
    config = image['Config']
    assert config['User']=='10001:10001' and config['StopSignal']=='SIGINT'
    assert config['Entrypoint']==['/usr/local/bin/pgderive']
    (output/'manifest.json').write_text(json.dumps({'image_id':image['Id'],'tag':tag,
        'package_manifest_sha256':digest(package/'manifest.json'),'package':manifest,
        'input_sha256':hashes,'image_config':config,'source_date_epoch':epoch,'image_created':image['Created'],
        'reproducibility':{'clean_build_image_ids':image_ids,'no_cache':True,'buildkit':True,'equal':True},
        'scope':'host-native ELF closure; exact bounded input hashes; scratch base; no network during image build'},indent=2)+'\n')
    probe = subprocess.run(['docker','run','--rm','--read-only','--network=none',image_ids[0],'--help'],capture_output=True,text=True)
    (output/'help.stdout').write_text(probe.stdout)
    (output/'help.stderr').write_text(probe.stderr)
    # The CLI currently interprets --help as a config path. Successful dynamic
    # startup is proved by its configuration error, rather than assuming clap.
    assert 'error while loading shared libraries' not in probe.stderr
    assert probe.returncode in (0,1), probe.stderr
    print(image['Id'])


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('package',type=pathlib.Path)
    parser.add_argument('output',type=pathlib.Path)
    parser.add_argument('--tag',required=True)
    args=parser.parse_args()
    build(args.package,args.output,args.tag)
