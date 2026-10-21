"""Performance observations with explicit phase and measurement boundaries."""
import hashlib
import json
import os
import pathlib
import platform
import subprocess


def process(worker):
    root = pathlib.Path(f'/proc/{worker.process.pid}')
    fields = (root/'stat').read_text().rsplit(')',1)[1].split()
    status = dict(line.split(':',1) for line in (root/'status').read_text().splitlines() if ':' in line)
    return {'pid':worker.process.pid,'cpu_seconds':(int(fields[11])+int(fields[12]))/os.sysconf('SC_CLK_TCK'),
            'rss_bytes':int(status['VmRSS'].split()[0])*1024,'peak_rss_bytes':int(status['VmHWM'].split()[0])*1024}


def checked_phases(event):
    values = event['phases_us']
    required = {'source_decode','inputs','prepare_including_object_io',
                'finalize','publication','local_completion'}
    assert set(values) == required, values
    nested = event['publication_us']
    assert set(nested) == {'membership_layout_progress','sink_dml','commit'}
    assert all(isinstance(value,int) and value>=0 for value in [*values.values(),*nested.values()])
    assert sum(nested.values()) <= values['publication'], (nested,values)
    return values


def requests(path, offset):
    rows = [json.loads(line) for line in path.read_text().splitlines()[offset:] if line.startswith('{')]
    rows = [row for row in rows if 'method' in row]
    return {method:{'count':sum(row['method']==method for row in rows),
                    'request_bytes':sum(row.get('request_body_bytes') or 0 for row in rows if row['method']==method),
                    'response_bytes':sum(row.get('response_body_bytes') or 0 for row in rows if row['method']==method),
                    'unknown_request_sizes':sum(row.get('request_body_bytes') is None for row in rows if row['method']==method)}
            for method in ('GET','PUT','POST','DELETE','HEAD')}


def source_files():
    return [pathlib.Path('Cargo.toml'),pathlib.Path('Cargo.lock'),pathlib.Path('rust-toolchain.toml'),
            *sorted(pathlib.Path('queries').rglob('*.json')),
            *sorted(pathlib.Path('queries').rglob('*.sql')),
            *sorted(pathlib.Path('src').rglob('*.rs')),
            *sorted(pathlib.Path('src').rglob('*.sql')),
            *sorted(pathlib.Path('scripts').glob('*.py'))]


def provenance(output, command, fixture):
    files = source_files()
    # Hash executables, never the configuration or inherited environment/secrets.
    binaries = {str(path):hashlib.sha256(path.read_bytes()).hexdigest() for item in command if (path:=pathlib.Path(item)).is_file()}
    value = {'version':1,'git_head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
             'dirty':bool(subprocess.check_output(['git','status','--porcelain'],text=True).strip()),
             'python':platform.python_version(),'platform':platform.platform(),
             'postgres':fixture.sql('SHOW server_version').strip(), 'binary_sha256':binaries,
             'source_sha256':{str(path):hashlib.sha256(path.read_bytes()).hexdigest() for path in files},
             'measurement_scope':'one local worker process and serial owned fixture; source PostgreSQL and proxy CPU excluded',
             'timing_scope':'engine preparation includes scalar/operator work and object I/O; publication subphases nest inside publication',
             'latency_scope':'client transaction invocation to published event observation, including psql startup; excludes subsequent oracle verification; event precedes ACK',
             'memory_scope':'Linux worker process VmHWM, not a resource bound',
             'build_profile':os.environ.get('PGDERIVE_PORTFOLIO_BUILD_PROFILE','qualification-instrumented-or-debug')}
    (output/'provenance.json').write_text(json.dumps(value,indent=2)+'\n')
    return value
