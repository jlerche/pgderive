"""Shared, dependency-free performance evidence primitives."""
import hashlib
import json
import math
import os
import pathlib
import statistics
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
SCHEMA_VERSION = 3
RW_IMAGE = 'risingwavelabs/risingwave@sha256:ee2e8e7a10b728d01b0e149d2a4f35864f0f52a0c889fa6cf9bc32c8ff46c602'
S3_IMAGE = 'chrislusf/seaweedfs@sha256:4e61d15fd35994cb1e43e1e553dff106794841fd9a99ade2fc8c8bfce4d7872d'
QUERY = 'SELECT book_id,COUNT(*) AS positions,SUM(quantity) AS units FROM public.positions GROUP BY book_id'


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def sha256(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def docker(*args):
    return subprocess.check_output(['docker', *args], text=True).strip()



def host_observation(owned_prefix):
    containers = docker('ps', '--filter', 'label=pgderive.performance', '--format', '{{.Names}}').splitlines()
    return {'load_per_cpu': os.getloadavg()[0] / len(os.sched_getaffinity(0)),
            'external_performance_containers': [name for name in containers if not name.startswith(owned_prefix + '-')]}

def wait(predicate, timeout=180):
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        try:
            result = predicate()
            if result:
                return result
        except ChildProcessError:
            raise
        except (OSError, ValueError, RuntimeError) as error:
            last = error
        time.sleep(.1)
    raise TimeoutError(f'Fixture/convergence timeout: {last}')


def distribution(values):
    values = sorted(values)
    if not values:
        return None
    return {'n': len(values), 'p50': statistics.median(values),
            'p95': values[math.ceil(len(values) * .95) - 1],
            'p99': values[math.ceil(len(values) * .99) - 1], 'max': values[-1]}


def process_stats(pid):
    text = pathlib.Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    rss = next(line.split()[1] for line in pathlib.Path(f'/proc/{pid}/status').read_text().splitlines()
               if line.startswith('VmRSS:'))
    return {'cpu_seconds': (int(text[11]) + int(text[12])) / os.sysconf('SC_CLK_TCK'),
            'rss_bytes': int(rss) * 1024}
