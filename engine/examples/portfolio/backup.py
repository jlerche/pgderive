"""Private, checked SQL dump compression for an explicitly owned demo reset."""
import gzip
import shutil
import subprocess


def dump_sql(command, destination, required):
    """Retain failed intermediates; validate a complete gzip round trip before reset."""
    plain=destination.with_suffix('.plain.sql')
    with plain.open('xb') as stream:
        plain.chmod(0o600)
        subprocess.run(command,stdout=stream,stderr=subprocess.PIPE,check=True,timeout=120)
    with plain.open('rb') as source,gzip.open(destination,'xb') as compressed:
        destination.chmod(0o600)
        shutil.copyfileobj(source,compressed)
    with gzip.open(destination,'rb') as compressed:
        restored=compressed.read()  # Full read verifies footer/CRC as well as payload.
    original=plain.read_bytes()
    if restored!=original or not original or not all(value.encode() in original for value in required):
        raise ValueError('SQL backup failed round-trip or expected-object validation')
    plain.unlink()
    return len(restored)
