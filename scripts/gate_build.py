#!/usr/bin/env python3
"""Identify the actual binaries from one Cargo build, including the live test executable."""
import json
import pathlib
import re
import sys

NAMES={'pgderive':'admin','pgderive_worker':'worker','replication_harness':'replication'}


def manifest(records):
    result={}
    for record in records:
        if record.get('reason')!='compiler-artifact' or not record.get('executable'):
            continue
        target=record['target']; key=None
        if target['kind']==['lib'] and target['name']=='pgderive' and record['profile']['test']:
            key='protocol_tests'
        elif target['kind']==['bin'] and not record['profile']['test']:
            key=NAMES.get(target['name'])
        if key is not None:
            path=pathlib.Path(record['executable']).resolve()
            if key in result or not path.is_file():
                raise ValueError('Missing or ambiguous built executable: '+key)
            result[key]=str(path)
    if set(result)!={'admin','worker','replication','protocol_tests'}:
        raise ValueError('Cargo build did not produce all gate executables')
    return result


PROTOCOL_TEST='storage::tenant::tests::tenant_storage_manifest_live'


def verify_protocol(text):
    if (f'test {PROTOCOL_TEST} ...' not in text
            or not re.search(r'^test result: ok\. 1 passed; 0 failed;',text,re.MULTILINE)):
        raise ValueError('Tenant manifest protocol test did not run successfully')


if __name__=='__main__':
    action, *arguments=sys.argv[1:]
    if action=='build':
        records=(json.loads(line) for line in pathlib.Path(arguments[0]).read_text().splitlines())
        pathlib.Path(arguments[1]).write_text(json.dumps(manifest(records),indent=2)+'\n')
    elif action=='protocol':
        verify_protocol(pathlib.Path(arguments[0]).read_text())
    else:
        raise ValueError('Unknown gate build action: '+action)
