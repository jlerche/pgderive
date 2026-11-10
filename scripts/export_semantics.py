#!/usr/bin/env python3
"""Refresh frozen SQL histories from existing fixtures and a native PostgreSQL oracle.

This is an explicit fixture refresh, not a normal gate dependency. No workers,
logical slots or object storage are used. Every source fixture owns its schema.
"""
import argparse
import collections
import hashlib
import contextlib
import runpy
import sys
import uuid
import json
import pathlib
import os
import subprocess
from unittest.mock import patch
from native_mv_oracle import quote
from worker_harness import Fixture, run_cases
import worker_harness
from sql_compiler_harness import SqlFixture
from application_model import statements

MODULES = ('sql_composition', 'set_operation', 'subquery', 'numeric_arithmetic',
           'temporal_arithmetic', 'join_predicate', 'outer_join', 'ordering',
           'ordering_domains', 'unordered', 'sliding_extrema', 'prefix_window',
           'expression_composition', 'sliding_window',
           'functions_compiler', 'computed_join',
           'aggregate_inputs_compiler', 'relational_compiler', 'partition_compiler',
           'numeric_compiler', 'circuit_numeric', 'native_compiler',
           'navigation_compiler', 'case_compiler', 'lookup_compiler', 'sql_compiler')


class SqlSession:
    """One psql connection per owned fixture; avoid a new process per statement."""
    def __init__(self, fixture):
        command=fixture.pg.copy(); command[command.index('-Atc')]='-At'
        command[1:1]=['-X','-q']
        self.error=(fixture.output/'oracle-postgres.log').open('w')
        self.process=subprocess.Popen(command,env=fixture.environment,stdin=subprocess.PIPE,
                                      stdout=subprocess.PIPE,stderr=self.error,text=True)

    def query(self, statement):
        marker='pgderive_export_'+uuid.uuid4().hex
        self.process.stdin.write(statement+';\n\\echo '+marker+'\n')
        self.process.stdin.flush()
        rows=[]
        for line in self.process.stdout:
            if line.rstrip('\n')==marker:
                return ''.join(rows)
            rows.append(line)
        raise RuntimeError('Oracle SQL connection failed; see oracle-postgres.log')

    def close(self):
        self.process.stdin.close()
        self.process.wait(timeout=10)
        self.process.stdout.close()
        self.error.close()


def sql(fixture, statement):
    if not hasattr(fixture,'semantic_session'):
        fixture.semantic_session=SqlSession(fixture)
    return fixture.semantic_session.query(statement)


class Captured(Exception):
    """Stop the live-only tail after exporting the shared semantic history."""


def snapshot(fixture, relations):
    result = collections.Counter()
    for relation in relations:
        fields = ','.join(f"'{c['name']}',"+(f"pg_catalog.boolout({quote(c['name'])})::text" if c['oid']==16 else f"{quote(c['name'])}::text") for c in relation['columns'])
        rows = json.loads(sql(fixture, "SET TimeZone='UTC'; SET DateStyle='ISO,MDY'; SET IntervalStyle='iso_8601'; "
                              f"SELECT coalesce(json_agg(json_build_object({fields})),'[]') "
                              f"FROM {quote(fixture.name)}.{quote(relation['table'])}"))
        result.update((relation['table'], json.dumps(row, sort_keys=True)) for row in rows)
    return result


def updates(rows):
    return [{'table': table, 'row': json.loads(row), 'weight': weight}
            for (table, row), weight in sorted(rows.items()) if weight]


def relations(fixture):
    rows = json.loads(sql(fixture, f"""SELECT json_agg(json_build_object(
      'oid',c.oid,'schema','source','table',c.relname,'columns',
      (SELECT json_agg(json_build_object('position',a.attnum,'name',a.attname,
        'oid',a.atttypid,'modifier',a.atttypmod,'nullable',NOT a.attnotnull,
        'primary',EXISTS(SELECT 1 FROM pg_index i WHERE i.indrelid=c.oid
                         AND i.indisprimary AND a.attnum=ANY(i.indkey)),
        'collation',a.attcollation) ORDER BY a.attnum)
       FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped))
      ORDER BY c.oid) FROM pg_class c JOIN pg_publication_rel pr ON pr.prrelid=c.oid
      JOIN pg_publication p ON p.oid=pr.prpubid WHERE p.pubname='{fixture.name}'"""))
    for index, row in enumerate(rows, 1):
        row['oid'] = index
        for column in row['columns']:
            for field in ('oid','collation'):
                column[field] = int(column[field])
    return rows


def native_cell(column):
    name=quote(column['name'])
    if column['oid']!=1186:
        return f"(to_json({name})#>>'{{}}')"
    # Preserve interval components, rather than collapsing month/day/time values
    # through duration equality. PostgreSQL computes every component independently.
    return (f"CASE WHEN {name} IS NULL OR {name}::text IN('infinity','-infinity') THEN {name}::text "
            f"ELSE 'P'||(EXTRACT(year FROM {name})*12+EXTRACT(month FROM {name}))::bigint::text"
            f"||'M'||EXTRACT(day FROM {name})::bigint::text"
            f"||'DT'||EXTRACT(hour FROM {name})::bigint::text"
            f"||'H'||(EXTRACT(minute FROM {name})*60+EXTRACT(second FROM {name}))::text||'S' END")


def observation(fixture):
    view = quote(fixture.name)+'."semantics_oracle"'
    sql(fixture, f"SET TimeZone='UTC'; SET DateStyle='ISO,MDY'; SET IntervalStyle='iso_8601'; DROP VIEW IF EXISTS {view}; CREATE VIEW {view} AS {getattr(fixture,'oracle_query',fixture.query)}")
    columns = json.loads(sql(fixture, f"SELECT json_agg(attname ORDER BY attnum) FROM pg_attribute WHERE attrelid='{view}'::regclass AND attnum>0"))
    fixture.semantic_output = json.loads(sql(fixture, f"""SELECT json_agg(json_build_object(
      'name',a.attname,'oid',a.atttypid::bigint,'modifier',a.atttypmod,
      'collation',CASE WHEN a.attcollation=0 THEN NULL ELSE
       (SELECT json_build_array(n.nspname,c.collname) FROM pg_collation c
        JOIN pg_namespace n ON n.oid=c.collnamespace WHERE c.oid=a.attcollation) END)
      ORDER BY a.attnum) FROM pg_attribute a WHERE a.attrelid='{view}'::regclass AND a.attnum>0"""))
    cells = ','.join(native_cell(c) for c in fixture.semantic_output)
    bag = json.loads(sql(fixture, "SET TimeZone='UTC'; SET DateStyle='ISO,MDY'; SET IntervalStyle='iso_8601'; "
                          f"SELECT coalesce(json_agg(json_build_array(cells,weight)),'[]') FROM "
                          f"(SELECT jsonb_build_array({cells}) cells,count(*) weight FROM {view} GROUP BY 1 ORDER BY 1) bag"))

    choices=None
    if fixture.output.parent.name=='circuit_numeric' and (fixture.mode.startswith('extrema-') or fixture.mode=='group-key'):
        rows=json.loads(sql(fixture, f"""SELECT coalesce(json_agg(json_build_array(canonical,spellings) ORDER BY canonical),'[]') FROM (
          SELECT (to_json(o.price)#>>'{{}}') canonical,
            json_agg(DISTINCT b.price::text ORDER BY b.price::text) spellings
          FROM {view} o JOIN {quote(fixture.name)}.bid b ON b.price IS NOT DISTINCT FROM o.price
          GROUP BY (to_json(o.price)#>>'{{}}')) retained"""))
        choices={'column':0,'choices':rows}
    return {'expected':bag,'representatives':choices}


def capture(fixture, command, history, *args, **options):
    # These audited hooks only assert duplicate output or exercise a rollback.
    # Their live-only checks remain in --full; every committed bag is replayed.
    hooks={'functions_compiler':{'initial'}, 'relational_compiler':{'initial'},
           'lookup_compiler':{'after'}}
    assert not args and set(options)<=hooks.get(fixture.output.parent.name,hooks.get(fixture.output.name,set())), 'Unmigrated history hook'

    layout = relations(fixture)
    prior = snapshot(fixture, layout)
    steps = [dict(updates=updates(prior), **observation(fixture), source_rows=0,
                  measure=False, restart=False)]
    for statement in [*history.changes, None, history.final]:
        if statement is None:
            steps.append(dict(updates=[], **observation(fixture), source_rows=0,
                              measure=False, restart=True))
            continue
        sql(fixture, history.session+'BEGIN;'+statement+';COMMIT')
        after = snapshot(fixture, layout)
        delta = after.copy(); delta.subtract(prior)
        rows = updates(delta)
        steps.append(dict(updates=rows, **observation(fixture),
                          source_rows=sum(abs(r['weight']) for r in rows), measure=False, restart=False))
        prior = after
    finish(fixture, layout, steps)

def finish(fixture, layout, steps):
    case = dict(name=fixture.output.name, sql=fixture.query.replace(fixture.name+'.','source.'),
                relations=layout, steps=steps, output=fixture.semantic_output,
                rejected=[q.replace(fixture.name+'.','source.') for q in getattr(fixture,'semantic_rejections',[])])
    fixture.output.joinpath('semantics-case.json').write_text(json.dumps(case, ensure_ascii=False)+'\n')
    raise Captured

def capture_trace(fixture, command):
    layout=relations(fixture); prior=snapshot(fixture,layout)
    steps=[dict(updates=updates(prior),**observation(fixture),source_rows=0,measure=False,restart=False)]
    for index, transaction in enumerate(fixture.trace['history']):
        sql(fixture,statements(fixture.name,transaction))
        after=snapshot(fixture,layout)
        if transaction['rollback']:
            assert after==prior, 'Rollback changed source snapshot'
        else:
            delta=after.copy(); delta.subtract(prior); rows=updates(delta)
            steps.append(dict(updates=rows,**observation(fixture),source_rows=sum(abs(r['weight']) for r in rows),measure=False,restart=False))
        if index==4:
            steps.append(dict(updates=[],**observation(fixture),source_rows=0,measure=False,restart=True))
        prior=after
    finish(fixture,layout,steps)


def export_cases(cases, qualify, **options):
    # runpy's SQL-compiler entry point defines its fixture in __main__.
    fixture_type=qualify.__globals__.get('SqlFixture',SqlFixture)
    with patch.object(fixture_type,'rejected',rejected):
        run_cases(cases,qualify,**options)


def no_worker(fixture, *args, **options):
    if hasattr(fixture, 'trace'):
        capture_trace(fixture, [])
    raise AssertionError('Unmigrated live setup in semantic export: '+str(fixture.output))


def rejected(fixture, command, label, query):
    if not hasattr(fixture,'semantic_rejections'):
        fixture.semantic_rejections = []
    fixture.semantic_rejections.append(query)


@contextlib.contextmanager
def managed(fixture):
    try:
        yield fixture
    except Captured:
        sql(fixture,f'DROP PUBLICATION {fixture.name}; DROP SCHEMA {fixture.name} CASCADE')
        fixture.semantic_session.close()
    except BaseException:
        fixture.semantic_session.close()
        fixture.preserve()
        raise
    else:
        raise AssertionError('Fixture never exported its history')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output',type=pathlib.Path)
    parser.add_argument('--modules',nargs='+',choices=MODULES,default=MODULES)
    args=parser.parse_args(); args.output.mkdir(parents=True,exist_ok=True)
    cases=[]
    with patch.dict(os.environ,{'PGDERIVE_CHECK_PROFILE':'full'}), patch.object(worker_harness,'run_cases',export_cases), patch.object(Fixture,'sql',sql), patch.object(Fixture,'run_history',capture), patch.object(Fixture,'managed',managed), patch.object(SqlFixture,'rejected',rejected), patch.object(Fixture,'start',no_worker):
        for name in args.modules:
            directory=args.output/name; directory.mkdir()
            with patch.object(sys,'argv',[name,str(directory)]):
                runpy.run_path(str(pathlib.Path(__file__).with_name(name+'_harness.py')),run_name='__main__')
            paths=sorted(directory.rglob('semantics-case.json'))
            assert paths, 'No semantic histories exported: '+name
            for path in paths:
                case=json.loads(path.read_text()); case['name']=name+'--'+(str(path.parent.relative_to(directory)) if path.parent!=directory else 'default'); cases.append(case)
            print(f'{name}: {len(paths)} histories captured',flush=True)
    suite={'version':1,'cases':cases}
    scripts=pathlib.Path(__file__).resolve().parent
    sources={scripts/(name+'_harness.py') for name in args.modules}
    sources.add(pathlib.Path(__file__).resolve())
    sources.update(pathlib.Path(m.__file__).resolve() for m in tuple(sys.modules.values())
                   if getattr(m,'__file__',None) and pathlib.Path(m.__file__).resolve().parent==scripts)
    manifest={'version':1,'families':dict(collections.Counter(case['name'].split('--')[0] for case in cases)),
              'cases':[case['name'] for case in cases],
              'histories':{case['name']:{'commits':sum(not step['restart'] for step in case['steps']),
                  'oracle_checks':sum(step['expected'] is not None for step in case['steps']),
                  'restarts':sum(step['restart'] for step in case['steps'])} for case in cases},
              'source_sha256':{str(p.relative_to(scripts.parent)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(sources)}}
    plain=args.output/'semantics.json'; plain.write_text(json.dumps(suite,ensure_ascii=False,separators=(',',':'))+'\n')
    packed=args.output/'semantics.json.zst'
    subprocess.run(['zstd','-q','-f','-19',str(plain),'-o',str(packed)],check=True)
    manifest['fixture_sha256']=hashlib.sha256(packed.read_bytes()).hexdigest()
    (args.output/'semantics-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')


if __name__=='__main__':
    main()
