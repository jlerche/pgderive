"""Owned native PostgreSQL materialized-view oracle over the compiled SQL itself."""
import json
import subprocess


def quote(name):
    return '"'+name.replace('"','""')+'"'


def sql(fixture,statement):
    command = fixture.pg.copy()
    command.insert(1,'-q')
    return subprocess.check_output(command+[statement],env=fixture.environment,text=True)


def expected(fixture,labels):
    view = quote(fixture.name)+'."pgderive_native_oracle"'
    context = "SET DateStyle='ISO,MDY'; SET TimeZone='UTC'; "
    if not getattr(fixture,'native_oracle_ready',False):
        # Freeze the original query definition across normalized worker restarts.
        # No output key or unique index: the native MV retains SQL duplicates.
        sql(fixture,context+f'CREATE MATERIALIZED VIEW {view} AS {fixture.query} WITH NO DATA')
        fixture.native_oracle_sql = fixture.query
        fixture.native_oracle_ready = True
    sql(fixture,context+f'REFRESH MATERIALIZED VIEW {view}')
    metadata = json.loads(sql(fixture,f"""SELECT json_build_object(
        'kind',c.relkind,'definition',pg_get_viewdef(c.oid,true),
        'published',EXISTS(SELECT 1 FROM pg_publication_rel pr JOIN pg_publication p ON p.oid=pr.prpubid WHERE pr.prrelid=c.oid AND p.pubname='{fixture.name}'),
        'columns',(SELECT json_agg(json_build_object('name',a.attname,'oid',a.atttypid,'modifier',a.atttypmod) ORDER BY a.attnum) FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped))
        FROM pg_class c WHERE c.oid='{view}'::regclass"""))
    assert metadata['kind']=='m' and not metadata['published'],metadata
    assert [column['name'] for column in metadata['columns']]==labels,metadata
    columns = ','.join(quote(label) for label in labels)
    query = f'SELECT jsonb_build_array(NULL,jsonb_build_array({columns})) tuple,count(*)::bigint weight FROM {view} GROUP BY 1'
    # Keep PostgreSQL's JSON text intact: Python float must not round numeric data.
    bag = sql(fixture,context+f"SELECT COALESCE(jsonb_agg(jsonb_build_array(tuple,weight) ORDER BY tuple),'[]') FROM({query}) weighted")
    record = {'query_sql':fixture.native_oracle_sql,'compiled_sql':fixture.query,
              'materialized_view':view,'catalog':metadata,'weighted_bag_json':bag.strip()}
    # Validate exact text without decoding PostgreSQL numeric values to floats.
    json.loads(bag,parse_float=str)
    with (fixture.output/'native-mv-oracle.jsonl').open('a') as evidence:
        evidence.write(json.dumps(record,ensure_ascii=False)+'\n')
    return query


def binary_difference(fixture):
    """Compare observable send representations and multiplicity, including NULL positions."""
    if not hasattr(fixture,'binary_columns'):
        columns = json.loads(sql(fixture,f"SELECT json_agg(json_build_array(a.attname,t.typsend::regproc::text) ORDER BY a.attnum) FROM pg_attribute a JOIN pg_type t ON t.oid=a.atttypid WHERE a.attrelid='{fixture.name}.groups'::regclass AND a.attnum>0 AND NOT a.attisdropped"))
        fixture.binary_columns = ','.join(function+'('+quote(name)+')' for name,function in columns)
    def select(table):
        return f'SELECT array_send(ARRAY[{fixture.binary_columns}]) FROM {fixture.name}.{table}'
    native,result = select('pgderive_native_oracle'),select('groups')
    return sql(fixture,f'SELECT count(*) FROM (({native} EXCEPT ALL {result}) UNION ALL ({result} EXCEPT ALL {native})) d').strip()
