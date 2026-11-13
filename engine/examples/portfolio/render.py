#!/usr/bin/env python3
"""Render nonsecret demo inputs; page choice is independent of query lifecycle."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
from string import Template

HERE = Path(__file__).resolve().parent
TOTALS_SQL = ('SELECT p.product_id, p.name, p.category, g.order_lines, g.net_units, g.net_revenue_cents '
              'FROM public.demo_products p JOIN ('
              'SELECT product_id, COUNT(*) AS order_lines, SUM(quantity) AS net_units, '
              'SUM(quantity * unit_price_cents) AS net_revenue_cents '
              'FROM public.demo_order_lines GROUP BY product_id) g ON p.product_id = g.product_id')
TOP10_SQL = TOTALS_SQL + ' ORDER BY net_revenue_cents DESC NULLS LAST, p.product_id ASC LIMIT 10'


def page():
    body = (HERE/'web/index.html').read_text()
    for name in ['storefront.css','storefront.js','metrics.css','history.css']:
        version=hashlib.sha256((HERE/'web'/name).read_bytes()).hexdigest()[:12]
        if name in ['metrics.css','history.css']:
            body=body.replace('</head>',f'<link rel="stylesheet" href="/{name}?v={version}"></head>')
        else:
            body=body.replace('/'+name,'/'+name+'?v='+version)
    for name,attribute in [('metrics.js','metrics'),('history.js','history')]:
        version=hashlib.sha256((HERE/'web'/name).read_bytes()).hexdigest()[:12]
        body=body.replace('id="metricsPanel"',f'id="metricsPanel" data-{attribute}-script="/{name}?v={version}"')
    panels=[]
    for name,label in [('totals','All-products query'),('top10','Top-ten query')]:
        panels.append(f'''<section class="metric-history" data-history-query="{name}" aria-label="{label} history"><div class="history-heading"><h2>{label} · server history</h2><p>One point per minute. Missing intervals stay blank.</p><button type="button" data-history-download>Download CSV</button></div><div class="history-note" role="status">Loading measurements…</div><div class="history-grid"></div></section>''')
    return body.replace('__HISTORY_PANELS__',''.join(panels))


def render(destination, environment):
    prefix = environment['DEMO_OBJECT_PREFIX'].rstrip('/')
    if not prefix:
        raise ValueError('DEMO_OBJECT_PREFIX must be an exclusively owned nonempty namespace')
    destination.mkdir(parents=True, exist_ok=True)
    template = Template((HERE/'worker.toml.in').read_text())
    common = {'postgres_host': environment.get('DEMO_POSTGRES_HOST', 'db'),
              'endpoint': environment['DEMO_OBJECT_ENDPOINT'], 'bucket': environment['DEMO_OBJECT_BUCKET']}
    for filename, schema, query_id, sql, suffix in [
        ('query.toml', 'demo_ivm', 'product_totals', TOTALS_SQL, 'totals'),
        ('top10.toml', 'demo_topk', 'product_top10', TOP10_SQL, 'top10')]:
        values = common | {'schema': schema, 'query_id': query_id, 'table': query_id,
                           'prefix': prefix+'/'+suffix, 'slot': 'pgderive_demo_'+suffix, 'sql': sql}
        (destination/filename).write_text(template.substitute({key: json.dumps(value) for key, value in values.items()}))
    shutil.copyfile(HERE/'engines.compose.yaml', destination/'engines.compose.yaml')
    web = destination/'web'
    web.mkdir(exist_ok=True)
    for path in (HERE/'web').iterdir():
        shutil.copyfile(path, web/path.name)
    (web/'index.html').write_text(page())
    (web/'config.json').write_text(json.dumps({'anonKey': environment['DEMO_ANON_KEY'],
        'schema': 'demo_ivm', 'table': 'product_totals', 'leaderboardSchema': 'demo_topk',
        'leaderboardTable': 'product_top10', 'sql': TOTALS_SQL.replace(' FROM ', '\nFROM ').replace(' GROUP BY ', '\nGROUP BY ').replace(' JOIN ', '\nJOIN ')})+'\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('destination', type=Path)
    render(parser.parse_args().destination, os.environ)
