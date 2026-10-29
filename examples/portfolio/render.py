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
TOTALS_SQL = 'SELECT book_id, COUNT(*) AS positions, SUM(quantity) AS units FROM public.demo_positions GROUP BY book_id'
TOP10_SQL = TOTALS_SQL + ' ORDER BY units DESC NULLS LAST, book_id ASC LIMIT 10'


def page(mode):
    original = (HERE/'web/totals.html').read_text()
    if mode == 'totals':
        return original
    body = original.replace('<title>Pgderive | Portfolio totals</title>', '<title>Pgderive | Portfolio leaderboard</title>')
    body = body.replace('<h1>Book totals</h1>', '<h1>Live book leaderboard</h1>')
    body = body.replace('The table shows the latest 100 books.', 'Two independent queries maintain the complete totals and the top ten. The complete book overview shows the latest 100 books.')
    body = body.replace('<section class="metrics pipeline"', (HERE/'web/leaderboard-panel.html').read_text()+'<section class="metrics pipeline"', 1)
    css_version = hashlib.sha256((HERE/'web/leaderboard.css').read_bytes()).hexdigest()[:12]
    js_version = hashlib.sha256((HERE/'web/leaderboard.js').read_bytes()).hexdigest()[:12]
    body = body.replace('</head>', f'<link rel="stylesheet" href="/leaderboard.css?v={css_version}"></head>')
    body = body.replace('</body>', f'<script type="module" src="/leaderboard.js?v={js_version}"></script></body>')
    body = body.replace("fetch('/config.json')", "fetch('/config.json',{cache:'no-store'})")
    body = body.replace("document.addEventListener('visibilitychange'", "setInterval(refresh,3000);document.addEventListener('visibilitychange'")
    guard = "const safeInteger=value=>{if(typeof value==='number'&&!Number.isSafeInteger(value))throw Error('Value exceeds exact demo range');const n=BigInt(value);if(n>BigInt(Number.MAX_SAFE_INTEGER)||n<BigInt(Number.MIN_SAFE_INTEGER))throw Error('Value exceeds exact demo range');return Number(n)};"
    body = body.replace('const history=[],seen=new Map();', guard+'const history=[],seen=new Map();')
    body = body.replace(".select('*').order('book_id'", ".select('book_id::text,positions::text,units::text').order('book_id'")
    body = body.replace('const rows=data.sort(', "const rows=data.map(row=>({...row,book_id:safeInteger(row.book_id),positions:safeInteger(row.positions),units:row.units===null?null:safeInteger(row.units)})).sort(")
    body = body.replace("$('rows').replaceChildren();", "if(!Number.isSafeInteger(gross))throw Error('Totals exceed exact demo range');$('rows').replaceChildren();")
    body = body.replace("hasSnapshot=true;$('positions')", "if(!Number.isSafeInteger(positions))throw Error('Positions exceed exact demo range');hasSnapshot=true;$('positions')")
    body = body.replace('R2 state size</div>', 'Totals R2 state size</div>').replace('Worker memory</div>', 'Totals worker memory</div>')
    return body


def render(destination, environment):
    mode = environment.get('DEMO_PAGE_MODE', 'leaderboard')
    if mode not in ('leaderboard', 'totals'):
        raise ValueError('DEMO_PAGE_MODE must be leaderboard or totals')
    prefix = environment['DEMO_OBJECT_PREFIX'].rstrip('/')
    if not prefix:
        raise ValueError('DEMO_OBJECT_PREFIX must be an exclusively owned nonempty namespace')
    destination.mkdir(parents=True, exist_ok=True)
    template = Template((HERE/'worker.toml.in').read_text())
    common = {'postgres_host': environment.get('DEMO_POSTGRES_HOST', 'db'),
              'endpoint': environment['DEMO_OBJECT_ENDPOINT'], 'bucket': environment['DEMO_OBJECT_BUCKET']}
    for filename, schema, query_id, sql, suffix in [
        ('query.toml', 'demo_ivm', 'book_totals_keyed', TOTALS_SQL, 'totals'),
        ('top10.toml', 'demo_topk', 'book_top10', TOP10_SQL, 'top10')]:
        values = common | {'schema': schema, 'query_id': query_id, 'table': query_id,
                           'prefix': prefix+'/'+suffix, 'slot': 'pgderive_demo_'+suffix, 'sql': sql}
        (destination/filename).write_text(template.substitute({key: json.dumps(value) for key, value in values.items()}))
    shutil.copyfile(HERE/'engines.compose.yaml', destination/'engines.compose.yaml')
    web = destination/'web'
    web.mkdir(exist_ok=True)
    for path in (HERE/'web').iterdir():
        if path.name != 'leaderboard-panel.html':
            shutil.copyfile(path, web/path.name)
    (web/'leaderboard.html').write_text(page('leaderboard'))
    (web/'index.html').write_text(page(mode))
    (web/'config.json').write_text(json.dumps({'anonKey': environment['DEMO_ANON_KEY'],
        'schema': 'demo_ivm', 'table': 'book_totals_keyed', 'leaderboardSchema': 'demo_topk',
        'leaderboardTable': 'book_top10', 'pageMode': mode})+'\n')
    (destination/'page-mode.json').write_text(json.dumps({'mode': mode, 'available': ['leaderboard', 'totals']})+'\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('destination', type=Path)
    render(parser.parse_args().destination, os.environ)
