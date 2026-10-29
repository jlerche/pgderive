"""Storefront schema, five-second workload and nonsecret renderer regressions."""
import gzip
import importlib.util
import sys
import pathlib
import re
import shutil
import tempfile
import tomllib
import unittest
from unittest import mock

HERE=pathlib.Path(__file__).resolve().parents[1]/'examples/portfolio'


def module(name):
    spec=importlib.util.spec_from_file_location(name,HERE/(name+'.py'))
    loaded=importlib.util.module_from_spec(spec);spec.loader.exec_module(loaded);return loaded


class PortfolioDemo(unittest.TestCase):
    def test_sql_backup_round_trip_and_failed_evidence(self):
        backup=module('backup')
        with tempfile.TemporaryDirectory() as directory:
            root=pathlib.Path(directory);path=root/'source.sql.gz'
            command=[sys.executable,'-c',"print('CREATE TABLE source; INSERT INTO source VALUES(1);')"]
            size=backup.dump_sql(command,path,['CREATE TABLE source','INSERT INTO source'])
            with gzip.open(path,'rb') as stream:self.assertEqual(len(stream.read()),size)
            self.assertEqual(path.stat().st_mode&0o777,0o600)
            self.assertFalse(path.with_suffix('.plain.sql').exists())
            failed=root/'failed.sql.gz'
            with self.assertRaises(ValueError):backup.dump_sql(command,failed,['missing expected object'])
            self.assertTrue(failed.exists());self.assertTrue(failed.with_suffix('.plain.sql').exists())

    def test_asset_hashes_change_and_metrics_are_closed(self):
        renderer=module('render')
        with tempfile.TemporaryDirectory() as directory:
            root=pathlib.Path(directory);shutil.copytree(HERE/'web',root/'web')
            with mock.patch.object(renderer,'HERE',root):
                first=renderer.page()
                for name in ['storefront.css','storefront.js','history.js','metrics.js']:
                    path=root/'web'/name;path.write_text(path.read_text()+'\n/* changed */')
                second=renderer.page()
            for name in ['storefront.css','storefront.js','history.js','metrics.js']:
                pattern='/'+re.escape(name)+r'\?v=[a-f0-9]+'
                self.assertNotEqual(re.search(pattern,first).group(),re.search(pattern,second).group())
            self.assertRegex(first,r'id="metricsPanel"[^>]* hidden')
            self.assertRegex(first,r'id="underHood"[^>]* hidden')
            self.assertIn('<option value="both">Both queries</option>',first)
            self.assertEqual(first.count('data-history-query='),2)
            ids=re.findall(r'id="([^"]+)"',first);self.assertEqual(len(ids),len(set(ids)))
            self.assertNotIn('Book totals',first)

    def test_elapsed_membership_clock_has_no_catch_up_burst(self):
        workload=module('workload');phases=workload.Phases(0,100)
        self.assertEqual(workload.MEMBERSHIP_SECONDS,5)
        for now,expected in [(104.999,0),(105,1),(109.999,1),(110,2),(1000,3),(1001,3)]:
            self.assertEqual(phases.current(now),expected)
        resumed=workload.Phases(7,2000)
        self.assertEqual(resumed.current(2004),7);self.assertEqual(resumed.current(2005),8)

    def test_each_product_cycles_real_sale_refund_adjustment_and_cancel(self):
        workload=module('workload')
        for product in range(8):
            statements=[];kinds=[]
            for cycle in range(4):
                sql,events=workload.changes_sql(product+cycle*8,0,0)
                statements.append(sql);kinds.append(events[0]['kind'])
            self.assertEqual(kinds,['sale','refund','sale','cancel'])
            self.assertIn(',2,',statements[0]);self.assertIn(',-1,',statements[1]);self.assertIn(',3,',statements[2])
            self.assertTrue(statements[3].startswith('DELETE'))
        sql,events=workload.changes_sql(0,1,0)
        self.assertIn('UPDATE public.demo_products',sql)
        self.assertIn('INSERT INTO public.demo_order_lines',sql)
        self.assertTrue(any(event['kind']=='catalog' for event in events))
        self.assertTrue(any(event.get('unit_price_cents')=='12900' for event in events))
        self.assertNotIn('cents each',str(events))

    def test_rendered_join_queries_keep_separate_owned_state(self):
        renderer=module('render')
        environment={'DEMO_OBJECT_PREFIX':'owned-storefront','DEMO_OBJECT_ENDPOINT':'https://objects.invalid',
            'DEMO_OBJECT_BUCKET':'fixture','DEMO_ANON_KEY':'public-browser-key'}
        with tempfile.TemporaryDirectory() as directory:
            output=pathlib.Path(directory);renderer.render(output,environment)
            self.assertEqual((output/'web/index.html').read_text(),renderer.page())
            totals=tomllib.loads((output/'query.toml').read_text());top=tomllib.loads((output/'top10.toml').read_text())
            self.assertEqual(totals['replication']['publication'],top['replication']['publication'])
            for section,field in [('worker','catalog_schema'),('worker','query_id'),('worker','object_prefix'),('replication','slot')]:
                self.assertNotEqual(totals[section][field],top[section][field])
            self.assertEqual(totals['worker']['sink_key'],['product_id'])
            self.assertIn('JOIN (SELECT product_id',top['worker']['query']['sql'])
            self.assertIn('FROM public.demo_order_lines GROUP BY product_id',top['worker']['query']['sql'])
            self.assertIn('SUM(quantity * unit_price_cents)',top['worker']['query']['sql'])
            self.assertTrue(top['worker']['query']['sql'].endswith('p.product_id ASC LIMIT 10'))
            self.assertNotIn('secret_access_key',(output/'web/config.json').read_text())
