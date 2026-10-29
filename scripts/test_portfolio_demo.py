"""Validate deterministic cutoff phases and nonsecret selectable demo rendering."""
import importlib.util
import pathlib
import re
import shutil
import tempfile
import tomllib
import unittest
from unittest import mock

HERE = pathlib.Path(__file__).resolve().parents[1]/'examples/portfolio'


def module(name):
    spec = importlib.util.spec_from_file_location(name, HERE/(name+'.py'))
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


class PortfolioDemo(unittest.TestCase):
    def test_generated_asset_urls_change_with_their_contents(self):
        renderer = module('render')
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            shutil.copytree(HERE/'web', root/'web')
            with mock.patch.object(renderer, 'HERE', root):
                first = renderer.page('leaderboard')
                css = root/'web/leaderboard.css'; css.write_text(css.read_text()+'\n/* updated */')
                second = renderer.page('leaderboard')
                js = root/'web/leaderboard.js'; js.write_text(js.read_text()+'\n// updated')
                third = renderer.page('leaderboard')
                history = root/'web/history.js'; history.write_text(history.read_text()+'\n// updated')
                fourth = renderer.page('leaderboard')
            css_url = lambda html: re.search(r'/leaderboard.css\?v=[a-f0-9]+', html).group()
            js_url = lambda html: re.search(r'/leaderboard.js\?v=[a-f0-9]+', html).group()
            self.assertNotEqual(css_url(first), css_url(second))
            self.assertEqual(js_url(first), js_url(second))
            self.assertEqual(css_url(second), css_url(third))
            self.assertNotEqual(js_url(second), js_url(third))
            history_url = lambda html: re.search(r'/history.js\?v=[a-f0-9]+', html).group()
            self.assertNotEqual(history_url(third), history_url(fourth))

    def test_slow_entry_exit_and_cutoff_tie(self):
        workload = module('workload')
        expected = [set(range(1, 11)), set(range(1, 10)) | {11}, set(range(1, 11)), set(range(1, 11))]
        for phase, members in enumerate(expected):
            ordered = sorted(range(1, 21), key=lambda book: (-workload.total(book, phase), book))
            self.assertEqual(set(ordered[:10]), members)
        self.assertEqual(workload.total(10, 3), workload.total(12, 3))
        self.assertEqual(workload.MEMBERSHIP_SECONDS, 35)
        self.assertEqual(workload.changes_sql(69, 0, 0)[1], 0)
        self.assertEqual(workload.changes_sql(70, 1, 0)[1], 1)

    def test_membership_clock_uses_elapsed_seconds_not_commit_count(self):
        workload = module('workload')
        phases = workload.Phases(0, 100)
        self.assertEqual(phases.current(134.999), 0)
        self.assertEqual(phases.current(135), 1)
        self.assertEqual(phases.current(169.999), 1)
        self.assertEqual(phases.current(170), 2)
        self.assertEqual(phases.current(1000), 3)
        self.assertEqual(phases.current(1001), 3)
        resumed = workload.Phases(7, 2000)
        self.assertEqual(resumed.current(2034), 7)
        self.assertEqual(resumed.current(2035), 8)

    def test_page_modes_preserve_original_and_isolate_queries(self):
        renderer = module('render')
        environment = {'DEMO_OBJECT_PREFIX': 'owned-demo', 'DEMO_OBJECT_ENDPOINT': 'https://objects.invalid',
                       'DEMO_OBJECT_BUCKET': 'owned-fixture', 'DEMO_ANON_KEY': 'public-browser-key'}
        with tempfile.TemporaryDirectory() as directory:
            output = pathlib.Path(directory)
            for mode in ('totals', 'leaderboard'):
                renderer.render(output, environment | {'DEMO_PAGE_MODE': mode})
                self.assertEqual((output/'web/index.html').read_text(), renderer.page(mode))
            for metric in ['positions', 'net', 'gross', 'count', 'sourceChanges', 'processed', 'stateSize', 'workerMemory', 'sinkLatency', 'engineLatency', 'sourceLatency', 'metricHistory', 'feed', 'chart']:
                self.assertIn('id="'+metric+'"', renderer.page('leaderboard'))
            totals = tomllib.loads((output/'query.toml').read_text())
            top10 = tomllib.loads((output/'top10.toml').read_text())
            self.assertEqual(totals['replication']['publication'], top10['replication']['publication'])
            for section, field in [('worker', 'catalog_schema'), ('worker', 'query_id'), ('worker', 'object_prefix'), ('replication', 'slot')]:
                self.assertNotEqual(totals[section][field], top10[section][field])
            self.assertTrue(top10['worker']['query']['sql'].endswith('ORDER BY units DESC NULLS LAST, book_id ASC LIMIT 10'))
            self.assertNotIn('secret_access_key', (output/'web/config.json').read_text())
            self.assertIn('name="historyQuery"', renderer.page('leaderboard'))
            compared = renderer.page('leaderboard')
            self.assertIn('<option value="both">Both queries</option>', compared)
            self.assertEqual(compared.count('data-history-query='), 2)
            self.assertEqual(compared.count('name="historyCategory"'), 1)
            ids = re.findall(r'id="([^"]+)"', compared)
            self.assertEqual(len(ids), len(set(ids)))
            self.assertNotIn('name="historyQuery"', renderer.page('totals'))
            self.assertEqual(renderer.page('totals'), (HERE/'web/totals.html').read_text())
            with self.assertRaises(ValueError):
                renderer.render(output, environment | {'DEMO_PAGE_MODE': 'invalid'})
