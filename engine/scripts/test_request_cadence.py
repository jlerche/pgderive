"""The exact-demo adapter binds production inputs without touching live sources."""
import pathlib
import tempfile
import unittest
from unittest import mock
from portfolio_cadence_fixture import DemoFixture, TOTALS_SQL, TOP10_SQL, changes_sql
from worker_harness import Fixture


def fake_fixture(fixture, output, index, **_options):
    fixture.name = 'isolated_'+index
    fixture.output = output


class ExactDemoCadence(unittest.TestCase):
    def test_queries_and_ddl_bind_only_isolated_shared_sources(self):
        with mock.patch.object(Fixture, '__init__', fake_fixture), \
             mock.patch.object(DemoFixture, 'sql', return_value='') as sql:
            primary = DemoFixture(pathlib.Path('/unused'), 'totals')
            peer = DemoFixture(pathlib.Path('/unused'), 'top10', source=primary, top10=True)
        self.assertEqual(primary.query, primary.local(TOTALS_SQL))
        self.assertEqual(peer.query, primary.local(TOP10_SQL))
        self.assertEqual(primary.product_count, 20)
        statements = [call.args[0] for call in sql.call_args_list]
        self.assertTrue(any('CREATE TABLE isolated_totals.demo_products' in text for text in statements))
        self.assertTrue(any('CREATE TABLE isolated_totals.demo_order_lines' in text for text in statements))
        self.assertTrue(any('ALTER PUBLICATION isolated_totals SET TABLE' in text for text in statements))
        self.assertFalse(any('CREATE TABLE isolated_top10.demo_products' in text for text in statements))
        self.assertFalse(any('public.demo_' in text for text in statements))
        self.assertFalse(any('CREATE SCHEMA demo_ivm' in text for text in statements))

    def test_production_phase_changes_catalog_and_fact_in_one_transaction(self):
        with mock.patch.object(Fixture, '__init__', fake_fixture), \
             mock.patch.object(DemoFixture, 'sql', return_value='') as sql:
            fixture = DemoFixture(pathlib.Path('/unused'), 'totals')
            sql.reset_mock()
            with mock.patch('portfolio_cadence_fixture.changes_sql', wraps=changes_sql) as change:
                fixture.change(2)
        change.assert_called_once_with(30, 2, 1)
        sql.assert_called_once()
        statement = sql.call_args.args[0]
        self.assertTrue(statement.startswith('BEGIN;'))
        self.assertTrue(statement.endswith('COMMIT'))
        self.assertIn('UPDATE isolated_totals.demo_products', statement)
        self.assertIn('INSERT INTO isolated_totals.demo_order_lines', statement)

    def test_exact_demo_uses_default_execution_budget_and_product_key(self):
        with tempfile.TemporaryDirectory() as directory, \
             mock.patch.object(Fixture, '__init__', fake_fixture), \
             mock.patch.object(DemoFixture, 'sql', return_value=''):
            fixture = DemoFixture(pathlib.Path(directory), 'totals')
            path = pathlib.Path(directory)/'config.toml'
            path.write_text('sink_table="groups"\n')
            with mock.patch.object(Fixture, 'config', return_value=path) as config:
                fixture.config()
            config.assert_called_once_with(0, query=fixture.query, execution=False)
            self.assertIn('sink_key=["product_id"]', path.read_text())
