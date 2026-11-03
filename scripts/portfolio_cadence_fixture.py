"""Exact storefront queries/workload in isolated schemas, with existing oracles."""
import collections
import json
import pathlib
from query_concurrent_harness import memory_rows, TOTALS_SQL, TOP10_SQL, initial_sql, changes_sql
from worker_harness import Fixture
from examples.portfolio.workload import MEMBERSHIP_SECONDS, PRODUCTS, TICKS_PER_SECOND

PORTFOLIO = pathlib.Path(__file__).resolve().parents[1]/'examples/portfolio'


class DemoFixture(Fixture):
    product_count = len(PRODUCTS)

    def __init__(self, output, index, *, source=None, top10=False):
        super().__init__(output, index, rows=0, auctions=0)
        self.source = source or self
        self.top10 = top10
        self.query = self.local(TOP10_SQL if top10 else TOTALS_SQL)
        if source is None:
            # Reuse the production column types, collation, PK and replica
            # identities. Exclude its live schema/publication setup statements.
            ddl = '\n'.join(line for line in (PORTFOLIO/'source.sql').read_text().splitlines()
                            if line.startswith(('CREATE TABLE ', 'ALTER TABLE ')))
            products, lines = self.relations()
            self.sql(self.local(ddl)+f' ALTER PUBLICATION {self.name} SET TABLE {products},{lines};'+self.local(initial_sql()))
        self.sql(f'CREATE MATERIALIZED VIEW {self.name}.native_oracle AS {self.query}')

    def relations(self):
        return self.source.name+'.demo_products', self.source.name+'.demo_order_lines'

    def local(self, sql):
        products, lines = self.relations()
        return sql.replace('public.demo_products', products).replace('public.demo_order_lines', lines)

    def config(self, maximum=0):
        # The generic harness's tiny 128-entry execution budget is deliberately
        # absent: use the worker's normal bounded execution limits for the demo.
        path = super().config(maximum, query=self.query, execution=False)
        path.write_text(path.read_text().replace('sink_table="groups"',
                                                'sink_table="groups"\nsink_key=["product_id"]'))
        return path

    def change(self, tick):
        # One elapsed five-second phase per measurement. Reuse the generator's
        # tick spacing (2Hz * 5s), including catalog and fact changes in one txn.
        statement, events = changes_sql((tick+1)*TICKS_PER_SECOND*MEMBERSHIP_SECONDS, tick, None if tick == 0 else tick-1)
        assert any(event['kind'] == 'catalog' for event in events), events
        self.sql('BEGIN;'+self.local(statement)+'COMMIT')

    def verify(self):
        self.sql(f'REFRESH MATERIALIZED VIEW {self.name}.native_oracle')
        assert self.sql(f'''SELECT count(*) FROM (
          (SELECT * FROM {self.name}.native_oracle EXCEPT ALL SELECT * FROM {self.name}.groups)
          UNION ALL (SELECT * FROM {self.name}.groups EXCEPT ALL SELECT * FROM {self.name}.native_oracle)) d''').strip() == '0', self.query
        products, lines = self.relations()
        memory = memory_rows(self, products, lines, top10=self.top10)
        public = json.loads(self.sql(f'SELECT json_agg(g) FROM {self.name}.groups g'))
        assert collections.Counter((row['product_id'],row['name'],row['category'],row['order_lines'],row['net_units'],row['net_revenue_cents']) for row in public) == collections.Counter(memory)
        assert self.acknowledgement_ahead().strip() == '0', 'slot acknowledged beyond checkpoint'
