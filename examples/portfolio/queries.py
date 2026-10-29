"""The two independently owned demo queries, shared by read-only instrumentation."""
QUERIES = {
    'totals': {'schema': 'demo_ivm', 'table': 'book_totals_keyed', 'config': 'query.toml',
               'container': 'pgderive-demo-engine-1'},
    'top10': {'schema': 'demo_topk', 'table': 'book_top10', 'config': 'top10.toml',
              'container': 'pgderive-demo-engine-top10-1'},
}
