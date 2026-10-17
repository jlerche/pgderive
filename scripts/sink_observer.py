"""Classify executed destination batches without retaining SQL or values."""

def category(query):
    for operation in ('insert','delete'):
        if query.startswith(f'/* pgderive:sink-{operation}-v1 */'.encode()):
            return operation

    if query.startswith(b'INSERT INTO ') and b'pg_catalog.generate_series(1::bigint,$2::bigint)' in query:
        return 'legacy_insert'
    if query.startswith(b'WITH selected AS (SELECT ctid FROM ') and b'LIMIT $2 FOR UPDATE' in query:
        return 'legacy_delete'
    return None
