"""Classify executed destination batches without retaining SQL or values."""

def category(query):
    for operation in ('insert','delete'):
        if query.startswith(f'/* pgderive:sink-{operation}-v1 */'.encode()):
            return operation

    return None
