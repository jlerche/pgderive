"""Native-oracle qualification of sink keys derived from shared edge properties."""
import json
from keyed_sink_harness import KeyedFixture


def qualify(output, command):
    output.mkdir()
    results = []
    for mode in ['projection', 'inner', 'outer', 'pair']:
        case = output / mode
        case.mkdir()
        fixture = KeyedFixture(case)
        with fixture.managed():
            schema = fixture.name
            queries = {
                'projection': (f'SELECT id AS key,price FROM {schema}.bid', ['key'], ['key', 'price']),
                'inner': (f'SELECT b.id AS key,a.category,b.price FROM {schema}.auction a JOIN {schema}.bid b ON a.id=b.auction', ['key'], ['key', 'category', 'price']),
                'outer': (f'SELECT b.id AS key,a.category,b.price FROM {schema}.bid b LEFT JOIN {schema}.auction a ON b.auction=a.id', ['key'], ['key', 'category', 'price']),
                'pair': (f'SELECT a.id AS aid,b.id AS bid,b.price FROM {schema}.auction a JOIN {schema}.bid b ON a.id-a.id=b.auction-b.auction', ['aid', 'bid'], ['aid', 'bid', 'price']),
            }
            fixture.query, fixture.key, fixture.output_names = queries[mode]
            worker = fixture.start(command, 'initial')
            worker.event('ready')
            fixture.verify()
            for statement in [
                f"BEGIN; UPDATE {schema}.auction SET category='changed' WHERE id=1; UPDATE {schema}.bid SET auction=1,price=coalesce(price,0)+1 WHERE id%3=0; COMMIT",
                f'DELETE FROM {schema}.auction WHERE id=1',
                f"INSERT INTO {schema}.auction VALUES(1,'returned')",
                f'UPDATE {schema}.bid SET id=id+1000 WHERE id<=8',
                f'DELETE FROM {schema}.bid WHERE id%2=0',
            ]:
                fixture.sql(statement)
                worker.event('published')
                fixture.verify()
            worker.finish(graceful=True)
            worker = fixture.start(command, 'restart')
            worker.event('ready')
            fixture.verify()
            fixture.sql(f'DELETE FROM {schema}.bid')
            worker.event('published')
            fixture.verify()
            worker.finish(graceful=True)
            results.append({'mode': mode, 'native_binary_equality': True,
                            'both_input_transaction': True, 'key_move': True,
                            'delete': True, 'restart': True, 'safe_ack': True})
    (output / 'properties.json').write_text(json.dumps(results, indent=2)+'\n')
