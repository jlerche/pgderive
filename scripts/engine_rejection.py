"""Qualify previously delegated SQL as a specific engine-only rejection."""
import json


def qualify_unavailable(fixture, command):
    try:
        fixture.rejected(command, 'engine-unavailable', fixture.query)
        error = (fixture.output/'engine-unavailable'/'stderr.log').read_text()
        assert 'not available in-engine' in error or 'unsupported' in error, error
        assert fixture.sql(f"SELECT to_regclass('{fixture.name}.pgderive_worker_registration') IS NULL").strip() == 't'
        assert fixture.sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE '{fixture.name}%'").strip() == '0'
        (fixture.output/'result.json').write_text(json.dumps({
            'query':fixture.query, 'qualification':'engine-only rejection',
            'reason':error, 'registration':'absent', 'slot':'absent',
        })+'\n')
        fixture.sql(f'DROP PUBLICATION {fixture.name}; DROP SCHEMA {fixture.name} CASCADE')
    except BaseException:
        fixture.preserve()
        raise
