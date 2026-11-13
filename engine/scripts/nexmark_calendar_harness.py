#!/usr/bin/env python3
"""Reject unsupported calendar formatting across native timestamp/text layouts."""
import pathlib
from engine_rejection import qualify_unavailable
from sql_compiler_harness import SqlFixture
from worker_harness import harness_arguments

QUERIES = pathlib.Path(__file__).resolve().parents[1]/'queries/nexmark'


class CalendarFixture(SqlFixture):
    def __init__(self, output, query, mode):
        super().__init__(output, False)
        instant = mode in ('instant', 'icu')
        temporal = 'timestamptz' if instant else 'timestamp'
        channel = 'varchar(128) COLLATE pg_catalog."C"' if mode == 'c' else 'text COLLATE pg_catalog."und-x-icu"' if mode == 'icu' else 'text'
        self.sql(f'ALTER TABLE {self.name}.bid ADD COLUMN date_time {temporal}, ADD COLUMN bidder bigint, ADD COLUMN extra text, ADD COLUMN channel {channel}')
        self.query = (QUERIES/(query+'.sql')).read_text().replace('nexmark.bid', f'{self.name}.bid').strip().removesuffix(';')
        if not instant:
            self.query = self.query.replace(" AT TIME ZONE 'UTC'", '').replace('TIMESTAMPTZ', 'TIMESTAMP').replace('2000-01-01 00:00:00+00', '2000-01-01 00:00:00')


if __name__ == '__main__':
    output, command = harness_arguments()
    for query, modes in [('q10', ('local', 'instant')), ('q16', ('local', 'instant', 'c', 'icu'))]:
        for mode in modes:
            directory = output/(query+'-'+mode)
            directory.mkdir()
            qualify_unavailable(CalendarFixture(directory, query, mode), command)
