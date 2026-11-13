#!/usr/bin/env python3
"""Reject unsupported q21 text compositions across native collation layouts."""
import pathlib
from engine_rejection import qualify_unavailable
from sql_compiler_harness import SqlFixture
from worker_harness import harness_arguments

SQL = pathlib.Path(__file__).resolve().parents[1]/'queries/nexmark/q21.sql'


class Q21Fixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        collation = 'pg_catalog."C"' if mode == 'c' else 'pg_catalog."default"'
        if mode == 'icu':
            self.sql(f"CREATE COLLATION {self.name}.unicode(provider=icu,locale='und',deterministic=true)")
            collation = f'{self.name}.unicode'
        text = 'text' if mode == 'default' else 'varchar(256)'
        self.sql(f'ALTER TABLE {self.name}.bid ADD COLUMN bidder bigint, ADD COLUMN channel {text} COLLATE {collation}, ADD COLUMN url {text} COLLATE {collation}')
        self.query = SQL.read_text().replace('nexmark.', self.name+'.').strip().removesuffix(';')


if __name__ == '__main__':
    output, command = harness_arguments()
    for mode in ('default', 'c', 'icu'):
        directory = output/mode
        directory.mkdir()
        qualify_unavailable(Q21Fixture(directory, mode), command)
