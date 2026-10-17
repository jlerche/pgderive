#!/usr/bin/env python3
"""Reject unsupported q14 compositions before registration or slot creation."""
import pathlib
from engine_rejection import qualify_unavailable
from sql_compiler_harness import SqlFixture
from worker_harness import harness_arguments

SQL = pathlib.Path(__file__).resolve().parents[1]/'queries/nexmark/q14.sql'


class Q14Fixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        temporal = 'timestamp' if mode == 'local' else 'timestamptz'
        self.sql(f'ALTER TABLE {self.name}.bid ADD COLUMN bidder bigint, ADD COLUMN date_time {temporal}, ADD COLUMN extra text')
        self.query = SQL.read_text().replace('nexmark.', self.name+'.').strip().removesuffix(';')
        if mode == 'local':
            self.query = self.query.replace("date_time AT TIME ZONE 'UTC'", 'date_time')


if __name__ == '__main__':
    output, command = harness_arguments()
    for mode in ('local', 'instant'):
        directory = output/mode
        directory.mkdir()
        qualify_unavailable(Q14Fixture(directory, mode), command)
