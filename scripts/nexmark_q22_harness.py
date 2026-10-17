#!/usr/bin/env python3
"""Reject unsupported q22 URL splitting across type and join layouts."""
import pathlib
from engine_rejection import qualify_unavailable
from sql_compiler_harness import SqlFixture
from worker_harness import harness_arguments

SQL = pathlib.Path(__file__).resolve().parents[1]/'queries/nexmark/q22.sql'


def literal(text):
    return "E'"+text.replace('\\','\\\\').replace("'","''")+"'"


class Q22Fixture(SqlFixture):
    def __init__(self, output, mode):
        super().__init__(output, False)
        native = 'varchar(256)' if mode == 'varchar' else 'text'
        self.sql(f'ALTER TABLE {self.name}.bid ADD COLUMN bidder bigint, ADD COLUMN channel text, ADD COLUMN url {native}')
        self.query = SQL.read_text().replace('nexmark.bid', f'{self.name}.bid').strip().removesuffix(';')
        if mode == 'delimiter':
            self.query = self.query.replace("'/'", literal("'\\🙂"))
        if mode == 'join':
            self.query = self.query.replace(f'FROM {self.name}.bid b', f'FROM {self.name}.auction a JOIN {self.name}.bid b ON a.id=b.auction WHERE a.enabled OR b.price IS NULL')


if __name__ == '__main__':
    output, command = harness_arguments()
    for mode in ('text', 'varchar', 'delimiter', 'join'):
        directory = output/mode
        directory.mkdir()
        qualify_unavailable(Q22Fixture(directory, mode), command)
