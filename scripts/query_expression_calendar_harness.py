#!/usr/bin/env python3
"""Reject unsupported UTC EXTRACT expressions before registration or slot creation."""
from engine_rejection import qualify_unavailable
from sql_compiler_harness import SqlFixture
from worker_harness import harness_arguments


class CalendarFixture(SqlFixture):
    def __init__(self, output):
        super().__init__(output,False)
        self.sql(f"ALTER TABLE {self.name}.bid ADD COLUMN date_time timestamptz; UPDATE {self.name}.bid SET date_time='2024-03-10 09:59:59.123456+00'::timestamptz+id*interval '1 hour'")
        self.labels=['year','month','day','minute','second','epoch','week','isoyear','isodow','dow','doy','quarter']
        self.query='SELECT '+','.join(f"extract({field} FROM date_time AT TIME ZONE 'UTC') AS {field}" for field in self.labels)+f' FROM {self.name}.bid'


if __name__ == '__main__':
    output, command = harness_arguments()
    qualify_unavailable(CalendarFixture(output), command)
