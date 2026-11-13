"""Exact civil-calendar and interval oracle checks without a PostgreSQL process."""
import datetime
from decimal import Decimal
import unittest

from temporal_oracle import (Civil, Interval, julian, civil, parse_civil, parse_interval,
    encoded, timestamp_add, timestamp_difference, date_integer, date_difference,
    interval_add, negate, extract, truncate, compare, DAY, HOUR)
from temporal_arithmetic_harness import TemporalFixture, interval_equal, contains, MODES
import json
import pathlib
COUNTS = json.loads((pathlib.Path(__file__).resolve().parents[1]/'tests/fixtures/semantics-manifest.json').read_text())['families']


class TemporalOracleTests(unittest.TestCase):
    def test_julian_conversion_matches_datetime_and_extreme_civil_anchors(self):
        for year in (1,4,100,400,1900,1969,2000,2024,9999):
            for month,day in ((1,1),(2,28),(3,1),(12,31)):
                value = datetime.date(year,month,day)
                self.assertEqual(julian(year,month,day),value.toordinal()+1721425)
                self.assertEqual(civil(julian(year,month,day)),(year,month,day))
        self.assertEqual(civil(0),(-4713,11,24))
        self.assertEqual(encoded(parse_civil('5874897-12-31'),'date'),'5874897-12-31')
        self.assertEqual(encoded(parse_civil('0001-02-29 BC'),'date'),'0001-02-29 BC')

    def test_iso_weeks_and_bc_year_century_decade_truncation(self):
        value = parse_civil('2016-01-01 01:02:03')
        self.assertEqual(extract(value,'stamp','week'),53)
        self.assertEqual(extract(value,'stamp','isoyear'),2015)
        for value,year,decade in [('0001-02-29 BC',-1,0),('0002-03-01 BC',-2,-1),('0011-01-01 BC',-11,-1),('0100-12-31 BC',-100,-10)]:
            parsed = parse_civil(value)
            self.assertEqual(extract(parsed,'date','year'),year)
            self.assertEqual(extract(parsed,'date','decade'),decade)
            self.assertEqual(extract(parsed,'date','century'),-1)
            self.assertEqual(encoded(truncate(parsed,'date','century'),'date'),'0100-01-01 BC')

    def test_calendar_month_clamp_precedes_days_and_microseconds(self):
        value = parse_civil('2024-01-31 00:00:00')
        self.assertEqual(encoded(timestamp_add(value,Interval(1)),'stamp'),'2024-02-29T00:00:00')
        self.assertEqual(encoded(timestamp_add(value,Interval(days=30)),'stamp'),'2024-03-01T00:00:00')
        self.assertEqual(encoded(timestamp_add(parse_civil('2024-02-29'),Interval(12)),'stamp'),'2025-02-28T00:00:00')
        mixed = parse_interval('P-1M2DT-3H-4M-5.123456S')
        self.assertEqual(encoded(mixed,'interval'),'P-1M2DT-3H-4M-5.123456S')
        self.assertEqual(encoded(timestamp_add(parse_civil('2024-03-31'),mixed),'stamp'),'2024-03-01T20:55:54.876544')

    def test_interval_representation_is_distinct_from_equality_and_difference(self):
        month,days,hours = Interval(1),Interval(days=30),Interval(micros=720*HOUR)
        self.assertTrue(interval_equal(month,days))
        self.assertTrue(interval_equal(days,hours))
        self.assertEqual([encoded(value,'interval') for value in (month,days,hours)],['P1M','P30D','PT720H'])
        value = timestamp_difference(parse_civil('2024-01-02 01:00:00'),parse_civil('2024-01-01'))
        self.assertEqual(value,Interval(days=1,micros=HOUR))
        self.assertEqual(encoded(value,'interval'),'P1DT1H')
        self.assertTrue(interval_equal(value,Interval(micros=25*HOUR)))

    def test_extract_decimal_scales_and_native_epoch_overflow_path(self):
        value = parse_civil('1969-12-31 23:59:59.999999')
        for unit,expected,scale in [('epoch','-0.000001',-6),('second','59.999999',-6),('milliseconds','59999.999',-3),('microseconds','59999999',0)]:
            result = extract(value,'stamp',unit)
            self.assertEqual(result,Decimal(expected))
            self.assertEqual(result.as_tuple().exponent,scale)
        self.assertEqual(extract(parse_civil('294276-12-31 23:59:59.999999'),'stamp','epoch'),Decimal('9224318016000.000000'))
        self.assertEqual(extract(parse_civil('294247-01-10 04:00:54.775807'),'stamp','epoch'),Decimal('9223372036854.775800'))
        self.assertEqual(extract(parse_civil('294276-12-31 23:59:59.999499'),'stamp','epoch'),Decimal('9224318015999.999500'))
        self.assertEqual(extract(parse_civil('1970-01-01'),'date','epoch').as_tuple().exponent,0)

    def test_interval_extract_epoch_uses_365_point_25_year_and_signed_fields(self):
        self.assertEqual(extract(Interval(13,1,1),'interval','epoch'),Decimal('34236000.000001'))
        self.assertEqual(extract(Interval(-1),'interval','quarter'),1)
        self.assertEqual(extract(Interval(-4),'interval','quarter'),0)
        self.assertEqual(encoded(truncate(Interval(micros=-1234567),'interval','milliseconds'),'interval'),'PT-1.234S')

    def test_infinities_nulls_and_independent_arithmetic_phase_bounds(self):
        self.assertEqual(extract('infinity','interval','hour'),Decimal('Infinity'))
        self.assertIsNone(extract('infinity','interval','month'))
        self.assertIsNone(extract('-infinity','stamp','second'))
        self.assertEqual(timestamp_add('infinity',Interval(1)),'infinity')
        self.assertEqual(date_integer(None,1),None)
        for interval in [Interval(months=1,days=-31),Interval(days=1,micros=-DAY),Interval(micros=1)]:
            with self.assertRaises(OverflowError):
                timestamp_add(parse_civil('294276-12-31 23:59:59.999999'),interval)
        with self.assertRaises(OverflowError):
            timestamp_add('infinity','-infinity')
        with self.assertRaises(OverflowError):
            timestamp_difference('infinity','infinity')
        with self.assertRaises(OverflowError):
            date_difference('infinity',parse_civil('2000-01-01'))
        with self.assertRaises(OverflowError):
            date_integer(parse_civil('5874897-12-31'),1)
        with self.assertRaises(OverflowError):
            interval_add(Interval(days=2**31-1),Interval(days=1))
        with self.assertRaises(OverflowError):
            negate(Interval(months=-2**31))

    def test_identity_oracle_retracts_one_equal_interval_representation(self):
        fixture = TemporalFixture.__new__(TemporalFixture)
        fixture.mode = 'interval-identities'
        rows = [dict(id=1,iv=Interval(1)),dict(id=16,iv=Interval(micros=720*HOUR)),dict(id=17,iv=Interval(days=30))]
        self.assertEqual(set(fixture.memory(rows)),{('P1M',),('PT720H',),('P30D',)})
        self.assertEqual(set(fixture.memory(rows[1:])),{('PT720H',),('P30D',)})

    def test_date_comparison_does_not_cast_wide_date_to_timestamp(self):
        date = parse_civil('5874897-12-31')
        stamp = parse_civil('294276-12-31 23:59:59.999999')
        self.assertEqual(compare(date,stamp),1)
        self.assertEqual(compare(stamp,date),-1)
        self.assertEqual(compare('infinity','infinity'),0)
        self.assertIsNone(compare(None,stamp))

    def test_mixed_in_uses_full_timestamp_and_three_valued_equality(self):
        date = parse_civil('2024-01-31')
        noon = parse_civil('2024-01-31 12:00:00')
        wide = parse_civil('5874897-12-31')
        self.assertFalse(contains(noon,[date,wide]))
        self.assertTrue(contains(date,[noon,date,None]))
        self.assertIsNone(contains(noon,[date,None]))
        self.assertFalse(contains(None,[]))

    def test_gate_case_count_stays_synchronized(self):
        self.assertEqual(len(MODES),COUNTS['temporal_arithmetic'])
        self.assertEqual(len(MODES),len(set(MODES)))


if __name__ == '__main__':
    unittest.main()
