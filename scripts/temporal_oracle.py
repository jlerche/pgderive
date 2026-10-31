"""Independent Gregorian/interval arithmetic with integer microseconds and Decimal."""
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP, localcontext
import re

from numeric_arithmetic_oracle import division

SECOND = 1_000_000
MINUTE = 60*SECOND
HOUR = 60*MINUTE
DAY = 24*HOUR
UNIX_JD = 2440588


def julian(year, month, day):
    shift = (14-month)//12
    adjusted = year+4800-shift
    return day+(153*(month+12*shift-3)+2)//5+365*adjusted+adjusted//4-adjusted//100+adjusted//400-32045


def civil(day):
    # Julian-day inversion, independent of the engine's civil-era conversion.
    a = day+32044
    b = (4*a+3)//146097
    c = a-146097*b//4
    d = (4*c+3)//1461
    e = c-1461*d//4
    m = (5*e+2)//153
    return 100*b+d-4800+m//10, m+3-12*(m//10), e-(153*m+2)//5+1


def tdiv(value, divisor):
    return -(abs(value)//divisor) if value < 0 else value//divisor


def decimal(value, scale=0):
    with localcontext() as context:
        context.prec = 80
        return Decimal(value).scaleb(-scale)


def special(value):
    return isinstance(value,str) and value in ('infinity','-infinity')


def compare(left, right):
    if left is None or right is None:
        return None
    if special(left) or special(right):
        rank = lambda value:0 if value == '-infinity' else 2 if value == 'infinity' else 1
        return (rank(left)>rank(right))-(rank(left)<rank(right))
    def value(part):
        return part.months*30*DAY+part.days*DAY+part.micros if isinstance(part,Interval) else (part.day,part.micros)
    first,second = value(left),value(right)
    return (first>second)-(first<second)


def opposite(value):
    return '-infinity' if value == 'infinity' else 'infinity'


@dataclass(frozen=True)
class Civil:
    day: int
    micros: int = 0


@dataclass(frozen=True)
class Interval:
    months: int = 0
    days: int = 0
    micros: int = 0


def parse_civil(value):
    if value is None or special(value):
        return value
    bc = value.endswith(' BC')
    value = value.removesuffix(' BC')
    date,*clock = re.split('[ T]',value,maxsplit=1)
    year,month,day = map(int,date.split('-'))
    year = 1-year if bc else year
    micros = 0
    if clock:
        match = re.fullmatch(r'(\d+):(\d+):(\d+)(?:\.(\d+))?(?:([+-])(\d+)(?::(\d+))?(?::(\d+))?|Z)?',clock[0])
        if not match:
            raise ValueError('noncanonical timestamp '+value)
        hour,minute,second,fraction,sign,zh,zm,zs = match.groups()
        micros = (int(hour)*3600+int(minute)*60+int(second))*SECOND+int((fraction or '').ljust(6,'0') or 0)
        if sign:
            offset = (int(zh)*3600+int(zm or 0)*60+int(zs or 0))*SECOND
            micros -= offset if sign == '+' else -offset
    carry,micros = divmod(micros,DAY)
    return Civil(julian(year,month,day)+carry,micros)


def parse_interval(value):
    if value is None or special(value):
        return value
    match = re.fullmatch(r'P(?:(?P<y>[+-]?\d+)Y)?(?:(?P<m>[+-]?\d+)M)?(?:(?P<d>[+-]?\d+)D)?(?:T(?:(?P<h>[+-]?\d+)H)?(?:(?P<n>[+-]?\d+)M)?(?:(?P<s>[+-]?\d+(?:\.\d+)?)S)?)?',value)
    if not match:
        raise ValueError('noncanonical interval '+value)
    parts = {key:Decimal(value or 0) for key,value in match.groupdict().items()}
    return Interval(int(parts['y']*12+parts['m']),int(parts['d']),int((parts['h']*3600+parts['n']*60+parts['s'])*SECOND))


def encoded(value, kind):
    if value is None or special(value):
        return value
    if kind == 'interval':
        years = tdiv(value.months,12)
        months = value.months-years*12
        hours = tdiv(value.micros,HOUR)
        remainder = value.micros-hours*HOUR
        minutes = tdiv(remainder,MINUTE)
        seconds = remainder-minutes*MINUTE
        date = ''.join(str(part)+unit for part,unit in ((years,'Y'),(months,'M'),(value.days,'D')) if part)
        time = ''.join(str(part)+unit for part,unit in ((hours,'H'),(minutes,'M')) if part)
        if seconds:
            time += format(decimal(seconds,6),'f').rstrip('0').rstrip('.')+'S'
        return 'P'+date+('T'+time if time else '') if date or time else 'PT0S'
    year,month,day = civil(value.day)
    bc = year <= 0
    date = f'{1-year if bc else year:04}-{month:02}-{day:02}'
    suffix = ' BC' if bc else ''
    if kind == 'date':
        return date+suffix
    hour,remainder = divmod(value.micros,HOUR)
    minute,remainder = divmod(remainder,MINUTE)
    second,micro = divmod(remainder,SECOND)
    fraction = (f'.{micro:06}'.rstrip('0')) if micro else ''
    return f'{date}T{hour:02}:{minute:02}:{second:02}{fraction}'+('+00:00' if kind == 'instant' else '')+suffix


def checked_interval(value):
    if not (-2**31 <= value.months < 2**31 and -2**31 <= value.days < 2**31 and -2**63 <= value.micros < 2**63):
        raise OverflowError('interval out of range')
    return value


def negate(value):
    if value is None:
        return None
    if special(value):
        return opposite(value)
    return checked_interval(Interval(-value.months,-value.days,-value.micros))


def interval_add(left, right, subtract=False):
    if left is None or right is None:
        return None
    right = negate(right) if subtract else right
    if special(left) or special(right):
        if special(left) and special(right) and left != right:
            raise OverflowError('opposite infinite intervals')
        return left if special(left) else right
    return checked_interval(Interval(left.months+right.months,left.days+right.days,left.micros+right.micros))


def date_integer(value, days):
    if value is None or days is None or special(value):
        return value if days is not None else None
    result = Civil(value.day+days)
    if not 0 <= result.day < julian(5874898,1,1):
        raise OverflowError('date out of range')
    return result


def date_difference(left, right):
    if left is None or right is None:
        return None
    if special(left) or special(right):
        raise OverflowError('cannot subtract infinite dates')
    return left.day-right.day


def timestamp_add(value, interval, subtract=False):
    if value is None or interval is None:
        return None
    interval = negate(interval) if subtract else interval
    if special(value) or special(interval):
        if special(value) and special(interval) and value != interval:
            raise OverflowError('opposite infinite temporal values')
        return value if special(value) else interval
    end = julian(294277,1,1)
    if not 0 <= value.day < end:
        raise OverflowError('date outside timestamp range')
    year,month,day = civil(value.day)
    year,month = divmod(year*12+month-1+interval.months,12)
    month += 1
    following = julian(year+1,1,1) if month == 12 else julian(year,month+1,1)
    day = min(day,following-julian(year,month,1))
    month_day = julian(year,month,day)
    if not 0 <= month_day < end:
        raise OverflowError('timestamp month phase out of range')
    calendar_day = month_day+interval.days
    if not 0 <= calendar_day < end:
        raise OverflowError('timestamp day phase out of range')
    carry,micros = divmod(value.micros+interval.micros,DAY)
    result = Civil(calendar_day+carry,micros)
    if not 0 <= result.day < end:
        raise OverflowError('timestamp out of range')
    return result


def timestamp_difference(left, right):
    if left is None or right is None:
        return None
    if special(left) or special(right):
        if left == right:
            raise OverflowError('subtracting equal infinities')
        return left if special(left) else opposite(right)
    micros = (left.day-right.day)*DAY+left.micros-right.micros
    if not -2**63 <= micros < 2**63:
        raise OverflowError('timestamp difference out of range')
    days = tdiv(micros,DAY)
    return checked_interval(Interval(0,days,micros-days*DAY))


def extract(value, kind, unit):
    if value is None:
        return None
    if special(value):
        increasing = {'year','decade','century','millennium','epoch'}
        increasing |= {'hour','day'} if kind == 'interval' else {'julian','isoyear'}
        return Decimal('-Infinity' if value.startswith('-') else 'Infinity') if unit in increasing else None
    if kind == 'interval':
        year = tdiv(value.months,12)
        month = value.months-year*12
        hour = tdiv(value.micros,HOUR)
        minute = tdiv(value.micros-hour*HOUR,MINUTE)
        second = value.micros-hour*HOUR-minute*MINUTE
        units = dict(year=year,month=month,day=value.days,hour=hour,minute=minute,
                     quarter=tdiv(month,3)+1,decade=tdiv(year,10),century=tdiv(year,100),millennium=tdiv(year,1000),
                     microseconds=second,milliseconds=decimal(second,3),second=decimal(second,6),
                     epoch=decimal((year*31557600+month*2592000+value.days*86400)*SECOND+value.micros,6))
        return Decimal(units[unit])
    year,month,day = civil(value.day)
    thursday = value.day+3-value.day%7
    iso_year = civil(thursday)[0]
    first_monday = julian(iso_year,1,4)-julian(iso_year,1,4)%7
    units = dict(year=year if year > 0 else year-1,month=month,day=day,quarter=(month-1)//3+1,
                 decade=year//10,century=(year+99)//100 if year > 0 else -((100-year)//100),
                 millennium=(year+999)//1000 if year > 0 else -((1000-year)//1000),
                 week=(value.day-first_monday)//7+1,isoyear=iso_year if iso_year > 0 else iso_year-1,
                 isodow=value.day%7+1,dow=(value.day+1)%7,doy=value.day-julian(year,1,1)+1,
                 hour=value.micros//HOUR,minute=(value.micros//MINUTE)%60,
                 microseconds=value.micros%MINUTE,milliseconds=decimal(value.micros%MINUTE,3),
                 second=decimal(value.micros%MINUTE,6),timezone=0,timezone_hour=0,timezone_minute=0)
    if unit == 'epoch':
        if kind == 'date':
            return Decimal((value.day-UNIX_JD)*86400)
        micros = (value.day-UNIX_JD)*DAY+value.micros
        timestamp = (value.day-julian(2000,1,1))*DAY+value.micros
        if timestamp >= 2**63-1-946684800*SECOND:
            # PostgreSQL's overflow-avoidance path divides numerics first,
            # then rounds to six places; its initial scale can lose a carry.
            with localcontext() as context:
                context.prec = 80
                return division(Decimal(micros),Decimal(SECOND)).quantize(Decimal('0.000001'),rounding=ROUND_HALF_UP)
        return decimal(micros,6)
    if unit == 'julian':
        with localcontext() as context:
            context.prec = 80
            return Decimal(value.day) if kind == 'date' else Decimal(value.day)+division(Decimal(value.micros),Decimal(DAY))
    return Decimal(units[unit])


def truncate(value, kind, unit):
    if value is None or special(value):
        return value
    if kind == 'interval':
        if unit in ('millennium','century','decade','year'):
            factor = {'millennium':12000,'century':1200,'decade':120,'year':12}[unit]
            return Interval(tdiv(value.months,factor)*factor)
        if unit in ('quarter','month'):
            return Interval(tdiv(value.months,3)*3 if unit == 'quarter' else value.months)
        factor = {'day':None,'hour':HOUR,'minute':MINUTE,'second':SECOND,'milliseconds':1000,'microseconds':1}[unit]
        return Interval(value.months,value.days,tdiv(value.micros,factor)*factor if factor else 0)
    year,month,day = civil(value.day)
    if unit == 'week':
        result = Civil(value.day-value.day%7)
    elif unit in ('millennium','century','decade','year','quarter','month'):
        if unit in ('millennium','century'):
            factor = 1000 if unit == 'millennium' else 100
            year = ((year-1)//factor)*factor+1 if year > 0 else -((factor-year)//factor)*factor+1
        elif unit == 'decade':
            year = (year//10)*10
        if unit in ('millennium','century','decade','year'):
            month = 1
        elif unit == 'quarter':
            month = ((month-1)//3)*3+1
        result = Civil(julian(year,month,1))
    else:
        factor = {'day':DAY,'hour':HOUR,'minute':MINUTE,'second':SECOND,'milliseconds':1000,'microseconds':1}[unit]
        result = Civil(value.day,(value.micros//factor)*factor)
    if not 0 <= result.day < julian(294277,1,1):
        raise OverflowError('truncated timestamp out of range')
    return result
