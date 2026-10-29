"""Independent Decimal oracle for bounded PostgreSQL numeric arithmetic tests."""
from decimal import Decimal, InvalidOperation, localcontext, ROUND_HALF_UP, ROUND_DOWN, ROUND_CEILING, ROUND_FLOOR


def number(value):
    return None if value is None else Decimal(value)


def clean(value):
    if value is None:
        return None
    return str(value) if not value.is_finite() else abs(value) if value == 0 else value


def division(left, right):
    if left is None or right is None:
        return None
    if left.is_nan() or right.is_nan():
        return Decimal('NaN')
    if right == 0:
        raise ZeroDivisionError('division by zero')
    if left.is_infinite():
        if right.is_infinite():
            return Decimal('NaN')
        return Decimal('-Infinity' if left.is_signed() != right.is_signed() else 'Infinity')
    if right.is_infinite():
        return Decimal(0)
    def leading(value):
        if value == 0:
            return 0, 0
        digits = value.as_tuple().digits
        exponent = value.as_tuple().exponent
        adjusted = len(digits) + exponent - 1
        weight = adjusted // 4
        width = adjusted - 4 * weight + 1
        prefix = ''.join(map(str, digits[:width])).ljust(width, '0')
        return weight, int(prefix)
    w1, d1 = leading(left)
    w2, d2 = leading(right)
    scale = min(1000, max(0, -left.as_tuple().exponent, -right.as_tuple().exponent,
                          16 - 4 * (w1 - w2 - int(d1 <= d2))))
    with localcontext() as context:
        context.prec = 10000
        return (left / right).quantize(Decimal(1).scaleb(-scale), rounding=ROUND_HALF_UP)


def rounding(value, places=0, mode='round'):
    if value is None or places is None or not value.is_finite():
        return value if places is not None else None
    rule = {'round': ROUND_HALF_UP, 'trunc': ROUND_DOWN,
            'ceil': ROUND_CEILING, 'ceiling': ROUND_CEILING, 'floor': ROUND_FLOOR}[mode]
    with localcontext() as context:
        context.prec = 10000
        result = value.quantize(Decimal(1).scaleb(-places), rounding=rule)
        return result.quantize(Decimal(1)) if places < 0 else result


def cast(value, precision, scale):
    if value is None or value.is_nan():
        return value
    if value.is_infinite():
        raise OverflowError('numeric field overflow')
    result = rounding(value, scale)
    if result.copy_abs() >= Decimal(10) ** (precision - scale):
        raise OverflowError('numeric field overflow')
    return result


def average(values):
    values = [value for value in values if value is not None]
    if not values:
        return None
    with localcontext() as context:
        context.prec = 10000
        context.traps[InvalidOperation] = False
        return division(sum(values, Decimal(0)), Decimal(len(values)))
