"""Exact durations shared by the warehouse and the historical benchmarks."""

import re
from decimal import Decimal, localcontext


def duration_ns(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d+(?:\.\d+)?s", value):
        return None
    with localcontext() as context:
        context.prec = max(28, len(value) + 12)
        number = Decimal(value[:-1]) * 10**9
    if number != number.to_integral_value() or number > 2**63 - 1:
        return None
    return int(number)
