"""Консервативный общий JSONB/BSON subset с точным fallback."""

from decimal import Decimal


def native_safe(value):
    """A deliberately conservative common JSONB/BSON subset, with exact fallback."""
    if value is None or isinstance(value, bool):
        return True
    if isinstance(value, int):
        return -(2**63) <= value < 2**63
    if isinstance(value, (float, Decimal)):
        return False  # do not silently round Decimal through double
    if isinstance(value, str):
        return "\x00" not in value and not any(
            0xD800 <= ord(c) <= 0xDFFF for c in value
        )
    if isinstance(value, list):
        return all(native_safe(v) for v in value)
    if isinstance(value, dict):
        return all(
            native_safe(k) and "." not in k and not k.startswith("$") and native_safe(v)
            for k, v in value.items()
        )
    return False
