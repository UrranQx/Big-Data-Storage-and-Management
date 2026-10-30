"""Канонический JSON с точными числами, digests и устойчивой записью ETL.

Порядок ключей нормализуется; массивы сохраняют порядок. Блокировки рассчитаны на Linux."""

from contextlib import contextmanager
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import tempfile
from datetime import datetime, timezone


def loads(value):
    """Читает JSON с Decimal; не допускает NaN/Infinity."""
    return json.loads(
        value,
        parse_float=Decimal,
        parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)),
    )


def number(value):
    """Нормализует конечное число без потери точности для ключей и digests."""
    decimal_value = Decimal(str(value))
    if not decimal_value.is_finite():
        raise ValueError("Non-finite JSON number")
    sign, digits, exponent = decimal_value.as_tuple()
    digits = list(digits)
    if not any(digits):
        return "0"
    while digits[-1] == 0:
        digits.pop()
        exponent += 1
    significand = "".join(map(str, digits))
    if 0 <= exponent <= 20:
        return ("-" if sign else "") + significand + "0" * exponent
    # This rule also normalizes 1, 1.0 and 1e0 to the same JSON value.
    return ("-" if sign else "") + significand + (f"e{exponent}" if exponent else "")


def dumps(value):
    """Сериализует канонический JSON; неизвестные типы отклоняются."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, Decimal, float)):
        return number(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=True)
    if isinstance(value, list):
        return "[" + ",".join(dumps(v) for v in value) + "]"
    if isinstance(value, dict) and all(isinstance(k, str) for k in value):
        return (
            "{"
            + ",".join(dumps(k) + ":" + dumps(value[k]) for k in sorted(value))
            + "}"
        )
    raise TypeError(f"Not JSON: {type(value).__name__}")


def digest(value):
    return hashlib.sha256(dumps(value).encode()).hexdigest()


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def read(path):
    return loads(Path(path).read_bytes())


def atomic_write(path, value, durable=True):
    """Атомарно заменяет JSON-файл; durable синхронизирует файл и каталог."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".pending-")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(dumps(value))
            handle.flush()
            if durable:
                os.fsync(handle.fileno())
        os.replace(temporary, path)
        if durable and os.name == "posix":
            directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


@contextmanager
def file_lock(path, blocking=True):
    # Production always runs on the same Linux named volume, never on SMB.
    """Удерживает межпроцессную Linux-блокировку на время операции ETL."""
    import fcntl

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)
