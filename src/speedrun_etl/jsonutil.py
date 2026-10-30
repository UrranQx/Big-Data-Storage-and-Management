"""json-v1: exact numbers, sorted object keys, ordered arrays, no float parsing."""
from contextlib import contextmanager
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import tempfile
from datetime import datetime, timezone


def loads(value):
    return json.loads(value, parse_float=Decimal, parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))


def number(value):
    d = Decimal(str(value))
    if not d.is_finite():
        raise ValueError("Non-finite JSON number")
    sign, digits, exp = d.as_tuple()
    digits = list(digits)
    if not any(digits):
        return "0"
    while digits[-1] == 0:
        digits.pop()
        exp += 1
    s = "".join(map(str, digits))
    if 0 <= exp <= 20:
        return ("-" if sign else "") + s + "0" * exp
    # This rule also normalizes 1, 1.0 and 1e0 to the same JSON value.
    return ("-" if sign else "") + s + (f"e{exp}" if exp else "")


def dumps(value):
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
        return "{" + ",".join(dumps(k) + ":" + dumps(value[k]) for k in sorted(value)) + "}"
    raise TypeError(f"Not JSON: {type(value).__name__}")


def digest(value):
    return hashlib.sha256(dumps(value).encode()).hexdigest()


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def read(path):
    return loads(Path(path).read_bytes())


def atomic_write(path, value, durable=True):
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
    import fcntl
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)
