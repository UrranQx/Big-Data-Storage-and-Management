"""Локальный пакет прежнего ODS: файлы/SQLite и immutable payloads."""

import base64
import hashlib
from pathlib import Path

from .. import ods
from ..jsoncodec import native_safe
from ..jsonutil import atomic_write, digest, dumps, loads, read
from ..ods import SETS, resolve_payload


class Package:
    def __init__(self, root, compact=False):
        self.root = Path(root)
        self.db = None
        self.pending = 0
        database = self.root / "records.sqlite3"
        if database.exists() or compact:
            import sqlite3

            self.root.mkdir(parents=True, exist_ok=True)
            if compact and any((self.root / kind).exists() for kind in SETS):
                raise ValueError("Existing file package cannot be converted in place")
            self.db = sqlite3.connect(database)
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("PRAGMA synchronous=NORMAL")
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS records(kind TEXT NOT NULL, key TEXT NOT NULL, document TEXT NOT NULL, PRIMARY KEY(kind,key)) WITHOUT ROWID"
            )
            self.db.commit()
        self._known_payloads = set()
        from functools import lru_cache

        self.resolve = lru_cache(maxsize=32768)(
            lambda key: resolve_payload(self.get, key)
        )

    def flush(self):
        if self.db is not None:
            self.db.commit()
            self.pending = 0

    def contains(self, kind, key):
        if self.db is not None:
            return (
                self.db.execute(
                    "SELECT 1 FROM records WHERE kind=? AND key=?", (kind, key)
                ).fetchone()
                is not None
            )
        return self.path(kind, key).exists()

    def path(self, kind, key):
        if kind not in SETS:
            raise ValueError(kind)
        return self.root / kind / (hashlib.sha256(key.encode()).hexdigest() + ".json")

    def put(self, kind, record):
        if kind not in SETS:
            raise ValueError(kind)
        if self.db is not None:
            raw = dumps(record)
            previous = self.db.execute(
                "SELECT document FROM records WHERE kind=? AND key=?",
                (kind, record["key"]),
            ).fetchone()
            if previous is not None:
                if previous[0] != raw:
                    raise ValueError(f"Immutable key conflict: {kind}/{record['key']}")
            else:
                self.db.execute(
                    "INSERT INTO records VALUES(?,?,?)", (kind, record["key"], raw)
                )
                self.pending += 1
                if self.pending >= 1000:
                    self.flush()
            return record["key"]
        path = self.path(kind, record["key"])
        if path.exists():
            try:
                previous = read(path)
            except (ValueError, UnicodeError):
                # Only derived package artifacts: regenerate a torn file from
                # the durable response journal after a host crash.
                atomic_write(path, record, durable=False)
                return record["key"]
            if dumps(previous) != dumps(record):
                raise ValueError(f"Immutable key conflict: {kind}/{record['key']}")
        else:
            atomic_write(path, record, durable=False)
        return record["key"]

    def get(self, kind, key):
        if self.db is not None:
            row = self.db.execute(
                "SELECT document FROM records WHERE kind=? AND key=?", (kind, key)
            ).fetchone()
            if row is None:
                raise KeyError((kind, key))
            return loads(row[0])
        return read(self.path(kind, key))

    def records(self, kind):
        if self.db is not None:
            self.flush()
            for row in self.db.execute(
                "SELECT document FROM records WHERE kind=? ORDER BY key", (kind,)
            ):
                yield loads(row[0])
            return
        # Sorting the hashed filenames is deterministic; summary sorts logical keys.
        for path in sorted((self.root / kind).glob("*.json")):
            yield read(path)

    def keys(self, kind):
        if self.db is not None:
            self.flush()
            for row in self.db.execute(
                "SELECT key FROM records WHERE kind=? ORDER BY key", (kind,)
            ):
                yield row[0]
        else:
            yield from sorted(row["key"] for row in self.records(kind))

    def payload_array(self, values):
        """Canonical array built in bounded memory, using the common block scheme."""
        checksum, buffer, blocks = hashlib.sha256(), bytearray(), []

        def append(raw):
            checksum.update(raw)
            buffer.extend(raw)
            while len(buffer) > ods.BLOCK_BYTES:
                block = bytes(buffer[: ods.BLOCK_BYTES])
                del buffer[: ods.BLOCK_BYTES]
                key = "block-v1:" + hashlib.sha256(block).hexdigest()
                self.put(
                    "payloads",
                    {
                        "key": key,
                        "encoding": "base64-block",
                        "data": base64.b64encode(block).decode(),
                    },
                )
                blocks.append(key)

        append(b"[")
        for index, value in enumerate(values):
            if index:
                append(b",")
            append(dumps(value).encode())
        append(b"]")
        if not blocks:
            return self.payload(loads(bytes(buffer)))
        if buffer:
            block = bytes(buffer)
            block_key = "block-v1:" + hashlib.sha256(block).hexdigest()
            self.put(
                "payloads",
                {
                    "key": block_key,
                    "encoding": "base64-block",
                    "data": base64.b64encode(block).decode(),
                },
            )
            blocks.append(block_key)
        key = "json-v1:" + checksum.hexdigest()
        self.put(
            "payloads", {"key": key, "encoding": "json-v1-blocks", "blocks": blocks}
        )
        return key

    def payload(self, value):
        raw = dumps(value).encode()
        key = "json-v1:" + hashlib.sha256(raw).hexdigest()
        if key in self._known_payloads:
            return key
        if len(raw) > ods.BLOCK_BYTES:
            blocks = []
            for offset in range(0, len(raw), ods.BLOCK_BYTES):
                block = raw[offset : offset + ods.BLOCK_BYTES]
                block_key = "block-v1:" + hashlib.sha256(block).hexdigest()
                self.put(
                    "payloads",
                    {
                        "key": block_key,
                        "encoding": "base64-block",
                        "data": base64.b64encode(block).decode(),
                    },
                )
                blocks.append(block_key)
            record = {"key": key, "encoding": "json-v1-blocks", "blocks": blocks}
        elif native_safe(value):
            record = {"key": key, "encoding": "json", "value": value}
        else:
            record = {
                "key": key,
                "encoding": "canonical-json-utf8",
                "text": raw.decode(),
            }
        self.put("payloads", record)
        self._known_payloads.add(key)
        return key

    def summary(self, kinds=SETS):
        result = {}
        for kind in kinds:
            if self.db is not None:
                checksum, count = hashlib.sha256(b"["), 0
                for row in self.records(kind):
                    if count:
                        checksum.update(b",")
                    checksum.update(dumps([row["key"], digest(row)]).encode())
                    count += 1
                checksum.update(b"]")
                result[kind] = {"count": count, "digest": checksum.hexdigest()}
                continue
            pairs = sorted((r["key"], digest(r)) for r in self.records(kind))
            result[kind] = {
                "count": len(pairs),
                "digest": digest([list(p) for p in pairs]),
            }
        return result
