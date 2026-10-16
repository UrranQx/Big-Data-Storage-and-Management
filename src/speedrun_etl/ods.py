"""Four immutable logical record sets and reversible response decomposition."""
import base64
import copy
import hashlib
from decimal import Decimal
from pathlib import Path
from urllib.parse import quote

from .jsonutil import atomic_write, digest, dumps, loads, read

SETS = ("payloads", "responses", "observations", "manifests")
BLOCK_BYTES = 5 * 1024 * 1024  # base64 document < 8 MiB, also safely below BSON limit
LISTS = {"gameList": "game", "runList": "run", "playerList": "player",
         "platformList": "platform", "categories": "category", "categoryList": "category",
         "variables": "variable", "variableList": "variable", "platforms": "platform",
         "values": "variable_value", "regionList": "region"}
OBJECTS = {"game": "game", "stats": "stats", "gameStats": "stats"}


def native_safe(value):
    """A deliberately conservative common JSONB/BSON subset, with exact fallback."""
    if value is None or isinstance(value, bool):
        return True
    if isinstance(value, int):
        return -(2**63) <= value < 2**63
    if isinstance(value, (float, Decimal)):
        return False  # do not silently round Decimal through double
    if isinstance(value, str):
        return "\x00" not in value and not any(0xD800 <= ord(c) <= 0xDFFF for c in value)
    if isinstance(value, list):
        return all(native_safe(v) for v in value)
    if isinstance(value, dict):
        return all(native_safe(k) and "." not in k and not k.startswith("$") and native_safe(v)
                   for k, v in value.items())
    return False


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
            self.db.execute("CREATE TABLE IF NOT EXISTS records(kind TEXT NOT NULL, key TEXT NOT NULL, document TEXT NOT NULL, PRIMARY KEY(kind,key)) WITHOUT ROWID")
            self.db.commit()
        self._known_payloads = set()
        from functools import lru_cache
        self.resolve = lru_cache(maxsize=32768)(lambda key: resolve_payload(self.get, key))

    def flush(self):
        if self.db is not None:
            self.db.commit()
            self.pending = 0

    def contains(self, kind, key):
        if self.db is not None:
            return self.db.execute("SELECT 1 FROM records WHERE kind=? AND key=?", (kind, key)).fetchone() is not None
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
            previous = self.db.execute("SELECT document FROM records WHERE kind=? AND key=?", (kind, record["key"])).fetchone()
            if previous is not None:
                if previous[0] != raw:
                    raise ValueError(f"Immutable key conflict: {kind}/{record['key']}")
            else:
                self.db.execute("INSERT INTO records VALUES(?,?,?)", (kind, record["key"], raw))
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
            row = self.db.execute("SELECT document FROM records WHERE kind=? AND key=?", (kind, key)).fetchone()
            if row is None:
                raise KeyError((kind, key))
            return loads(row[0])
        return read(self.path(kind, key))

    def records(self, kind):
        if self.db is not None:
            self.flush()
            for row in self.db.execute("SELECT document FROM records WHERE kind=? ORDER BY key", (kind,)):
                yield loads(row[0])
            return
        # Sorting the hashed filenames is deterministic; summary sorts logical keys.
        for path in sorted((self.root / kind).glob("*.json")):
            yield read(path)

    def keys(self, kind):
        if self.db is not None:
            self.flush()
            for row in self.db.execute("SELECT key FROM records WHERE kind=? ORDER BY key", (kind,)):
                yield row[0]
        else:
            yield from sorted(row["key"] for row in self.records(kind))

    def payload_array(self, values):
        """Canonical array built in bounded memory, using the common block scheme."""
        checksum, buffer, blocks = hashlib.sha256(), bytearray(), []
        def append(raw):
            checksum.update(raw)
            buffer.extend(raw)
            while len(buffer) > BLOCK_BYTES:
                block = bytes(buffer[:BLOCK_BYTES])
                del buffer[:BLOCK_BYTES]
                key = "block-v1:" + hashlib.sha256(block).hexdigest()
                self.put("payloads", {"key": key, "encoding": "base64-block", "data": base64.b64encode(block).decode()})
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
            self.put("payloads", {"key": block_key, "encoding": "base64-block", "data": base64.b64encode(block).decode()})
            blocks.append(block_key)
        key = "json-v1:" + checksum.hexdigest()
        self.put("payloads", {"key": key, "encoding": "json-v1-blocks", "blocks": blocks})
        return key

    def payload(self, value):
        raw = dumps(value).encode()
        key = "json-v1:" + hashlib.sha256(raw).hexdigest()
        if key in self._known_payloads:
            return key
        if len(raw) > BLOCK_BYTES:
            blocks = []
            for offset in range(0, len(raw), BLOCK_BYTES):
                block = raw[offset:offset + BLOCK_BYTES]
                block_key = "block-v1:" + hashlib.sha256(block).hexdigest()
                self.put("payloads", {"key": block_key, "encoding": "base64-block",
                                      "data": base64.b64encode(block).decode()})
                blocks.append(block_key)
            record = {"key": key, "encoding": "json-v1-blocks", "blocks": blocks}
        elif native_safe(value):
            record = {"key": key, "encoding": "json", "value": value}
        else:
            record = {"key": key, "encoding": "canonical-json-utf8", "text": raw.decode()}
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
            result[kind] = {"count": len(pairs), "digest": digest([list(p) for p in pairs])}
        return result


def resolve_payload(get, key):
    item = get("payloads", key)
    if item["encoding"] == "json":
        value = item["value"]
    elif item["encoding"] == "canonical-json-utf8":
        value = loads(item["text"])
    elif item["encoding"] == "json-v1-blocks":
        raw = bytearray()
        for k in item["blocks"]:
            block = base64.b64decode(get("payloads", k)["data"])
            if k != "block-v1:" + hashlib.sha256(block).hexdigest():
                raise ValueError("Block digest mismatch")
            raw.extend(block)
        value = loads(raw)
    else:
        raise ValueError("Not a JSON payload")
    if key != "json-v1:" + digest(value):
        raise ValueError("Payload digest mismatch")
    return value


def at_path(value, path):
    for part in path:
        value = value[part]
    return value


def restore(get, response, resolver=None):
    resolve = resolver or (lambda key: resolve_payload(get, key))
    envelope = copy.deepcopy(resolve(response["envelope"]))
    for ref in response["references"]:
        value = resolve(ref["payload"])
        if not ref["path"]:
            envelope = value
        else:
            at_path(envelope, ref["path"][:-1])[ref["path"][-1]] = value
    if digest(envelope) != response["body_digest"]:
        raise ValueError("Restored response digest mismatch")
    return envelope


def pack_response(package, staged, snapshot_id, context):
    body = staged["body"]
    envelope = copy.deepcopy(body)
    references = []
    observations = []
    if isinstance(body, dict):
        for field, value in body.items():
            items = []
            if field in LISTS and isinstance(value, list):
                items = [([field, i], LISTS[field], v) for i, v in enumerate(value) if isinstance(v, dict)]
            elif field in OBJECTS and isinstance(value, dict):
                items = [([field], OBJECTS[field], value)]
            for path, entity_type, entity in items:
                payload = package.payload(entity)
                references.append({"path": path, "payload": payload})
                at_path(envelope, path[:-1])[path[-1]] = None
                owner = context.get("game_id", staged["endpoint"])
                source_id = str(entity.get("id") or (owner + ":" + "/".join(map(str, path))))
                key = digest([staged["response_id"], entity_type, path])
                observations.append({"key": key, "source_system": "speedruncom", "api_variant": "v2",
                    "snapshot_id": snapshot_id, "entity_type": entity_type, "source_id": source_id,
                    "payload": payload, "response_id": staged["response_id"], "path": path,
                    "observed_at": staged["fetched_at"], "context": context})
    response = {"key": staged["response_id"], "snapshot_id": snapshot_id, "api_variant": "v2",
                "source_system": "speedruncom", "endpoint": staged["endpoint"], "params": staged["params"],
                "requested_at": staged["requested_at"], "fetched_at": staged["fetched_at"],
                "http_status": staged["http_status"], "attempt": staged["attempt"],
                "network_bytes": staged["network_bytes"], "body_digest": digest(body),
                "error": staged.get("error"),
                "envelope": package.payload(envelope), "references": references,
                "diagnostics": [] if references else ["unrecognized_or_scalar_shape_preserved"]}
    package.put("responses", response)
    for record in observations:
        package.put("observations", record)
    if dumps(restore(package.get, response, resolver=package.resolve)) != dumps(body):
        raise ValueError("Response reconstruction failed")
    return response


def s3_key(kind, record):
    if kind == "observations":
        # Entity-prefix history without a SQL engine. Quote every source component.
        parts = [record["source_system"], record["entity_type"], record["source_id"],
                 record["response_id"], record["key"]]
        return kind + "/" + "/".join(quote(p, safe="") for p in parts) + ".json"
    return kind + "/" + quote(record["key"], safe="") + ".json"
