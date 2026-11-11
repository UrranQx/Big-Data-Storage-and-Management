"""Прежний ODS: четыре immutable-набора и обратимое разложение ответов.

Локальные пакеты сохраняют точные поля и историю; текущие DAG используют entity_ods."""

import base64
import copy
import hashlib
from urllib.parse import quote
from typing import TYPE_CHECKING

from .jsoncodec import native_safe as native_safe
from .jsonutil import digest, dumps, loads

__all__ = [
    "SETS",
    "BLOCK_BYTES",
    "LISTS",
    "OBJECTS",
    "native_safe",
    "Package",
    "resolve_payload",
    "at_path",
    "restore",
    "pack_response",
    "s3_key",
]

if TYPE_CHECKING:
    from .legacy.package import Package

SETS = ("payloads", "responses", "observations", "manifests")
BLOCK_BYTES = 5 * 1024 * 1024  # base64 document < 8 MiB, also safely below BSON limit
LISTS = {
    "gameList": "game",
    "runList": "run",
    "playerList": "player",
    "platformList": "platform",
    "categories": "category",
    "categoryList": "category",
    "variables": "variable",
    "variableList": "variable",
    "platforms": "platform",
    "values": "variable_value",
    "regionList": "region",
}
OBJECTS = {"game": "game", "stats": "stats", "gameStats": "stats"}


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
                items = [
                    ([field, i], LISTS[field], v)
                    for i, v in enumerate(value)
                    if isinstance(v, dict)
                ]
            elif field in OBJECTS and isinstance(value, dict):
                items = [([field], OBJECTS[field], value)]
            for path, entity_type, entity in items:
                payload = package.payload(entity)
                references.append({"path": path, "payload": payload})
                at_path(envelope, path[:-1])[path[-1]] = None
                owner = context.get("game_id", staged["endpoint"])
                source_id = str(
                    entity.get("id") or (owner + ":" + "/".join(map(str, path)))
                )
                key = digest([staged["response_id"], entity_type, path])
                observations.append(
                    {
                        "key": key,
                        "source_system": "speedruncom",
                        "api_variant": "v2",
                        "snapshot_id": snapshot_id,
                        "entity_type": entity_type,
                        "source_id": source_id,
                        "payload": payload,
                        "response_id": staged["response_id"],
                        "path": path,
                        "observed_at": staged["fetched_at"],
                        "context": context,
                    }
                )
    response = {
        "key": staged["response_id"],
        "snapshot_id": snapshot_id,
        "api_variant": "v2",
        "source_system": "speedruncom",
        "endpoint": staged["endpoint"],
        "params": staged["params"],
        "requested_at": staged["requested_at"],
        "fetched_at": staged["fetched_at"],
        "http_status": staged["http_status"],
        "attempt": staged["attempt"],
        "network_bytes": staged["network_bytes"],
        "body_digest": digest(body),
        "error": staged.get("error"),
        "envelope": package.payload(envelope),
        "references": references,
        "diagnostics": [] if references else ["unrecognized_or_scalar_shape_preserved"],
    }
    package.put("responses", response)
    for record in observations:
        package.put("observations", record)
    if dumps(restore(package.get, response, resolver=package.resolve)) != dumps(body):
        raise ValueError("Response reconstruction failed")
    return response


def s3_key(kind, record):
    if kind == "observations":
        # Entity-prefix history without a SQL engine. Quote every source component.
        parts = [
            record["source_system"],
            record["entity_type"],
            record["source_id"],
            record["response_id"],
            record["key"],
        ]
        return kind + "/" + "/".join(quote(p, safe="") for p in parts) + ".json"
    return kind + "/" + quote(record["key"], safe="") + ".json"


def __getattr__(name):
    # Lazy import avoids a cycle: Package uses the public codecs above.
    if name == "Package":
        from .legacy.package import Package

        return Package
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
