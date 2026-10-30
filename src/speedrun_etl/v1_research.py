"""Ограниченные исследовательские запросы публичного v1 /runs.

Использует общий журнал, бюджет и блокировку без API key; не меняет маршруты DAG."""

import base64
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import Request, urlopen

from .jsonutil import loads
from .source import Client, ENDPOINTS


class V1RunsTransport:
    def __init__(self, transport=urlopen):
        self.transport = transport

    def __call__(self, request, timeout=60):
        parts = urlsplit(request.full_url)
        if (
            request.get_method() != "GET"
            or parts.scheme != "https"
            or parts.netloc != "www.speedrun.com"
            or parts.path != "/api/v2/v1/runs"
        ):
            raise ValueError("Research transport only accepts public v1 /runs")
        encoded = parse_qs(parts.query)["_r"][0]
        params = loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
        allowed = {
            "game",
            "category",
            "platform",
            "examiner",
            "status",
            "orderby",
            "direction",
            "max",
            "offset",
            "embed",
        }
        if set(params) - allowed or not params.get("game"):
            raise ValueError("Unexpected v1 research parameters")
        if (
            not 1 <= int(params.get("max", 200)) <= 200
            or int(params.get("offset", 0)) < 0
        ):
            raise ValueError("Invalid v1 research pagination")
        headers = {
            k: v
            for k, v in request.header_items()
            if k.lower() in ("accept", "accept-encoding", "user-agent")
        }
        actual = Request(
            "https://www.speedrun.com/api/v1/runs?" + urlencode(params), headers=headers
        )
        return self.transport(actual, timeout=timeout)


def client(state, collection, snapshot, budget=24, transport=urlopen):
    # The allow-list extension is local to this research process. No DAG configuration
    # or source module on disk is changed; journal endpoint explicitly says v1/runs.
    ENDPOINTS.add("v1/runs")
    return Client(
        state,
        collection,
        snapshot,
        budget=budget,
        transport=V1RunsTransport(transport),
        adaptive_rate=False,
        interval=3.1,
        max_attempts=1,
    )


def validate_runs(saved, params):
    body = saved["body"]
    rows, pagination = body["data"], body["pagination"]
    if not isinstance(rows, list) or len(rows) != int(pagination["size"]):
        raise ValueError("v1 page length differs from pagination")
    if int(pagination["offset"]) != int(params.get("offset", 0)):
        raise ValueError("v1 returned another offset")
    if int(pagination["max"]) != int(params.get("max", 200)) or len(rows) > int(
        pagination["max"]
    ):
        raise ValueError("v1 returned another page limit")
    ids = []
    for row in rows:
        if (
            not row.get("id")
            or row.get("game") != params["game"]
            or (params.get("category") and row.get("category") != params["category"])
            or (
                params.get("status")
                and row.get("status", {}).get("status") != params["status"]
            )
            or (
                params.get("examiner")
                and row.get("status", {}).get("examiner") != params["examiner"]
            )
            or (
                params.get("platform")
                and row.get("system", {}).get("platform") != params["platform"]
            )
        ):
            raise ValueError("v1 ID/game/category/status/platform filter violation")
        ids.append(row["id"])
    if len(ids) != len(set(ids)):
        raise ValueError("v1 repeated IDs within page")
    return ids
