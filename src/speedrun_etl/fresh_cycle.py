"""Bounded post-reset sequence: fresh Half-Life, directory, then Minecraft."""
import os
from pathlib import Path
import sys
import time
from urllib.error import HTTPError

from .cli import api
from .entity_storage import ready
from .jsonutil import utcnow, file_lock

HALF_LIFE = "half-life-v2-fresh-20261016-001"
CATALOG = "catalog-v2-fresh-20261016-001"
MINECRAFT = "minecraft-v2-fresh-20261016-001"


def token():
    return api("POST", "/auth/token", {"username": "admin", "password": os.environ["AIRFLOW_ADMIN_PASSWORD"]})["access_token"]


def wait(collection, kinds):
    previous = None
    while True:
        states = {}
        auth = token()
        for kind in kinds:
            try:
                states[kind] = api("GET", f"/api/v2/dags/{kind}/dagRuns/{collection}-{kind}", token=auth)["state"]
            except HTTPError as error:
                if error.code != 404:
                    raise
                states[kind] = "not_created"
        if states != previous:
            print(utcnow(), collection, states, flush=True)
            previous = states
        if "failed" in states.values():
            raise RuntimeError("Fresh cycle stopped after a failed DAG; no further extraction started")
        if all(v == "success" for v in states.values()):
            for kind in kinds:
                ready(collection + "-" + kind, role="runs" if kind == "runs" else "all")
            return
        time.sleep(20)


def start(collection, conf):
    try:
        api("POST", "/api/v2/dags/games_metadata/dagRuns", {
            "dag_run_id": collection + "-games_metadata", "logical_date": utcnow(),
            "conf": {"collection_id": collection, **conf}}, token())
    except HTTPError as error:
        if error.code != 409:
            raise


def main():
    state = Path(os.environ["ETL_STATE"])
    if "--log" in sys.argv:
        sys.stdout = sys.stderr = (state / "fresh-cycle.log").open("a", buffering=1)
    with file_lock(state / "fresh-cycle.lock"):
        wait(HALF_LIFE, ("games_metadata", "runs", "players"))
        start(CATALOG, {"scope": "catalog", "request_budget": 650})
        wait(CATALOG, ("games_metadata",))
        start(MINECRAFT, {"scope": "full_game", "game": "mc", "request_budget": 1500})
        wait(MINECRAFT, ("games_metadata", "runs", "players"))
        print(utcnow(), "Fresh collection cycle complete; top-five remains disabled", flush=True)


if __name__ == "__main__":
    main()
