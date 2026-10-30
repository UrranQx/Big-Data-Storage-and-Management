"""Проверяет замороженные входы отдельного стенда сравнительных опытов.

Обычный запуск проверяет receipts хранилищ. Изолированный стенд использует
сохранённые подтверждения и хеши копий, не подключаясь к production.
"""

import hashlib
import os
from pathlib import Path

from .jsonutil import digest, read


def isolated(state):
    marker = Path(state) / "isolated-experiment.json"
    if not marker.exists():
        return False
    expected = os.environ.get("ETL_EXPERIMENT_ID")
    actual = read(marker)
    if not expected or actual.get("experiment_id") != expected:
        raise ValueError(
            "Isolated experiment marker does not match explicit environment"
        )
    if actual.get("production_access") is not False:
        raise ValueError("Experiment marker must prohibit production access")
    return True


def verify_ref(state, ref):
    if not isolated(state):
        from .entity_storage import ready

        return ready(
            ref["snapshot_id"],
            namespace=ref.get("namespace", "entity_v2"),
            role=ref["role"],
            expected=ref["digest"],
        )
    frozen = read(Path(state) / "frozen-inputs.json")["snapshots"][ref["snapshot_id"]]
    proof = frozen["verified"]
    if frozen["digest"] != ref["digest"] or proof["digest"] != ref["digest"]:
        raise ValueError("Frozen input digest differs from source confirmation")
    if proof["state"] != "ready" or set(proof["stores"]) != {
        "postgres",
        "mongo",
        "garage",
    }:
        raise ValueError("Frozen source has no three-store confirmation")
    if any(digest(summary) != ref["digest"] for summary in proof["stores"].values()):
        raise ValueError("Frozen store summaries differ")
    package = Path(ref["package_uri"]).resolve()
    if not package.is_relative_to(Path(state).resolve()):
        raise ValueError("Frozen package is outside isolated state")
    with (package / "entities.sqlite3").open("rb") as handle:
        checksum = hashlib.file_digest(handle, "sha256").hexdigest()
    if checksum != frozen["sqlite_sha256"]:
        raise ValueError("Frozen SQLite copy changed")
    return {
        "digest": ref["digest"],
        "source_confirmation": "frozen three-store receipts",
    }
