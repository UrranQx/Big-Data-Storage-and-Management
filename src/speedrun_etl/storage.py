"""Совместимые импорты прежнего ODS; текущий ETL использует connections."""

from .connections import namespace, pg_connect
from .legacy.storage import (
    Mongo,
    Postgres,
    S3,
    STORES,
    init_stores,
    load_package,
    require_ready,
    set_status,
    verify_store,
)

__all__ = [
    "namespace",
    "pg_connect",
    "Mongo",
    "Postgres",
    "S3",
    "STORES",
    "init_stores",
    "load_package",
    "require_ready",
    "set_status",
    "verify_store",
]
