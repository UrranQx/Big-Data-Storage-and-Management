"""Рабочий ODS — PostgreSQL; три сервиса доступны для архива ЛР1."""

import os


def stores():
    value = os.environ.get("ETL_STORES", "postgres")
    if value == "postgres":
        return ("postgres",)
    if value == "postgres,mongo,garage":
        return ("postgres", "mongo", "garage")
    raise ValueError("ETL_STORES must be postgres or postgres,mongo,garage")
