"""Контракты entity-v2/v3, роли пакетов и читаемые ID запусков Airflow.

Выделяет core рана без контекстных полей; старые снимки сохраняют прежний смысл."""

import re
import unicodedata


V2 = "entity-v2"
V3 = "entity-v3"
CONTEXT_FIELDS = ("place", "obsolete")


def version(conf):
    return conf.get("data_model", V2)


def namespace(conf):
    return "entity_v3" if version(conf) == V3 else "entity_v2"


def kinds(conf):
    """Возвращает порядок этапов; для старой v2 отдельного WR-этапа нет."""
    return (
        ("games_metadata", "runs", "wr_history", "players")
        if version(conf) == V3
        else ("games_metadata", "runs", "players")
    )


def role(kind):
    return kind if kind in ("runs", "wr_history") else "all"


def run_core(raw):
    """Сохраняет неизвестные поля и различие missing/null, убирая только контекст."""
    return {field: value for field, value in raw.items() if field not in CONTEXT_FIELDS}


def dag_run_id(conf, kind):
    """Строит стабильный ID запуска из сохранённой конфигурации без HTTP."""
    if version(conf) != V3:
        return conf["collection_id"] + "-" + kind
    game_id = conf.get("game_id") or conf.get("display_game_id")
    game_label = conf.get("game_label") or conf.get("game")
    if not game_label and not game_id:
        return conf["collection_id"] + "-" + kind
    game_label = (
        unicodedata.normalize("NFKD", game_label or "game")
        .encode("ascii", "ignore")
        .decode()
    )
    game_label = re.sub(r"[^a-z0-9]+", "-", game_label.lower()).strip("-") or "game"
    parts = [
        conf["collection_id"],
        *([game_id] if game_id else []),
        game_label[:48],
        kind,
    ]
    run_id = "--".join(parts)
    if len(run_id) > 250:
        raise ValueError("Readable DAG run ID exceeds Airflow length limit")
    return run_id
