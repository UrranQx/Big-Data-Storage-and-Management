"""Idempotently provision laboratory dashboards by stable UUID."""

import json
import os
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from superset_assets import DASHBOARDS, NOTES, charts, datasets


def uid(kind, name):
    return uuid5(NAMESPACE_URL, "https://speedrun-lab.local/lab3/" + kind + "/" + name)


LABELS = {
    "runner": "Раннер",
    "best_rank": "Место по лучшему результату",
    "result_points": "Баллы зачёта (0–150)",
    "portfolio_rank": "Место по универсальности",
    "portfolio_points": "Общие баллы (0–1500)",
    "portfolio_quality": "За качество",
    "portfolio_density": "За плотность",
    "portfolio_tenure": "За прошлое удержание",
    "unknown_history_games": "Игр с неизвестной историей",
    "quality_points": "Качество, баллы",
    "density_points": "Плотность, баллы",
    "tenure_points": "Удержание, баллы (пусто = неизвестно)",
    "history_note": "Статус истории",
    "quality_days": "Условные дни с учётом качества",
    "days_first": "Дней на 1-м месте",
    "days_second": "Дней на 2-м месте",
    "days_third": "Дней на 3-м месте",
    "game_order": "Порядок вклада",
    "game_weight": "Вес игры",
    "weighted_points": "Вклад в общую сумму",
    "people_with_unknown_history": "Раннеров с неполной историей",
    "portfolio_games": "Игры и вклад в баллы",
    "rank_k5": "Место при поправке 5",
    "rank_k50": "Место при поправке 50",
    "ranking_status": "Основание включения",
    "calculated_medal_rank": "Место по медалям",
    "calculated_gold": "Расчётных 1-х мест",
    "calculated_silver": "Расчётных 2-х мест",
    "calculated_bronze": "Расчётных 3-х мест",
    "eligible_fields": "Зачётов выше порога",
    "eligible_games": "Игр выше порога",
    "solo_place": "Сольное место",
    "solo_field_size": "Сольных соперников, включая себя",
    "solo_tie_size": "Равных результатов, включая себя",
    "percentile": "Обойдены с учётом ничьих, %",
    "result_value": "Результат в шкале таймера",
    "wr_value": "WR в шкале таймера",
    "wr_gap_pct": "Отставание от WR, %",
    "comparison_direction": "Направление сравнения",
    "timer": "Таймер источника",
    "near_wr_people": "В пределах 1% от WR",
    "leader_top10_gap_pct": "Разрыв топ-10 и WR, %",
    "leader_top25_gap_pct": "Разрыв топ-25 и WR, %",
    "completion_median_seconds": "Медиана завершений, с (меньше лучше)",
    "archive_people": "Профилей в аналитическом срезе",
    "ranked_people": "Раннеров с оценкой",
    "people_without_solo_pb": "Без текущего сольного PB",
    "people_below_minimum": "Только малые поля",
    "gold": "1-е места",
    "silver": "2-е места",
    "bronze": "3-и места",
    "medal_score": "Итоговые баллы",
    "overall_rank": "Место в зачёте",
    "country_name": "Страна",
    "game_name": "Игра",
    "scope_name": "Категория и условия",
    "place": "Место",
    "field_size": "Участников",
    "pb_seconds": "PB, с",
    "seconds": "Время, с",
    "threshold_seconds": "Порог, с",
    "improvement_seconds": "Сократить, с",
    "improvement_pct": "Улучшение, %",
    "gap_pct": "До WR, %",
    "gap_seconds": "До WR, с",
    "active_rivals": "Активных у WR",
    "opportunity_rank": "Приоритет",
    "target_code": "Цель",
    "sufficient_field": "Достаточный размер поля",
    "median_seconds": "Медиана, с",
    "q1_seconds": "Q1, с",
    "q3_seconds": "Q3, с",
    "sample_size": "Публикаций",
    "holder_name": "Раннер",
    "minimum_support": "Минимум переходов",
    "supporting_games": "Игр-оснований",
    "evidence": "Основания",
    "recommendation_score": "Средний lift",
    "destination_name": "Новая игра",
    "category_name": "Категория",
    "values_name": "Условия",
    "entry_year": "Год первого рана",
    "start_gap_bin": "Стартовый разрыв ×10%",
    "horizon_days": "Горизонт, дней",
    "observed_people": "Наблюдений",
    "reached_people": "Достигли",
    "reached_pct": "Доля достигших, %",
    "plateau_days": "Плато, дней",
    "before_authors": "Участников до WR",
    "same_day_authors": "В день WR",
    "after_authors": "После WR",
    "event_at": "Дата события",
    "wr_improvements": "Улучшений WR",
    "wr_authors": "Авторов WR",
    "wr_race_rank": "Ранг борьбы за WR",
    "top10_authors": "Авторов PB в топ-10",
    "top10_pbs": "PB в топ-10",
    "top10_entries": "Входов в топ-10",
    "top10_gap_pct": "Разрыв топ-10, %",
    "strength": "Перцентиль",
    "tag": "Тип категории",
    "category_strong_people": "Сильных раннеров, не менее",
    "game_count": "Игр в оценке",
    "scope_count": "Полей в оценке",
    "availability": "Наличие данных",
    "median_gap": "Разница медиан, с",
    "first_publication": "Начало окна публикаций",
    "last_publication": "Конец окна публикаций",
    "method": "Метод",
    "train_until": "Обучение до",
    "test_people": "Раннеров в тесте",
    "people_with_new_game": "Перешли в новую игру",
    "people_with_recommendations": "Получили рекомендации",
    "hits": "Попаданий top-5",
    "conditional_hit_at_5_pct": "Попадания среди перешедших, %",
    "role": "Роль рана",
    "performed_at": "Дата выполнения",
    "run_url": "Страница рана",
    "video_url": "Видео",
    "interpretation": "Интерпретация",
    "complete_after_window": "Полное окно после WR",
    "episode_kind": "Тип события",
    "source_runs": "Ранов на входе",
    "unresolved_scope_runs": "Поле не определено",
    "invalid_timer": "Таймер не подходит",
    "orphaned_runs": "Orphaned",
    "missing_participants": "Нет участников",
    "missing_scope_value": "Не хватает переменных",
    "unknown_direction": "Неизвестно направление",
    "missing_submission_date": "Нет даты подачи",
    "quarantined_date_anomalies": "Аномалии дат",
}


def native_filter(key, name, dataset, column, chart_ids, default=None, multi=False):
    mask = {"extraFormData": {}, "filterState": {}, "ownState": {}}
    if default is not None:
        values = default if isinstance(default, list) else [default]
        mask = {
            "extraFormData": {"filters": [{"col": column, "op": "IN", "val": values}]},
            "filterState": {"value": values},
            "ownState": {},
        }
    return {
        "id": "NATIVE_FILTER-" + key,
        "name": name,
        "type": "NATIVE_FILTER",
        "filterType": "filter_select",
        "targets": [{"datasetId": dataset.id, "column": {"name": column}}],
        "defaultDataMask": mask,
        "cascadeParentIds": [],
        "chartsInScope": chart_ids,
        "tabsInScope": [],
        "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
        "description": "",
        "controlValues": {
            "multiSelect": multi,
            "showSearch": True,
            "searchAllOptions": True,
            "inverseSelection": False,
            "enableEmptyFilter": False,
            "defaultToFirstItem": False,
        },
    }


def layout(title, chart_list, note):
    pos = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {
            "id": "GRID_ID",
            "type": "GRID",
            "parents": ["ROOT_ID"],
            "children": ["ROW-NOTE"],
        },
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": title}},
        "ROW-NOTE": {
            "id": "ROW-NOTE",
            "type": "ROW",
            "parents": ["ROOT_ID", "GRID_ID"],
            "children": ["NOTE"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        "NOTE": {
            "id": "NOTE",
            "type": "MARKDOWN",
            "parents": ["ROOT_ID", "GRID_ID", "ROW-NOTE"],
            "children": [],
            "meta": {"code": note, "height": 22, "width": 12},
        },
    }
    groups = [chart_list[i : i + 2] for i in range(0, len(chart_list), 2)]
    if title == "Рейтинг раннеров":
        groups = [
            chart_list[:1],
            chart_list[1:3],
            chart_list[3:4],
            chart_list[4:5],
            chart_list[5:],
        ]
    for i, group in enumerate(groups):
        row = "ROW-" + str(i)
        pos["GRID_ID"]["children"].append(row)
        pos[row] = {
            "id": row,
            "type": "ROW",
            "parents": ["ROOT_ID", "GRID_ID"],
            "children": [],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for chart in group:
            key = "CHART-" + str(chart.id)
            pos[row]["children"].append(key)
            pos[key] = {
                "id": key,
                "type": "CHART",
                "parents": ["ROOT_ID", "GRID_ID", row],
                "children": [],
                "meta": {
                    "chartId": chart.id,
                    "uuid": str(chart.uuid),
                    "sliceName": chart.slice_name,
                    "width": 12 // len(group),
                    "height": 65,
                },
            }
    return pos


def provision():
    from flask import g
    from superset import db, security_manager
    from superset.app import create_app

    app = create_app()
    with app.app_context():
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.core import Database
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        admin = security_manager.find_user(username="admin")
        g.user = admin
        uri = URL.create(
            "postgresql+psycopg2",
            username="analytics_reader",
            password=os.environ["ANALYTICS_READER_PASSWORD"],
            host="postgres",
            database="speedrun_ods",
        )
        engine = create_engine(uri)
        with engine.connect() as conn:
            selection = dict(
                conn.execute(text("SELECT * FROM speedrun_marts.demo_selection"))
                .mappings()
                .one()
            )
            context = dict(
                conn.execute(text("SELECT * FROM speedrun_marts.context"))
                .mappings()
                .one()
            )
        database = (
            db.session.query(Database)
            .filter_by(uuid=uid("database", "postgres"))
            .one_or_none()
        )
        if database is None:
            database = Database(
                uuid=uid("database", "postgres"), database_name="Speedrun · PostgreSQL"
            )
            db.session.add(database)
        database.sqlalchemy_uri = uri.render_as_string(hide_password=False)
        database.expose_in_sqllab = True
        database.allow_dml = False
        database.allow_run_async = False
        db.session.flush()
        tables = {}
        for key, query in datasets().items():
            table = (
                db.session.query(SqlaTable)
                .filter_by(uuid=uid("dataset", key))
                .one_or_none()
            )
            if table is None:
                table = SqlaTable(
                    uuid=uid("dataset", key),
                    table_name="lab3_" + key,
                    database=database,
                    schema="speedrun_marts",
                )
                db.session.add(table)
            table.sql = query.strip()
            table.owners = [admin]
            table.filter_select_enabled = True
            table.cache_timeout = -1
            table.description = "ЛР3. Версионируемое определение: infra/superset_assets.py; время в секундах, дата среза в speedrun_marts.context."
            db.session.flush()
            table.fetch_metadata()
            for column in table.columns:
                if column.column_name in LABELS:
                    column.verbose_name = LABELS[column.column_name]
            tables[key] = table
            print("Dataset", key, flush=True)
        slices = {}
        for key, title, source, viz, settings in charts():
            chart = (
                db.session.query(Slice).filter_by(uuid=uid("chart", key)).one_or_none()
            )
            if chart is None:
                chart = Slice(uuid=uid("chart", key))
                db.session.add(chart)
            chart.slice_name, chart.viz_type = title, viz
            chart.datasource_id, chart.datasource_type = tables[source].id, "table"
            chart.owners = [admin]
            chart.cache_timeout = -1
            db.session.flush()
            form = {
                "slice_id": chart.id,
                "datasource": f"{tables[source].id}__table",
                "viz_type": viz,
                "adhoc_filters": [],
                "time_range": "No filter",
                "row_limit": 10000,
                "color_scheme": "supersetColors",
                "linear_color_scheme": "schemeBlues",
                "y_axis_format": ",.2f",
                "x_axis_sort_asc": True,
                "show_legend": True,
                "rich_tooltip": True,
                "extra_form_data": {},
                **settings,
            }
            chart.params = json.dumps(form, ensure_ascii=False)
            chart.query_context = None
            slices[key] = chart
        for key, (title, members) in DASHBOARDS.items():
            dash = (
                db.session.query(Dashboard)
                .filter_by(uuid=uid("dashboard", key))
                .one_or_none()
            )
            if dash is None:
                dash = Dashboard(uuid=uid("dashboard", key))
                db.session.add(dash)
            dash.dashboard_title, dash.slug, dash.published = title, "lab3-" + key, True
            dash.owners, dash.slices = [admin], [slices[x] for x in members]
            ids = [slices[x].id for x in members]
            filters = []

            def add(
                column,
                name,
                dataset,
                included,
                default=None,
                multi=False,
                *,
                dashboard_key=key,
                all_ids=ids,
                filter_config=filters,
            ):
                selected = [slices[x].id for x in included]
                item = native_filter(
                    dashboard_key + "-" + column,
                    name,
                    tables[dataset],
                    column,
                    selected,
                    default,
                    multi,
                )
                item["scope"]["excluded"] = [x for x in all_ids if x not in selected]
                filter_config.append(item)

            if key == "runner":
                add(
                    "runner",
                    "Раннер",
                    "runner_selector",
                    [x for x in members if x != "recommendation_evaluation"],
                    selection["runner"],
                )
                add(
                    "scope_name",
                    "Сопоставимое поле",
                    "personal_scope_selector",
                    [
                        "goals",
                        "rank_curve",
                        "cohorts",
                        "survival",
                        "form",
                        "recent_distribution",
                        "rivals",
                    ],
                    selection["scope_name"],
                )
                filters[-1]["cascadeParentIds"] = ["NATIVE_FILTER-runner-runner"]
                add(
                    "target_code",
                    "Цель",
                    "parameters",
                    ["goals", "cohorts", "survival"],
                    "top10",
                )
                add(
                    "window_days", "Окно публикаций, дней", "parameters", ["rivals"], 30
                )
            elif key == "records":
                add(
                    "game_name",
                    "Игры",
                    "scope_selector",
                    ["activity", "revival", "source_history"],
                    multi=True,
                )
                add(
                    "scope_name",
                    "Поле для истории и разбора",
                    "scope_selector",
                    ["records", "calendar", "comparison"],
                    selection["scope_name"],
                )
                add(
                    "window_days",
                    "Окно активности, дней",
                    "parameters",
                    ["activity", "calendar"],
                    30,
                )
                add(
                    "time_basis",
                    "Основание даты истории",
                    "parameters",
                    ["records"],
                    "performed_at",
                )
                add(
                    "min_plateau_days",
                    "Плато не менее, дней",
                    "parameters",
                    ["revival"],
                    730,
                )
                add(
                    "min_authors",
                    "Участников до WR не менее",
                    "parameters",
                    ["revival"],
                    3,
                )
                add(
                    "min_improvement_pct",
                    "Улучшение WR не менее, %",
                    "parameters",
                    ["revival"],
                    1.0,
                )
            elif key == "ranking":
                add(
                    "minimum_field_size",
                    "Минимум участников в сольном поле",
                    "ranking_parameters",
                    members,
                    10,
                )
                add(
                    "runner",
                    "Раннер после расчёта общего места",
                    "ranking_selector",
                    [x for x in members if x != "ranking_coverage"],
                    multi=True,
                )
            else:
                add("game_name", "Игры", "medal_selector", members, multi=True)
                add("category_name", "Категории", "medal_selector", members, multi=True)
                add("is_misc", "Misc-категории", "medal_selector", members, multi=True)
                add(
                    "country_name",
                    "Страны после расчёта рейтинга",
                    "medals",
                    ["medals", "medal_bars"],
                    multi=True,
                )
            note = (
                f"**Срез: {context['as_of']} · {context['collection_id']}**\n\n"
                + NOTES[key]
            )
            dash.position_json = json.dumps(
                layout(title, dash.slices, note), ensure_ascii=False
            )
            dash.json_metadata = json.dumps(
                {
                    "native_filter_configuration": filters,
                    "color_scheme": "supersetColors",
                    "refresh_frequency": 0,
                    "expanded_slices": {},
                    "label_colors": {},
                },
                ensure_ascii=False,
            )
        db.session.commit()
        return {
            "datasets": len(tables),
            "charts": len(slices),
            "dashboards": len(DASHBOARDS),
            "build_id": context["build_id"],
        }


if __name__ == "__main__":
    print(json.dumps(provision(), ensure_ascii=False))
