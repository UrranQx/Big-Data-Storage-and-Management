"""Version-controlled SQL datasets and chart definitions for Superset 6.1."""

PROFILE_AXES = [
    ("any_pct", "Any%"),
    ("full_pct", "100%"),
    ("low_pct", "Low%"),
    ("all_bosses", "All Bosses"),
    ("glitchless", "Glitchless"),
    ("nmg", "No Major Glitches"),
]

PREFIX = """
{% set runners = filter_values('runner', remove_filter=True) %}
{% set scopes = filter_values('scope_name', remove_filter=True) %}
{% set games = filter_values('game_name', remove_filter=True) %}
{% set windows = filter_values('window_days', remove_filter=True) %}
{% set targets = filter_values('target_code', remove_filter=True) %}
{% set basis = filter_values('time_basis', remove_filter=True) %}
"""


def person(alias="p"):
    return (
        "{% if runners %} "
        + alias
        + ".holder_id IN (SELECT holder_id FROM speedrun_marts.runner_selector "
        "WHERE runner IN {{ runners|where_in }}) {% else %} "
        + alias
        + ".holder_id=(SELECT holder_id FROM speedrun_marts.demo_selection) {% endif %}"
    )


def field(alias="s"):
    return (
        "{% if scopes %} "
        + alias
        + ".scope_name IN {{ scopes|where_in }} {% else %} "
        + alias
        + ".scope_id=(SELECT scope_id FROM speedrun_marts.demo_selection) {% endif %}"
    )


def game(alias="s"):
    return (
        "{% if games %} "
        + alias
        + ".game_name IN {{ games|where_in }} {% else %} true {% endif %}"
    )


RUNNER = (
    "(SELECT runner FROM speedrun_marts.runner_selector p WHERE "
    + person()
    + " LIMIT 1) AS runner"
)
WINDOW = "{{ windows[0]|int if windows else 30 }}"
TARGET = "{% if targets %} {{ targets|where_in }} {% else %} ('top10') {% endif %}"
BASIS = "{% if basis %} {{ basis|where_in }} {% else %} ('performed_at') {% endif %}"


def datasets():
    result = {
        "runner_selector": "SELECT * FROM speedrun_marts.runner_selector",
        "scope_selector": "SELECT * FROM speedrun_marts.field",
        "personal_scope_selector": "SELECT s.*,r.runner FROM speedrun_marts.field s JOIN speedrun_marts.pb p USING(scope_id) JOIN speedrun_marts.runner_selector r ON r.holder_id=p.holder_id WHERE "
        + person(),
        "parameters": """SELECT * FROM (VALUES (7),(30),(90)) w(window_days)
            CROSS JOIN (VALUES ('top3'),('top5'),('top10'),('top50'),('top100'),('top1pct')) t(target_code)
            CROSS JOIN (VALUES ('performed_at'),('submitted_at')) b(time_basis)
            CROSS JOIN (VALUES (365),(730),(1095)) p(min_plateau_days)
            CROSS JOIN (VALUES (1),(3),(5)) a(min_authors)
            CROSS JOIN (VALUES (0.1),(1.0),(5.0)) i(min_improvement_pct)""",
        "goals": "SELECT p.*,s.scope_name,s.game_name,"
        + RUNNER
        + " FROM speedrun_marts.goal p JOIN speedrun_marts.scope s USING(scope_id) WHERE "
        + person()
        + " AND "
        + field()
        + " AND p.target_code IN "
        + TARGET,
        "rank_curve": "SELECT p.place,p.seconds,p.field_size,s.scope_name,s.game_name,"
        + RUNNER
        + ",CASE WHEN "
        + person()
        + " THEN 'Выбранный раннер' ELSE 'Поле' END AS series FROM speedrun_marts.pb p JOIN speedrun_marts.scope s USING(scope_id) WHERE "
        + field(),
        "wr_opportunities": "SELECT p.*,s.scope_name,s.game_name,"
        + RUNNER
        + " FROM speedrun_marts.wr_opportunity p JOIN speedrun_marts.scope s USING(scope_id) WHERE "
        + person()
        + " AND s.field_platform='all' AND "
        + game(),
        "recommendations": """WITH selected AS (
              SELECT p.* FROM speedrun_marts.game_portfolio p WHERE """
        + person()
        + """
            ), ranked AS (
              SELECT t.destination_game,t.destination_name,count(*) AS supporting_games,
                     exp(avg(ln(t.lift))) AS recommendation_score,min(t.transitioned_people) AS minimum_support,
                     string_agg(t.source_name || ' (lift ' || round(t.lift,2) || ')',', ' ORDER BY t.lift DESC) AS evidence,
                     max(t.strong_people) AS strong_people
              FROM selected p JOIN speedrun_marts.game_transition t ON t.source_game=p.game_id
              WHERE t.supported AND NOT EXISTS (SELECT 1 FROM selected k WHERE k.game_id=t.destination_game)
              GROUP BY t.destination_game,t.destination_name
            ) SELECT r.*,"""
        + RUNNER
        + """,c.category_name,c.values_name,c.strong_people AS category_strong_people
            FROM ranked r LEFT JOIN LATERAL (
              SELECT d.category_name,d.values_name,max(d.strong_people) AS strong_people
              FROM speedrun_marts.destination_category d JOIN selected p ON p.game_id=d.source_game
              WHERE d.destination_game=r.destination_game GROUP BY d.scope_id,d.category_name,d.values_name
              ORDER BY max(d.strong_people) DESC,max(d.transitioned_people) DESC,d.scope_id LIMIT 1
            ) c ON true""",
        "transitions": "SELECT 'Из: ' || t.source_name AS source_name,'В: ' || t.destination_name AS destination_name,t.transitioned_people,t.lift,"
        + RUNNER
        + " FROM speedrun_marts.game_transition t WHERE t.supported AND t.source_game IN (SELECT p.game_id FROM speedrun_marts.game_portfolio p WHERE "
        + person()
        + ") ORDER BY t.lift DESC LIMIT 30",
        "similarity": """WITH names AS (SELECT DISTINCT game_id,game_name FROM speedrun_marts.scope), pairs AS (
              SELECT source_game,destination_game,jaccard FROM speedrun_marts.game_similarity
              UNION ALL SELECT destination_game,source_game,jaccard FROM speedrun_marts.game_similarity
            ) SELECT a.game_name AS source_name,b.game_name AS destination_name,x.jaccard,"""
        + RUNNER
        + """
            FROM pairs x JOIN names a ON a.game_id=x.source_game JOIN names b ON b.game_id=x.destination_game
            WHERE x.source_game IN (SELECT p.game_id FROM speedrun_marts.game_portfolio p WHERE """
        + person()
        + """ )
            ORDER BY x.jaccard DESC LIMIT 100""",
        "form": "SELECT p.*,s.scope_name,s.game_name,"
        + RUNNER
        + " FROM speedrun_marts.runner_form p JOIN speedrun_marts.scope s USING(scope_id) WHERE "
        + person()
        + " AND "
        + field(),
        "profile": "SELECT p.holder_name,"
        + ",".join(
            "max(p.strength) FILTER(WHERE tag='" + tag + "') AS " + col
            for col, tag in PROFILE_AXES
        )
        + ","
        + RUNNER
        + " FROM speedrun_marts.runner_profile p WHERE "
        + person()
        + " GROUP BY p.holder_name HAVING count(DISTINCT p.tag)=6",
        "profile_values": "WITH selected AS (SELECT p.* FROM speedrun_marts.runner_selector p WHERE "
        + person()
        + "), axes(tag) AS (VALUES "
        + ",".join("('" + tag + "')" for _, tag in PROFILE_AXES)
        + ") SELECT r.runner,a.tag,p.strength,p.game_count,p.scope_count,"
        + "CASE WHEN p.strength IS NULL THEN 'Нет данных' ELSE 'Есть наблюдения' END AS availability "
        + "FROM selected r CROSS JOIN axes a LEFT JOIN speedrun_marts.runner_profile p "
        + "ON p.holder_id=r.holder_id AND p.tag=a.tag",
        "cohorts": """SELECT c.*,s.scope_name,"""
        + RUNNER
        + """ FROM speedrun_marts.cohort_summary c
            JOIN speedrun_marts.scope s USING(scope_id) WHERE """
        + field()
        + " AND c.target_code IN "
        + TARGET
        + """
            AND c.start_gap_bin IN (SELECT p.start_gap_bin FROM speedrun_marts.cohort_member p
              WHERE p.scope_id=c.scope_id AND p.target_code=c.target_code AND """
        + person()
        + ")",
        "survival": """SELECT c.*,s.scope_name,c.entry_year::text AS cohort_year,"""
        + RUNNER
        + """
            FROM speedrun_marts.cohort_survival c JOIN speedrun_marts.scope s USING(scope_id)
            WHERE """
        + field()
        + " AND c.target_code IN "
        + TARGET
        + """
            AND c.start_gap_bin IN (SELECT p.start_gap_bin FROM speedrun_marts.cohort_member p
              WHERE p.scope_id=c.scope_id AND p.target_code=c.target_code AND """
        + person()
        + ")",
        "rivals": """WITH anchor AS (SELECT p.* FROM speedrun_marts.runner_window p
              JOIN speedrun_marts.scope s USING(scope_id) WHERE """
        + person()
        + " AND "
        + field()
        + " AND window_days="
        + WINDOW
        + """ AND sample_size>=5)
            SELECT w.*,s.scope_name,"""
        + RUNNER
        + """,abs(w.median_seconds-a.median_seconds) AS median_gap,
                   w.median_seconds<a.median_seconds AS faster_median
            FROM anchor a JOIN speedrun_marts.runner_window w USING(scope_id,window_days)
            JOIN speedrun_marts.scope s USING(scope_id) WHERE w.sample_size>=5
            ORDER BY abs(w.median_seconds-a.median_seconds),w.holder_id LIMIT 21""",
        "recent_distribution": """WITH recent AS (
            SELECT p.*,row_number() OVER(ORDER BY submitted_at DESC,run_id) AS n,s.scope_name
            FROM speedrun_marts.run p JOIN speedrun_marts.scope s USING(scope_id)
            WHERE """
        + person()
        + " AND "
        + field()
        + """
          ) SELECT r.*,"""
        + RUNNER
        + " FROM recent r WHERE n<=10",
        "activity": "SELECT a.*,s.scope_name,s.game_name FROM speedrun_marts.activity a JOIN speedrun_marts.scope s USING(scope_id) WHERE a.window_days="
        + WINDOW
        + " AND s.field_platform='all' AND (a.wr_improvements>0 OR a.top10_pbs>0 OR a.top10_entries>0) AND "
        + game(),
        "calendar": "SELECT c.*,s.scope_name,s.game_name,"
        + WINDOW
        + " AS window_days,extract(isodow FROM event_date)::text AS weekday,to_char(event_date,'IYYY-IW') AS week FROM speedrun_marts.calendar_activity c JOIN speedrun_marts.scope s USING(scope_id) WHERE "
        + field()
        + " AND event_date>(SELECT as_of FROM speedrun_marts.context)-"
        + WINDOW
        + "*interval '1 day'",
        "records": "SELECT r.*,s.scope_name,s.game_name FROM speedrun_marts.record_event r JOIN speedrun_marts.scope s USING(scope_id) WHERE r.event_kind IN ('wr','wr_tie') AND "
        + field()
        + " AND time_basis IN "
        + BASIS,
        "revival": """
            {% set plateaus = filter_values('min_plateau_days', remove_filter=True) %}
            {% set authors = filter_values('min_authors', remove_filter=True) %}
            {% set improvements = filter_values('min_improvement_pct', remove_filter=True) %}
            SELECT r.*,s.scope_name,s.game_name,
                {{ plateaus[0]|int if plateaus else 730 }} AS min_plateau_days,
                {{ authors[0]|int if authors else 3 }} AS min_authors,
                {{ improvements[0]|float if improvements else 1.0 }} AS min_improvement_pct
            FROM speedrun_marts.revival r JOIN speedrun_marts.scope s USING(scope_id)
            WHERE s.field_platform='all' AND """
        + game()
        + """
              AND plateau_days>={{ plateaus[0]|int if plateaus else 730 }}
              AND before_authors>={{ authors[0]|int if authors else 3 }}
              AND improvement_pct>={{ improvements[0]|float if improvements else 1.0 }}""",
        "comparison": """WITH episode AS (
            SELECT r.* FROM speedrun_marts.revival r JOIN speedrun_marts.scope s USING(scope_id)
            WHERE """
        + field()
        + """ ORDER BY r.improvement_pct DESC,r.event_at DESC LIMIT 1
          ) SELECT s.scope_name,p.run_id,p.holder_name,p.seconds,p.performed_at,p.video_url,
            'https://www.speedrun.com/runs/' || p.run_id AS run_url,
            CASE WHEN p.run_id=ANY(e.previous_run_ids) THEN 'Предыдущий WR'
                 WHEN p.run_id=ANY(e.run_ids) THEN 'Новый WR' ELSE 'После события: лучше прежнего WR' END AS role,
            'Кандидат на проверку по видео; причина скачка данными не доказана'::text AS interpretation
          FROM episode e JOIN speedrun_marts.run p ON p.scope_id=e.scope_id
          JOIN speedrun_marts.scope s ON s.scope_id=p.scope_id
          WHERE p.run_id=ANY(e.previous_run_ids) OR p.run_id=ANY(e.run_ids)
             OR (p.performed_at>e.event_at AND p.performed_at<=e.event_at+interval '30 days'
                 AND (p.seconds-e.previous_seconds)*s.direction<0)
          ORDER BY p.performed_at LIMIT 100""",
        "medals": """
          {% set categories=filter_values('category_name', remove_filter=True) %}
          {% set misc=filter_values('is_misc', remove_filter=True) %}
          WITH counts AS (
            SELECT c.country_code,count(*) FILTER(WHERE source_place=1) AS gold,
              count(*) FILTER(WHERE source_place=2) AS silver,count(*) FILTER(WHERE source_place=3) AS bronze
            FROM speedrun_marts.country_contribution c WHERE c.country_code<>'UNKNOWN' AND """
        + game("c")
        + """
            {% if categories %} AND c.category_name IN {{ categories|where_in }} {% endif %}
            {% if misc %} AND c.is_misc::text IN {{ misc|where_in }} {% endif %}
            GROUP BY c.country_code
          ), weighted AS (SELECT *,1+greatest(max(silver) OVER(),max(bronze) OVER())::numeric AS base FROM counts)
          SELECT w.*,c.country_name,NULL::text AS game_name,NULL::text AS category_name,NULL::text AS is_misc,
            gold*base*base+silver*base+bronze AS medal_score,
            rank() OVER(ORDER BY gold DESC,silver DESC,bronze DESC) AS overall_rank
          FROM weighted w JOIN speedrun_marts.country c USING(country_code)""",
        "unknown_medals": """
          {% set categories=filter_values('category_name', remove_filter=True) %}
          {% set misc=filter_values('is_misc', remove_filter=True) %}
          SELECT count(*) AS unknown_contributions,NULL::text AS game_name,NULL::text AS category_name,NULL::text AS is_misc
          FROM speedrun_marts.country_contribution c WHERE country_code='UNKNOWN' AND """
        + game("c")
        + """
          {% if categories %} AND c.category_name IN {{ categories|where_in }} {% endif %}
          {% if misc %} AND c.is_misc::text IN {{ misc|where_in }} {% endif %}""",
        "medal_selector": "SELECT DISTINCT game_name,category_name,is_misc::text AS is_misc FROM speedrun_marts.country_contribution",
        "coverage": "SELECT * FROM speedrun_marts.quality_summary CROSS JOIN (SELECT collection_id FROM speedrun_marts.context) c",
        "source_history": "SELECT * FROM speedrun_marts.source_wr s WHERE " + game(),
        "recommendation_evaluation": "SELECT * FROM speedrun_marts.recommendation_evaluation",
    }
    # PostgreSQL WhereInMacro compiles literals for psycopg2 (%%), but the
    # virtual dataset is subsequently compiled again by SQLAlchemy. Undo only
    # percent escaping here; where_in still handles SQL quotes safely.
    rendered = {}
    for name, query in result.items():
        prefix = PREFIX
        if name in {"runner_selector", "scope_selector", "medal_selector", "parameters"}:
            # Keep Superset's own search predicate on filter-option datasets.
            prefix = ""
        elif name == "personal_scope_selector":
            prefix = "{% set runners = filter_values('runner', remove_filter=True) %}\n"
        rendered[name] = (prefix + query).replace(
            "|where_in", "|where_in|replace('%%', '%')"
        )
    return rendered


def metric(column, aggregate="MAX", label=None):
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": column},
        "aggregate": aggregate,
        "label": label or column,
        "optionName": "metric_" + (label or column),
    }


def charts():
    """Key, title, dataset, visualization and its public form-data controls."""

    def table(key, title, cols, order=None):
        return (
            key,
            title,
            key,
            "table",
            {
                "query_mode": "raw",
                "all_columns": cols,
                "order_by_cols": [f'["{order[0]}", {str(order[1]).lower()}]']
                if order
                else [],
                "page_length": 15,
                "include_search": True,
                "allow_rearrange_columns": True,
                "show_cell_bars": False,
                "column_config": {
                    column: {"d3NumberFormat": ",.2f"}
                    for column in cols
                    if column.endswith(("_seconds", "_pct"))
                    or column in ("recommendation_score", "strength", "median_gap")
                },
            },
        )

    def line(key, title, x, metrics, **extra):
        return (
            key,
            title,
            key,
            "echarts_timeseries_line",
            {
                "x_axis": x,
                "metrics": [metric(m) for m in metrics],
                "groupby": [],
                "time_grain_sqla": None,
                "show_legend": True,
                "markerEnabled": True,
                "row_limit": 100000,
                **extra,
            },
        )

    return [
        table(
            "goals",
            "01 · Следующая цель",
            [
                "target_code",
                "place",
                "field_size",
                "pb_seconds",
                "threshold_seconds",
                "improvement_seconds",
                "improvement_pct",
                "sufficient_field",
            ],
        ),
        line(
            "rank_curve",
            "Поле соперников: место и время",
            "place",
            ["seconds"],
            groupby=["series"],
        ),
        table(
            "wr_opportunities",
            "02 · Где приблизиться к WR",
            [
                "game_name",
                "scope_name",
                "place",
                "field_size",
                "gap_pct",
                "gap_seconds",
                "active_rivals",
                "opportunity_rank",
            ],
            ("opportunity_rank", True),
        ),
        table(
            "recommendations",
            "03 · Новые игры по переходам похожих раннеров",
            [
                "destination_name",
                "recommendation_score",
                "minimum_support",
                "supporting_games",
                "category_name",
                "values_name",
                "category_strong_people",
                "evidence",
            ],
            ("recommendation_score", False),
        ),
        (
            "transitions",
            "Переходы в течение 365 дней",
            "transitions",
            "sankey_v2",
            {
                "source": "source_name",
                "target": "destination_name",
                "metric": metric("transitioned_people"),
            },
        ),
        (
            "similarity",
            "Пересечение сообществ · Jaccard",
            "similarity",
            "heatmap_v2",
            {
                "x_axis": "destination_name",
                "groupby": "source_name",
                "metric": metric("jaccard"),
                "normalize_across": "heatmap_v2",
                "show_legend": True,
                "value_bounds": [0, 1],
            },
        ),
        table(
            "cohorts",
            "04 · Достижение фиксированного порога",
            [
                "entry_year",
                "start_gap_bin",
                "horizon_days",
                "observed_people",
                "reached_people",
                "reached_pct",
            ],
            ("observed_people", False),
        ),
        line(
            "survival",
            "Календарные дни до цели · Kaplan–Meier",
            "day",
            ["reached_pct"],
            groupby=["cohort_year"],
            seriesType="step",
        ),
        line(
            "form",
            "05 · Публикации, PB и форма за 10 ранов",
            "submitted_at",
            ["seconds", "pb_seconds", "median_seconds", "q1_seconds", "q3_seconds"],
        ),
        (
            "recent_distribution",
            "Разброс последних 10 опубликованных ранов",
            "recent_distribution",
            "box_plot",
            {
                "groupby": ["holder_name"],
                "metrics": [metric("seconds", "AVG")],
                "columns": ["run_id"],
                "whiskerOptions": "Tukey",
            },
        ),
        (
            "profile",
            "08 · Паутинка · при наличии всех 6 осей",
            "profile",
            "radar",
            {
                "groupby": ["holder_name"],
                "metrics": [metric(col, label=tag) for col, tag in PROFILE_AXES],
                "show_legend": True,
                "column_config": {
                    tag: {"radarMetricMinValue": 0, "radarMetricMaxValue": 100}
                    for _, tag in PROFILE_AXES
                },
            },
        ),
        table(
            "profile_values",
            "Профиль: оценки и наличие данных",
            ["tag", "strength", "game_count", "scope_count", "availability"],
        ),
        table(
            "rivals",
            "09 · Ближайшие соперники по последним публикациям",
            [
                "holder_name",
                "sample_size",
                "median_seconds",
                "q1_seconds",
                "q3_seconds",
                "median_gap",
                "first_publication",
                "last_publication",
            ],
            ("median_gap", True),
        ),
        table(
            "activity",
            "10 · Где идёт борьба за WR и топ-10",
            [
                "game_name",
                "scope_name",
                "wr_race_rank",
                "wr_improvements",
                "wr_authors",
                "top10_authors",
                "top10_pbs",
                "top10_entries",
                "top10_gap_pct",
                "active_rivals",
            ],
            ("wr_race_rank", True),
        ),
        (
            "calendar",
            "Календарь движения в топ-10",
            "calendar",
            "heatmap_v2",
            {
                "x_axis": "week",
                "groupby": "weekday",
                "metric": metric("top10_authors", "SUM"),
                "show_legend": True,
            },
        ),
        line(
            "records",
            "Реконструкция истории WR",
            "event_at",
            ["seconds"],
            seriesType="step",
        ),
        table(
            "revival",
            "06 · Возвращение игры после плато WR",
            [
                "game_name",
                "scope_name",
                "event_at",
                "plateau_days",
                "improvement_pct",
                "before_authors",
                "same_day_authors",
                "after_authors",
                "complete_after_window",
                "episode_kind",
            ],
            ("improvement_pct", False),
        ),
        table(
            "comparison",
            "07 · До и после скачка: раны для разбора",
            [
                "role",
                "holder_name",
                "seconds",
                "performed_at",
                "run_url",
                "video_url",
                "interpretation",
            ],
            ("performed_at", True),
        ),
        table(
            "source_history",
            "Исходная история WR · отдельные наблюдения",
            [
                "game_name",
                "category_name",
                "performed_at",
                "run_ids",
                "scope_key",
                "observed_at",
            ],
            ("performed_at", False),
        ),
        table(
            "coverage",
            "Покрытие и исключения из расчёта",
            [
                "source_runs",
                "unresolved_scope_runs",
                "invalid_timer",
                "orphaned_runs",
                "missing_participants",
                "missing_scope_value",
                "unknown_direction",
                "missing_submission_date",
                "quarantined_date_anomalies",
            ],
        ),
        table(
            "recommendation_evaluation",
            "Проверка рекомендаций на следующем году",
            [
                "method",
                "train_until",
                "test_people",
                "people_with_new_game",
                "people_with_recommendations",
                "hits",
                "conditional_hit_at_5_pct",
            ],
        ),
        table(
            "medals",
            "Олимпийский зачёт стран",
            ["country_name", "gold", "silver", "bronze", "medal_score", "overall_rank"],
            ("overall_rank", True),
        ),
        (
            "medal_bars",
            "Призовые места по странам",
            "medals",
            "echarts_timeseries_bar",
            {
                "x_axis": "country_name",
                "metrics": [metric(x) for x in ("gold", "silver", "bronze")],
                "groupby": [],
                "stack": "Stack",
                "show_legend": True,
                "row_limit": 20,
                "order_desc": True,
                "orientation": "horizontal",
            },
        ),
        (
            "unknown_medals",
            "Призовые вклады без известной страны",
            "unknown_medals",
            "big_number_total",
            {
                "metric": metric("unknown_contributions"),
                "subheader": "Не участвуют в рейтинге стран",
            },
        ),
    ]


DASHBOARDS = {
    "runner": (
        "Раннер",
        [
            "goals",
            "rank_curve",
            "wr_opportunities",
            "recommendations",
            "transitions",
            "similarity",
            "cohorts",
            "survival",
            "form",
            "recent_distribution",
            "profile",
            "profile_values",
            "rivals",
            "recommendation_evaluation",
        ],
    ),
    "records": (
        "Борьба за рекорды",
        [
            "activity",
            "revival",
            "records",
            "calendar",
            "comparison",
            "source_history",
            "coverage",
        ],
    ),
    "countries": ("Страны", ["medals", "medal_bars", "unknown_medals"]),
}

NOTES = {
    "runner": "Архивный срез, не текущий сайт. Время в секундах. Цели рассчитаны по всему сопоставимому полю до выбора раннера. Линия PB относится к опубликованным успешным ранам. Когорты: календарные дни, минимум 30 наблюдений; пустая оценка означает недостаточно данных. Рекомендации — наблюдаемые переходы, не доказательство переноса навыка. Профиль: средний лучший перцентиль по игре. Паутинка строится только при наличии всех 6 осей; иначе смотрите таблицу оценок рядом — неизвестные значения не равны нулю. Соперники: минимум 5 публикаций за выбранное окно, максимум 10 на человека.",
    "records": "История реконструирована по ранам, верифицированным в архивном срезе. Одновременные результаты обработаны вместе. Предполагаемый скип или баг требует датированного внешнего подтверждения. В таблице плато отдельно показаны участники до, в день и после WR. Разбор видео показывает крупнейший скачок выбранной категории. Активность измеряется относительно даты среза.",
    "countries": "Медали: источник подтверждает места 1–3, один run × страна; международная команда даёт вклад каждой стране. Итог упорядочен по золоту, серебру, бронзе. Баллы G×B²+S×B+Br, B=1+max(S,Br) в выбранном поле. Фильтры игры и категории пересчитывают зачёт, фильтр страны только выбирает строку после расчёта. Неизвестная страна вынесена отдельно.",
}
