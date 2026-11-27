# ЛР3. Реализация по этапам

Витрины PostgreSQL отвечают на вопросы о результатах раннеров, мировой рекордной истории и медалях стран. Airflow обновляет витрины после проверки DDS, Superset визуализирует опубликованные данные, OpenMetadata показывает их происхождение.

Номера строк относятся к исходникам ветки `lab3` в коммите `c590a3e`. Данные относятся к сохранённому срезу на 30 октября 2026 года. Результаты запусков приведены по протоколам приёмки 26–27 ноября.

## 1. Развёртывание Superset и OpenMetadata в Docker Compose

Сервисы добавлены в корневой `compose.yaml` под профилем `lab3`. Инициализатор `lab3-init` ждёт готовности общего PostgreSQL и вызывает [скрипт служебных БД](infra/lab3_databases.py). Тот готовит базы Superset и OpenMetadata, роль чтения аналитики и ключи каталога. Compose передаёт настройки через переменные окружения.

Superset запускается после миграций своей базы и инициализации приложения. Gunicorn обслуживает UI на локальном порту 8088. Compose проверяет готовность по `/health`.

**[compose.yaml](compose.yaml), строки 110–124**

```yaml
110 |   lab3-init:
111 |     image: speedrun-lab1-airflow:local
112 |     profiles: [lab3]
113 |     user: "0:0"
114 |     entrypoint: [python, /opt/project/infra/lab3_databases.py]
115 |     environment:
116 |       POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:?configure}
117 |       SUPERSET_DB_PASSWORD: ${SUPERSET_DB_PASSWORD:-}
118 |       OPENMETADATA_DB_PASSWORD: ${OPENMETADATA_DB_PASSWORD:-}
119 |       ANALYTICS_READER_PASSWORD: ${ANALYTICS_READER_PASSWORD:-}
120 |     volumes:
121 |       - ./infra:/opt/project/infra:ro
122 |       - ./data/lab3/keys:/opt/project/keys
123 |     depends_on:
124 |       postgres: {condition: service_healthy}
```

**[compose.yaml](compose.yaml), строки 127–160**

```yaml
127 |   superset-init:
128 |     image: speedrun-lab3-superset:local
129 |     build:
130 |       context: .
131 |       dockerfile: infra/Dockerfile.superset
132 |     profiles: [lab3]
133 |     entrypoint: [/bin/sh, -c]
134 |     command: ["superset db upgrade && superset init && python /app/lab3/superset_init.py"]
135 |     environment: &superset-env
136 |       SUPERSET_CONFIG_PATH: /app/lab3/superset_config.py
137 |       SUPERSET_SECRET_KEY: ${SUPERSET_SECRET_KEY:-}
138 |       SUPERSET_DB_PASSWORD: ${SUPERSET_DB_PASSWORD:-}
139 |       SUPERSET_ADMIN_PASSWORD: ${SUPERSET_ADMIN_PASSWORD:-}
140 |       ANALYTICS_READER_PASSWORD: ${ANALYTICS_READER_PASSWORD:-}
141 |     volumes:
142 |       - ./infra:/app/lab3:ro
143 |     depends_on:
144 |       lab3-init: {condition: service_completed_successfully}
145 |     mem_limit: 1200m
146 |
147 |   superset:
148 |     image: speedrun-lab3-superset:local
149 |     profiles: [lab3]
150 |     environment: *superset-env
151 |     entrypoint: [/app/.venv/bin/gunicorn]
152 |     command: ["--bind", "0.0.0.0:8088", "--workers", "1", "--threads", "4", "--timeout", "120", "superset.app:create_app()"]
153 |     ports: ["127.0.0.1:8088:8088"]
154 |     volumes:
155 |       - ./infra:/app/lab3:ro
156 |     depends_on:
157 |       superset-init: {condition: service_completed_successfully}
158 |     mem_limit: 1200m
159 |     healthcheck:
160 |       test: [CMD, curl, --fail, http://localhost:8088/health]
```

OpenMetadata хранит каталог в общем PostgreSQL, а поиск обслуживает Elasticsearch с собственным томом. Миграции ждут готовности поиска и успешной подготовки базы. Контейнер каталога стартует после миграций и открывает UI на локальном порту 8585. Синхронизацию выполняет DAG существующего Airflow.

**[compose.yaml](compose.yaml), строки 164–182**

```yaml
164 |   elasticsearch:
165 |     image: docker.elastic.co/elasticsearch/elasticsearch:9.3.0@sha256:4f6bdcb742e892539c6ac49b0dd3e4e182e90218546e8c6a22db378c344acb60
166 |     profiles: [lab3]
167 |     environment:
168 |       discovery.type: single-node
169 |       xpack.security.enabled: "false"
170 |       ES_JAVA_OPTS: "-Xms512m -Xmx512m"
171 |     volumes:
172 |       - lab3-search:/usr/share/elasticsearch/data
173 |     mem_limit: 1200m
174 |     healthcheck:
175 |       test: [CMD, curl, --fail, "http://localhost:9200/_cluster/health?wait_for_status=yellow&timeout=5s"]
176 |       interval: 15s
177 |       retries: 20
178 |
179 |   openmetadata-migrate:
180 |     image: docker.getcollate.io/openmetadata/server:2.0.9@sha256:8575ad1b61866f76075f096c40ec4e9ee19339dcab50169adec9b69bec380f98
181 |     profiles: [lab3]
182 |     command: ["./bootstrap/openmetadata-ops.sh", "migrate"]
```

**[compose.yaml](compose.yaml), строки 183–197**

```yaml
183 |     environment: &openmetadata-env
184 |       DB_DRIVER_CLASS: org.postgresql.Driver
185 |       DB_SCHEME: postgresql
186 |       DB_PARAMS: "sslmode=disable"
187 |       DB_HOST: postgres
188 |       DB_PORT: "5432"
189 |       DB_USER: openmetadata
190 |       DB_USER_PASSWORD: ${OPENMETADATA_DB_PASSWORD:-}
191 |       OM_DATABASE: openmetadata
192 |       ELASTICSEARCH_HOST: elasticsearch
193 |       ELASTICSEARCH_PORT: "9200"
194 |       ELASTICSEARCH_SCHEME: http
195 |       SEARCH_TYPE: elasticsearch
196 |       OPENMETADATA_HEAP_OPTS: "-Xms512m -Xmx1024m"
197 |       PIPELINE_SERVICE_CLIENT_ENABLED: "false"
```

**[compose.yaml](compose.yaml), строки 207–233**

```yaml
207 |     volumes:
208 |       - ./data/lab3/keys:/opt/lab3/keys:ro
209 |     depends_on:
210 |       lab3-init: {condition: service_completed_successfully}
211 |       elasticsearch: {condition: service_healthy}
212 |     mem_limit: 1800m
213 |
214 |   openmetadata:
215 |     image: docker.getcollate.io/openmetadata/server:2.0.9@sha256:8575ad1b61866f76075f096c40ec4e9ee19339dcab50169adec9b69bec380f98
216 |     profiles: [lab3]
217 |     environment: *openmetadata-env
218 |     ports: ["127.0.0.1:8585:8585"]
219 |     volumes:
220 |       - ./data/lab3/keys:/opt/lab3/keys:ro
221 |     depends_on:
222 |       openmetadata-migrate: {condition: service_completed_successfully}
223 |     mem_limit: 2000m
224 |     healthcheck:
225 |       test: [CMD, wget, -q, -O, /dev/null, http://localhost:8586/healthcheck]
226 |       interval: 20s
227 |       retries: 20
228 |
229 | volumes:
230 |   postgres-data:
231 |   etl-state:
232 |   airflow-logs:
233 |   lab3-search:
```

## 2. Витрины на основе DDS

Сборка ограничивает вход подтверждёнными снимками из завершённых цепочек игр. В `speedrun_3nf` хранятся нормализованные версии сущностей и наблюдения. Дополнение [lab3_dds.sql](sql/lab3_dds.sql) сохраняет контекст, необходимый для аналитики, включая исходное место, obsolete, состав участников, страну игрока, историю WR и покрытие снимков. Витрины выбирают последнее доступное наблюдение каждого рана на дату `as_of` и соединяют его с версией рана, категорией и временем нужного таймера.

**[sql/marts/01_base.sql](sql/marts/01_base.sql), строки 12–24**

```sql
 12 | CREATE TEMP TABLE _observed_runs ON COMMIT DROP AS
 13 | SELECT DISTINCT ON (o.run_id) o.*
 14 | FROM speedrun_3nf.run_observation o JOIN _snapshots s USING (snapshot_id)
 15 | WHERE o.observed_at <= (SELECT as_of FROM context)
 16 | ORDER BY o.run_id,o.observed_at DESC,o.observation_key DESC;
 17 | CREATE UNIQUE INDEX ON _observed_runs (run_id);
 18 |
 19 | CREATE TEMP TABLE _places ON COMMIT DROP AS
 20 | SELECT DISTINCT ON (o.run_id) o.*
 21 | FROM speedrun_3nf.leaderboard_observation o JOIN _snapshots s USING (snapshot_id)
 22 | WHERE o.observed_at <= (SELECT as_of FROM context)
 23 | ORDER BY o.run_id,o.observed_at DESC,o.observation_key DESC;
 24 | CREATE UNIQUE INDEX ON _places (run_id);
```

**[sql/marts/01_base.sql](sql/marts/01_base.sql), строки 45–67**

```sql
 45 | CREATE TEMP TABLE _input ON COMMIT DROP AS
 46 | SELECT o.run_id,o.version_id,o.snapshot_id,o.source_version_key,o.observed_at,
 47 |        o.is_orphaned,o.participant_ids,r.category_id,c.game_id,g.name AS game_name,
 48 |        d.name AS category_name,coalesce(d.is_misc,false) AS is_misc,d.rules_text,
 49 |        d.time_direction,CASE d.time_direction WHEN '0' THEN 1 WHEN '1' THEN -1 END AS direction,
 50 |        r.status_code,r.platform_id,r.is_emulated,r.performed_at,r.submitted_at,r.verified_at,
 51 |        r.level_id,r.comment_text,coalesce(pn.name,r.platform_id,'Неизвестно') AS platform_name,
 52 |        CASE g.default_timer_code WHEN '0' THEN 'time' WHEN '1' THEN 'timeWithLoads' WHEN '2' THEN 'igt' END AS timer,
 53 |        t.duration_ns,t.parse_status,p.source_place,p.source_obsolete,
 54 |        array_to_string(o.participant_ids,'+') AS holder_id,
 55 |        coalesce(h.holder_name,array_to_string(o.participant_ids,' + ')) AS holder_name,
 56 |        v.values_key,coalesce(v.values_name,'') AS values_name,
 57 |        v.missing_required,
 58 |        coalesce(video.source_video,'https://www.speedrun.com/runs/' || o.run_id) AS video_url
 59 | FROM _observed_runs o
 60 | JOIN speedrun_3nf.fact_run_version r USING (version_id)
 61 | JOIN speedrun_3nf.dim_category c USING (category_id)
 62 | JOIN _games g ON g.game_id=c.game_id
 63 | JOIN speedrun_3nf.dim_category_definition d ON d.game_version_id=g.game_version_id AND d.category_id=r.category_id
 64 | LEFT JOIN _places p ON p.run_id=o.run_id
 65 | LEFT JOIN speedrun_3nf.dim_platform_definition pn ON pn.game_version_id=g.game_version_id AND pn.platform_id=r.platform_id
 66 | LEFT JOIN speedrun_3nf.dim_run_time t ON t.version_id=r.version_id AND t.source_field=
 67 |     CASE g.default_timer_code WHEN '0' THEN 'time' WHEN '1' THEN 'timeWithLoads' WHEN '2' THEN 'igt' END
```

Поле сравнения определяется игрой, категорией, значениями подкатегории, таймером, направлением результата, версией правил и платформой. В расчёт входят проверенные full-game раны с пригодным временем и известным составом. В одном поле у каждого участника или состава команды остаётся один лучший актуальный результат. `rank()` даёт одинаковое место при ничьей. Цель сравнивает PB с порогом выбранного top-N.

**[sql/marts/01_base.sql](sql/marts/01_base.sql), строки 113–122**

```sql
113 | CREATE TEMP TABLE _eligible ON COMMIT DROP AS
114 | SELECT i.*,f.field_platform,
115 |        md5(concat_ws('|',i.game_id,i.category_id,coalesce(i.values_key,''),i.timer,i.time_direction,
116 |                      md5(coalesce(i.rules_text,'')),f.field_platform)) AS scope_id
117 | FROM _input i
118 | CROSS JOIN LATERAL (SELECT 'all'::text AS field_platform UNION ALL SELECT i.platform_id WHERE i.platform_id IS NOT NULL) f
119 | WHERE i.status_code=1 AND i.is_orphaned IS DISTINCT FROM true AND i.level_id IS NULL
120 |   AND i.duration_ns>0 AND i.direction IS NOT NULL AND cardinality(i.participant_ids)>0
121 |   AND i.missing_required IS DISTINCT FROM true
122 |   AND (i.verified_at<i.submitted_at) IS NOT TRUE;
```

**[sql/marts/01_base.sql](sql/marts/01_base.sql), строки 145–161**

```sql
145 | CREATE TABLE pb AS
146 | WITH best AS (
147 |     SELECT DISTINCT ON (scope_id,holder_id) *
148 |     FROM run WHERE source_obsolete=false
149 |     ORDER BY scope_id,holder_id,score,run_id
150 | ), ranked AS (
151 |     SELECT *,rank() OVER (PARTITION BY scope_id ORDER BY score) AS place,
152 |            count(*) OVER (PARTITION BY scope_id) AS field_size,
153 |            count(*) OVER (PARTITION BY scope_id,score) AS tie_size,
154 |            first_value(seconds) OVER (PARTITION BY scope_id ORDER BY score) AS wr_seconds
155 |     FROM best
156 | )
157 | SELECT *,100.0 * (field_size-place-(tie_size-1)/2.0) / nullif(field_size-1,0) AS strength,
158 |        abs(seconds-wr_seconds) AS gap_seconds,
159 |        100.0*abs(seconds-wr_seconds)/nullif(wr_seconds,0) AS gap_pct
160 | FROM ranked;
161 | CREATE UNIQUE INDEX ON pb (scope_id,holder_id);
```

**[sql/marts/01_base.sql](sql/marts/01_base.sql), строки 175–195**

```sql
175 | CREATE TABLE target AS
176 | SELECT f.scope_id,t.target_code,t.target_n,max(p.score) AS threshold_score,
177 |        max(p.score)*f.direction/1e9::numeric AS threshold_seconds,f.field_size,
178 |        f.field_size>=t.target_n AS sufficient_field
179 | FROM field f
180 | CROSS JOIN LATERAL (
181 |     VALUES ('top3',3::bigint),('top5',5),('top10',10),('top50',50),('top100',100),
182 |            ('top1pct',greatest(1,ceil(f.field_size*0.01)::bigint)),
183 |            ('top25pct',greatest(1,ceil(f.field_size*0.25)::bigint))
184 | ) t(target_code,target_n)
185 | JOIN pb p ON p.scope_id=f.scope_id AND p.place<=t.target_n
186 | GROUP BY f.scope_id,t.target_code,t.target_n,f.field_size,f.direction;
187 | CREATE UNIQUE INDEX ON target (scope_id,target_code);
188 |
189 | CREATE TABLE goal AS
190 | SELECT p.scope_id,p.holder_id,p.holder_name,p.run_id,p.place,p.field_size,p.seconds AS pb_seconds,p.strength,
191 |        t.target_code,t.target_n,t.threshold_seconds,t.sufficient_field,
192 |        greatest(0,p.score-t.threshold_score)/1e9::numeric AS improvement_seconds,
193 |        100.0*greatest(0,p.score-t.threshold_score)/nullif(abs(t.threshold_score),0) AS improvement_pct
194 | FROM pb p JOIN target t USING (scope_id) WHERE t.target_code<>'top25pct';
195 | CREATE INDEX ON goal (holder_id,scope_id,target_code);
```

Медальный зачёт использует призовые места, сохранённые из источника. Для командного рана каждая представленная страна получает один вклад. Неизвестные страны учитываются отдельно. Сортировка по золоту, серебру и бронзе определяет место. Весовая формула с основанием больше максимума серебра и бронзы сохраняет тот же порядок.

**[sql/marts/01_base.sql](sql/marts/01_base.sql), строки 197–219**

```sql
197 | CREATE TABLE country_contribution AS
198 | SELECT DISTINCT i.run_id,i.game_id,i.game_name,i.category_id,i.category_name,i.is_misc,
199 |        i.source_place,coalesce(c.country_code,'UNKNOWN') AS country_code
200 | FROM _input i
201 | LEFT JOIN LATERAL unnest(i.participant_ids) u(player_id) ON true
202 | LEFT JOIN player p USING (player_id)
203 | LEFT JOIN country c USING (country_code)
204 | WHERE i.status_code=1 AND i.source_obsolete=false AND i.source_place BETWEEN 1 AND 3
205 |   AND i.is_orphaned IS DISTINCT FROM true AND i.level_id IS NULL
206 |   AND (i.verified_at<i.submitted_at) IS NOT TRUE;
207 | CREATE INDEX ON country_contribution (game_id,country_code);
208 |
209 | CREATE TABLE country_medals AS
210 | WITH counts AS (
211 |     SELECT country_code,count(*) FILTER (WHERE source_place=1) AS gold,
212 |            count(*) FILTER (WHERE source_place=2) AS silver,count(*) FILTER (WHERE source_place=3) AS bronze
213 |     FROM country_contribution WHERE country_code<>'UNKNOWN' GROUP BY country_code
214 | ), weighted AS (
215 |     SELECT *,1+greatest(max(silver) OVER (),max(bronze) OVER ())::numeric AS base FROM counts
216 | )
217 | SELECT w.*,c.country_name,gold*base*base+silver*base+bronze AS medal_score,
218 |        rank() OVER (ORDER BY gold DESC,silver DESC,bronze DESC) AS overall_rank
219 | FROM weighted w JOIN country c USING (country_code);
```

| Аналитический вопрос | Витрина или запрос | Как читать результат |
| --- | --- | --- |
| Сколько нужно улучшить до top10 или top50? | `pb`, `target`, `goal` | Место и отставание от выбранного порога внутри одного поля |
| Где у раннера есть шанс приблизиться к WR? | [`wr_opportunity`](sql/marts/02_history.sql#L30) | Список полей с малым отставанием и контекстом активности |
| Какие игры часто осваивают участники соседних сообществ? | [`game_portfolio`, `game_similarity`, `game_transition`, `destination_category`](sql/marts/04_recommendations.sql) | Переходы между играми, пересечение аудиторий и свидетельства для рекомендаций |
| За сколько дней сравнимая когорта достигала цели? | [`cohort_member`, `cohort_summary`](sql/marts/03_runner.sql#L71), [`cohort_survival`](src/speedrun_etl/cohort_survival.py) | Календарные дни от первой видимой публикации с учётом неполного наблюдения |
| Как менялась форма раннера? | [`runner_form`](sql/marts/03_runner.sql#L1) | История публикаций и PB, скользящая медиана и квартили |
| Где рекорд улучшился после долгого плато? | [`revival`](sql/marts/02_history.sql#L38) | Длительность плато и величина улучшения в реконструированной истории |
| Где стоит разбирать изменение маршрута? | [`record_event`](src/speedrun_etl/record_history.py), [`source_wr`](sql/marts/05_demo.sql#L22) | Скачки результата и ссылки на видео для ручной проверки гипотезы |
| В каких типах категорий раннер сильнее? | [`scope_tag`, `runner_profile`](sql/marts/03_runner.sql#L40) | Сравнение нормированных результатов по типам с указанием охвата |
| Кто показывает близкие времена в том же поле? | [`runner_window`](sql/marts/03_runner.sql#L24) | Соперники по медианам недавних публикаций и размеру выборки |
| Какие поля активнее за 7, 30 или 90 дней? | [`activity`, `calendar_activity`](sql/marts/02_history.sql) | Сравнение числа публикаций, участников и рекордных событий по окнам |
| Какие страны получают больше призовых мест? | [`country_contribution`, `country_medals`](sql/marts/01_base.sql#L197) | Медальный порядок на срезе или в выбранной игре и категории |
| Кто набрал больше всего баллов в разных играх? | [Виртуальный SQL рейтинга](infra/runner_ranking.sql) | Лучший сольный результат в каждой игре и сумма максимум по десяти играм |

Рейтинг читает опубликованные витрины. Он выбирает по одному зачётному результату в игре, ранжирует игры внутри профиля и суммирует баллы десяти лучших. Верхняя граница шкалы составляет 1000.

**[infra/runner_ranking.sql](infra/runner_ranking.sql), строки 31–54**

```sql
 31 | scored AS MATERIALIZED (
 32 |     SELECT r.*,100.0*opponents_credit/(solo_field_size-1) AS percentile,
 33 |            100.0*opponents_credit/(solo_field_size-1+20) AS result_points,
 34 |            100.0*opponents_credit/(solo_field_size-1+5) AS points_k5,
 35 |            100.0*opponents_credit/(solo_field_size-1+50) AS points_k50
 36 |     FROM relative r CROSS JOIN settings
 37 |     WHERE solo_field_size>=greatest(2,minimum_field_size)
 38 | ),
 39 | ordered_games AS (
 40 |     SELECT DISTINCT ON (holder_id,game_id) *
 41 |     FROM scored
 42 |     ORDER BY holder_id,game_id,result_points DESC,scope_id
 43 | ),
 44 | best_games AS (
 45 |     SELECT *,row_number() OVER(PARTITION BY holder_id ORDER BY result_points DESC,game_id) AS in_portfolio
 46 |     FROM ordered_games
 47 | ),
 48 | portfolios AS (
 49 |     SELECT holder_id,count(*) AS eligible_games,
 50 |            sum(result_points) FILTER(WHERE in_portfolio<=10) AS portfolio_points,
 51 |            string_agg(game_name || ' [' || round(result_points,2)::text || ']', '; ' ORDER BY in_portfolio)
 52 |                FILTER(WHERE in_portfolio<=10) AS portfolio_games
 53 |     FROM best_games GROUP BY holder_id
 54 | ),
```

На контрольном срезе у RealXones было место 196 из 1103, до top10 требовалось улучшить результат на 310 секунд, до top50 — на 240. США получили 914 золотых, 416 серебряных и 398 бронзовых вкладов, Россия заняла четвёртое место. Это выводы по охвату сохранённого архива. [Отчёт и примеры](reports/lab3/REPORT.md)

## 3. Скрипт формирования витрин и связь ETL с DDS

Цепочка Airflow выглядит так: `normalize_entity_v3` → `check_data_quality` → `refresh_marts` → `sync_analytics_metadata`. Нормализация подписана на готовность ODS. При ручном повторе можно передать ID существующей коллекции. Сенсор проверяет receipts, затем задачи загружают нормализованные данные и публикуют событие `DDS_READY` со ссылками на пакеты.

**[dags/dds/normalize_entity_v3.py](dags/dds/normalize_entity_v3.py), строки 13–59**

```python
 13 | @dag(
 14 |     schedule=[Asset(ODS_READY)],
 15 |     catchup=False,
 16 |     max_active_runs=1,
 17 |     default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
 18 |     tags=["dds", "entity-v3"],
 19 | )
 20 | def normalize_entity_v3():
 21 |     @task.sensor(poke_interval=15, timeout=600, mode="reschedule")
 22 |     def verified_snapshots():
 23 |         from airflow.sdk import PokeReturnValue
 24 |
 25 |         from speedrun_etl.dds import verify_snapshot_batches
 26 |
 27 |         events = get_current_context()["triggering_asset_events"]
 28 |         batches = [
 29 |             event.extra["snapshots"] for items in events.values() for event in items
 30 |         ]
 31 |         conf = get_current_context()["dag_run"].conf or {}
 32 |         if not batches and conf.get("collection"):
 33 |             from speedrun_etl.marts import collection_batches
 34 |
 35 |             # Explicit replay of committed ODS receipts; no source tasks are run.
 36 |             batches = collection_batches(conf["collection"])
 37 |         if not batches:
 38 |             raise ValueError(
 39 |                 "DDS requires an ODS event or an explicit existing collection"
 40 |             )
 41 |         return PokeReturnValue(verify_snapshot_batches(batches), batches)
 42 |
 43 |     @task(execution_timeout=timedelta(hours=4), outlets=[Asset(DDS_READY)])
 44 |     def normalize(batches):
 45 |         from speedrun_etl.analytics_dds import load_analytics_batch
 46 |         from speedrun_etl.dds import load_batch
 47 |
 48 |         ddl = Path(
 49 |             os.environ.get(
 50 |                 "DDS_SCHEMA_SQL", "/opt/project/docs/design/entity_v3_3nf.sql"
 51 |             )
 52 |         )
 53 |         results = [load_batch(batch, ddl) for batch in batches]
 54 |         for batch in batches:
 55 |             load_analytics_batch(batch)
 56 |         yield Metadata(Asset(DDS_READY), {"batches": batches})
 57 |         return results
 58 |
 59 |     normalize(verified_snapshots())
```

Проверка качества принимает тот же набор пакетов из события. После успешного `validate_dds` она публикует `DDS_VALIDATED`. DAG витрин подписан на этот Asset и вызывает ту же функцию `build_marts`, что и CLI. Параметры сборки берутся из `dag_run.conf` с резервной коллекцией по умолчанию. После публикации витрин событие `MARTS_READY` запускает отдельную синхронизацию каталога, которую можно повторить независимо.

**[dags/dds/check_data_quality.py](dags/dds/check_data_quality.py), строки 12–47**

```python
 12 | @dag(
 13 |     schedule=[Asset(DDS_READY)],
 14 |     catchup=False,
 15 |     max_active_runs=1,
 16 |     tags=["dds", "data-quality"],
 17 | )
 18 | def check_data_quality():
 19 |     @task(outlets=[Asset(DDS_VALIDATED)])
 20 |     def validate():
 21 |         from airflow.sdk.exceptions import AirflowFailException
 22 |
 23 |         from speedrun_etl.analytics_quality import validate_dds
 24 |
 25 |         events = get_current_context()["triggering_asset_events"]
 26 |         batches = [
 27 |             batch
 28 |             for items in events.values()
 29 |             for event in items
 30 |             for batch in event.extra["batches"]
 31 |         ]
 32 |         conf = get_current_context()["dag_run"].conf or {}
 33 |         if not batches and conf.get("collection"):
 34 |             from speedrun_etl.marts import collection_batches
 35 |
 36 |             batches = collection_batches(conf["collection"])
 37 |         if not batches:
 38 |             raise AirflowFailException(
 39 |                 "Data Quality requires a DDS event or an explicit existing collection"
 40 |             )
 41 |
 42 |         report = validate_dds(batches)
 43 |         logging.getLogger(__name__).info("Data Quality: %s", json.dumps(report))
 44 |         yield Metadata(Asset(DDS_VALIDATED), {**report, "batches": batches})
 45 |         return report
 46 |
 47 |     validate()
```

**[dags/marts/refresh_marts.py](dags/marts/refresh_marts.py), строки 10–29**

```python
 10 | @dag(
 11 |     schedule=[Asset(DDS_VALIDATED)],
 12 |     catchup=False,
 13 |     max_active_runs=1,
 14 |     default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
 15 |     tags=["lab3", "marts"],
 16 | )
 17 | def refresh_marts():
 18 |     @task(outlets=[Asset(MARTS_READY)], execution_timeout=timedelta(hours=4))
 19 |     def refresh():
 20 |         from speedrun_etl.marts import DEFAULT_COLLECTION, build_marts
 21 |
 22 |         conf = get_current_context()["dag_run"].conf or {}
 23 |         result = build_marts(
 24 |             conf.get("collection", DEFAULT_COLLECTION), conf.get("as_of")
 25 |         )
 26 |         yield Metadata(Asset(MARTS_READY), result)
 27 |         return result
 28 |
 29 |     refresh()
```

**[dags/marts/sync_analytics_metadata.py](dags/marts/sync_analytics_metadata.py), строки 10–24**

```python
 10 | @dag(
 11 |     schedule=[Asset(MARTS_READY)],
 12 |     catchup=False,
 13 |     max_active_runs=1,
 14 |     default_args={"retries": 3, "retry_delay": timedelta(minutes=1)},
 15 |     tags=["lab3", "metadata"],
 16 | )
 17 | def sync_analytics_metadata():
 18 |     @task(execution_timeout=timedelta(minutes=30))
 19 |     def synchronize():
 20 |         from speedrun_etl.analytics_metadata import synchronize
 21 |
 22 |         return synchronize()
 23 |
 24 |     synchronize()
```

`build_marts` перечитывает подтверждённые receipts, проверяет DDS и сортирует SQL-файлы по имени. В транзакции `REPEATABLE READ` берётся блокировка загрузки и сверяется ключ качества. Снимки, дата среза и версия расчётов определяют ID сборки. Повтор с тем же ID использует готовую физическую схему, при необходимости снова публикуя её.

**[src/speedrun_etl/marts.py](src/speedrun_etl/marts.py), строки 22–39**

```python
 22 | def collection_batches(collection):
 23 |     """Select completed game chains and require all four PostgreSQL receipts."""
 24 |     with pg_connect(True) as conn:
 25 |         entries = conn.execute(
 26 |             """SELECT q.collection_id, COALESCE((r.summary->'runs'->>'count')::bigint, 0)
 27 |                FROM entity_batch_queue q
 28 |                JOIN entity_receipts r ON r.snapshot_id=q.collection_id || '-runs'
 29 |                     AND r.store='postgres' AND r.namespace='entity_v3'
 30 |                WHERE q.batch_id=%s AND q.state='complete'
 31 |                ORDER BY 2, q.collection_id""",
 32 |             (collection,),
 33 |         ).fetchall()
 34 |     if not entries:
 35 |         raise ValueError(f"No completed PostgreSQL chains for {collection}")
 36 |     batches = [{stage: key + "-" + stage for stage in STAGES} for key, _ in entries]
 37 |     if not verify_snapshot_batches(batches):
 38 |         raise ValueError("The collection has missing PostgreSQL receipts")
 39 |     return batches
```

**[src/speedrun_etl/marts.py](src/speedrun_etl/marts.py), строки 100–125**

```python
100 | def build_marts(collection=DEFAULT_COLLECTION, as_of=None):
101 |     from .analytics_quality import quality_key, validate_dds
102 |     from .cohort_survival import build_survival
103 |     from .record_history import build_history
104 |
105 |     batches = collection_batches(collection)
106 |     quality = validate_dds(batches)
107 |     snapshots = sorted({value for batch in batches for value in batch.values()})
108 |     scripts = sorted((PROJECT / "sql/marts").glob("*.sql"))
109 |     method = digest(
110 |         [p.read_text(encoding="utf-8") for p in scripts]
111 |         + [
112 |             (Path(__file__).parent / name).read_text(encoding="utf-8")
113 |             for name in (
114 |                 "record_history.py",
115 |                 "cohort_survival.py",
116 |                 "analytics_dds.py",
117 |                 "marts.py",
118 |             )
119 |         ]
120 |     )
121 |     with pg_connect() as conn:
122 |         conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
123 |         conn.execute("SELECT pg_advisory_xact_lock(hashtext('speedrun_3nf_load'))")
124 |         if quality_key(conn, batches) != quality["quality_key"]:
125 |             raise ValueError("DDS changed after validation. Retry the build.")
```

**[src/speedrun_etl/marts.py](src/speedrun_etl/marts.py), строки 135–163**

```python
135 |         cutoff = (
136 |             datetime.fromisoformat(as_of)
137 |             if as_of
138 |             else conn.execute(
139 |                 "SELECT max(observed_until) FROM speedrun_3nf.snapshot_coverage WHERE snapshot_id=ANY(%s)",
140 |                 (snapshots,),
141 |             ).fetchone()[0]
142 |         )
143 |         if cutoff is None or cutoff.tzinfo is None:
144 |             raise ValueError("The cut-off must be an observed timestamp with timezone")
145 |         build_id = digest([snapshots, cutoff.isoformat(), method])
146 |         schema = "speedrun_build_" + build_id[:16]
147 |         conn.execute("CREATE SCHEMA IF NOT EXISTS speedrun_marts")
148 |         conn.execute("""CREATE TABLE IF NOT EXISTS speedrun_marts.build_registry (
149 |             build_id text PRIMARY KEY,physical_schema text NOT NULL,as_of timestamptz NOT NULL,
150 |             collection_id text NOT NULL,method_version text NOT NULL,snapshots jsonb NOT NULL,
151 |             published_at timestamptz NOT NULL DEFAULT now()
152 |         )""")
153 |         if conn.execute(
154 |             "SELECT 1 FROM speedrun_marts.build_registry WHERE build_id=%s", (build_id,)
155 |         ).fetchone():
156 |             active = conn.execute(
157 |                 "SELECT build_id FROM speedrun_marts.context"
158 |             ).fetchone()[0]
159 |             if active != build_id:
160 |                 publish_build(
161 |                     conn, schema, build_id, cutoff, collection, method, snapshots
162 |                 )
163 |             return {"build_id": build_id, "skipped": True}
```

Внутри новой схемы SQL создаёт таблицы, Python реконструирует историю рекордов и строит кривые достижения цели. Затем проверяются инварианты PB, когорт, медалей и покрытия. Только после успешных проверок в той же транзакции переключаются постоянные представления `speedrun_marts`. При ошибке транзакция откатывается, поэтому предыдущий опубликованный срез остаётся доступен.

**[src/speedrun_etl/marts.py](src/speedrun_etl/marts.py), строки 180–205**

```python
180 |         conn.execute(
181 |             "CREATE TEMP TABLE _snapshots (snapshot_id text PRIMARY KEY) ON COMMIT DROP"
182 |         )
183 |         with conn.cursor().copy("COPY _snapshots FROM STDIN") as copy:
184 |             for snapshot in snapshots:
185 |                 copy.write_row((snapshot,))
186 |         for script in scripts:
187 |             LOG.info("Building %s", script.name)
188 |             conn.execute(script.read_text(encoding="utf-8"))
189 |             if script.name == "01_base.sql":
190 |                 LOG.info("Reconstructing record events")
191 |                 build_history(conn)
192 |         build_survival(conn)
193 |         # Planner statistics must be available before readers see the new build.
194 |         # In particular, context is one row, not the default estimate of 610.
195 |         for (table,) in conn.execute(
196 |             "SELECT tablename FROM pg_tables WHERE schemaname=%s", (schema,)
197 |         ).fetchall():
198 |             conn.execute(
199 |                 sql.SQL("ANALYZE {}.{}").format(
200 |                     sql.Identifier(schema), sql.Identifier(table)
201 |                 )
202 |             )
203 |         validate_build(conn)
204 |         publish_build(conn, schema, build_id, cutoff, collection, method, snapshots)
205 |         return {"build_id": build_id, "as_of": cutoff.isoformat(), "schema": schema}
```

**[src/speedrun_etl/marts.py](src/speedrun_etl/marts.py), строки 208–227**

```python
208 | def validate_build(conn):
209 |     """Fail before atomic publication if a ranking or temporal invariant breaks."""
210 |     checks = {
211 |         "empty_pb": "SELECT (count(*)=0)::int FROM pb",
212 |         "invalid_pb": "SELECT count(*) FROM pb WHERE place<1 OR place>field_size OR strength NOT BETWEEN 0 AND 100",
213 |         "invalid_cohort": "SELECT count(*) FROM cohort_member WHERE days_to_target<0 OR followup_days<0 OR days_to_target>followup_days",
214 |         "invalid_survival": "SELECT count(*) FROM cohort_survival WHERE reached_pct NOT BETWEEN 0 AND 100 OR at_risk<reached_people+censored_people",
215 |         "medal_order": "SELECT count(*) FROM (SELECT overall_rank,rank() OVER (ORDER BY medal_score DESC) r FROM country_medals) q WHERE r<>overall_rank",
216 |         "missing_manifest": "SELECT count(*) FROM _snapshots s LEFT JOIN speedrun_3nf.snapshot_coverage c USING(snapshot_id) WHERE c.snapshot_id IS NULL OR c.source_coverage='[]'::jsonb",
217 |     }
218 |     results = {
219 |         name: int(conn.execute(query).fetchone()[0]) for name, query in checks.items()
220 |     }
221 |     conn.execute(
222 |         "CREATE TABLE validation (check_name text PRIMARY KEY,failures bigint NOT NULL)"
223 |     )
224 |     for name, failures in results.items():
225 |         conn.execute("INSERT INTO validation VALUES (%s,%s)", (name, failures))
226 |     if any(results.values()):
227 |         raise ValueError(f"Mart validation failed: {results}")
```

**[src/speedrun_etl/marts.py](src/speedrun_etl/marts.py), строки 238–258**

```python
238 |     for table in tables:
239 |         if table == "context":
240 |             query = sql.SQL(
241 |                 "CREATE OR REPLACE VIEW speedrun_marts.context AS SELECT * FROM {}.context"
242 |             ).format(sql.Identifier(schema))
243 |         else:
244 |             query = sql.SQL("""CREATE OR REPLACE VIEW speedrun_marts.{} AS
245 |                 SELECT c.build_id,c.as_of,t.* FROM {}.{} t CROSS JOIN {}.context c""").format(
246 |                 sql.Identifier(table),
247 |                 sql.Identifier(schema),
248 |                 sql.Identifier(table),
249 |                 sql.Identifier(schema),
250 |             )
251 |         conn.execute(query)
252 |     conn.execute(
253 |         """INSERT INTO speedrun_marts.build_registry
254 |         (build_id,physical_schema,as_of,collection_id,method_version,snapshots)
255 |         VALUES (%s,%s,%s,%s,%s,%s)
256 |         ON CONFLICT (build_id) DO UPDATE SET published_at=now()""",
257 |         (build_id, schema, cutoff, collection, method, Jsonb(snapshots)),
258 |     )
```

## 4. Дашборды Superset и выбор визуализаций

Provisioning создаёт виртуальный dataset из SQL, привязывает к нему график нужного типа и включает график в опубликованный дашборд. Постоянные UUID позволяют повторять настройку без дубликатов. Компоновка и native filters задаются в коде. На дашборде «Раннер» можно менять профиль, поле и цель.

На одном дашборде «Раннер» есть как минимум пять разных типов.

| Визуализация | Зачем нужна |
| --- | --- |
| Таблица «Следующая цель» | Даёт точные значения PB, порога и улучшения в секундах и процентах |
| Линейный график «Публикации, PB и форма за 10 ранов» | Показывает, когда изменился PB, и помогает сопоставить отдельные публикации со скользящей медианой и квартилями |
| Тепловая карта Jaccard | Позволяет быстро найти пары игр с сильным пересечением сообществ в матрице от 0 до 1 |
| Box plot последних десяти ранов | Показывает медиану, разброс и крайние результаты в выбранном поле |
| Радар профиля | Сопоставляет шесть типов категорий на общей шкале 0–100. При неполных данных соседняя таблица показывает доступные оценки и охват |

На дашборде «Борьба за рекорды» временные графики показывают последовательность улучшений, а календарная карта — концентрацию активности. На дашборде «Страны» горизонтальные составные столбцы показывают одновременно число призовых вкладов и состав медалей, таблица даёт точное место, крупный показатель фиксирует объём вкладов с неизвестной страной. «Рейтинг раннеров» использует таблицы для точных сравнений места, баллов и оснований оценки.

**[infra/superset_assets.py](infra/superset_assets.py), строки 343–355**

```python
343 |     def table(key, title, cols, order=None):
344 |         return (
345 |             key,
346 |             title,
347 |             key,
348 |             "table",
349 |             {
350 |                 "query_mode": "raw",
351 |                 "all_columns": cols,
352 |                 "order_by_cols": [f'["{order[0]}", {str(order[1]).lower()}]']
353 |                 if order
354 |                 else [],
355 |                 "page_length": 15,
```

**[infra/superset_assets.py](infra/superset_assets.py), строки 368–400**

```python
368 |     def line(key, title, x, metrics, **extra):
369 |         return (
370 |             key,
371 |             title,
372 |             key,
373 |             "echarts_timeseries_line",
374 |             {
375 |                 "x_axis": x,
376 |                 "metrics": [metric(m) for m in metrics],
377 |                 "groupby": [],
378 |                 "time_grain_sqla": None,
379 |                 "show_legend": True,
380 |                 "markerEnabled": True,
381 |                 "row_limit": 100000,
382 |                 **extra,
383 |             },
384 |         )
385 |
386 |     result = [
387 |         table(
388 |             "goals",
389 |             "01 · Следующая цель",
390 |             [
391 |                 "target_code",
392 |                 "place",
393 |                 "field_size",
394 |                 "pb_seconds",
395 |                 "threshold_seconds",
396 |                 "improvement_seconds",
397 |                 "improvement_pct",
398 |                 "sufficient_field",
399 |             ],
400 |         ),
```

**[infra/superset_assets.py](infra/superset_assets.py), строки 451–464**

```python
451 |         (
452 |             "similarity",
453 |             "Пересечение сообществ · Jaccard",
454 |             "similarity",
455 |             "heatmap_v2",
456 |             {
457 |                 "x_axis": "destination_name",
458 |                 "groupby": "source_name",
459 |                 "metric": metric("jaccard"),
460 |                 "normalize_across": "heatmap_v2",
461 |                 "show_legend": True,
462 |                 "value_bounds": [0, 1],
463 |             },
464 |         ),
```

**[infra/superset_assets.py](infra/superset_assets.py), строки 488–527**

```python
488 |         line(
489 |             "form",
490 |             "05 · Публикации, PB и форма за 10 ранов",
491 |             "submitted_at",
492 |             ["seconds", "pb_seconds", "median_seconds", "q1_seconds", "q3_seconds"],
493 |             x_axis_title="Дата подачи",
494 |             y_axis_title="Время, с",
495 |         ),
496 |         (
497 |             "recent_distribution",
498 |             "Разброс последних 10 опубликованных ранов",
499 |             "recent_distribution",
500 |             "box_plot",
501 |             {
502 |                 "groupby": ["holder_name"],
503 |                 "metrics": [metric("seconds", "AVG")],
504 |                 "columns": ["run_id"],
505 |                 "whiskerOptions": "Tukey",
506 |             },
507 |         ),
508 |         (
509 |             "profile",
510 |             "08 · Паутинка · при наличии всех 6 осей",
511 |             "profile",
512 |             "radar",
513 |             {
514 |                 "groupby": ["holder_name"],
515 |                 "metrics": [metric(col, label=tag) for col, tag in PROFILE_AXES],
516 |                 "show_legend": True,
517 |                 "column_config": {
518 |                     tag: {"radarMetricMinValue": 0, "radarMetricMaxValue": 100}
519 |                     for _, tag in PROFILE_AXES
520 |                 },
521 |             },
522 |         ),
523 |         table(
524 |             "profile_values",
525 |             "Профиль: оценки и наличие данных",
526 |             ["tag", "strength", "game_count", "scope_count", "availability"],
527 |         ),
```

**[infra/superset_assets.py](infra/superset_assets.py), строки 659–684**

```python
659 |         (
660 |             "medal_bars",
661 |             "Призовые места по странам",
662 |             "medals",
663 |             "echarts_timeseries_bar",
664 |             {
665 |                 "x_axis": "country_name",
666 |                 "metrics": [metric(x) for x in ("gold", "silver", "bronze")],
667 |                 "groupby": [],
668 |                 "stack": "Stack",
669 |                 "show_legend": True,
670 |                 "row_limit": 20,
671 |                 "order_desc": True,
672 |                 "orientation": "horizontal",
673 |             },
674 |         ),
675 |         (
676 |             "unknown_medals",
677 |             "Призовые вклады без известной страны",
678 |             "unknown_medals",
679 |             "big_number_total",
680 |             {
681 |                 "metric": metric("unknown_contributions"),
682 |                 "subheader": "Не участвуют в рейтинге стран",
683 |             },
684 |         ),
```

**[infra/provision_superset.py](infra/provision_superset.py), строки 273–287**

```python
273 |         for key, query in datasets().items():
274 |             table = (
275 |                 db.session.query(SqlaTable)
276 |                 .filter_by(uuid=uid("dataset", key))
277 |                 .one_or_none()
278 |             )
279 |             if table is None:
280 |                 table = SqlaTable(
281 |                     uuid=uid("dataset", key),
282 |                     table_name="lab3_" + key,
283 |                     database=database,
284 |                     schema="speedrun_marts",
285 |                 )
286 |                 db.session.add(table)
287 |             table.sql = query.strip()
```

**[infra/provision_superset.py](infra/provision_superset.py), строки 300–308**

```python
300 |         for key, title, source, viz, settings in charts():
301 |             chart = (
302 |                 db.session.query(Slice).filter_by(uuid=uid("chart", key)).one_or_none()
303 |             )
304 |             if chart is None:
305 |                 chart = Slice(uuid=uid("chart", key))
306 |                 db.session.add(chart)
307 |             chart.slice_name, chart.viz_type = title, viz
308 |             chart.datasource_id, chart.datasource_type = tables[source].id, "table"
```

**[infra/provision_superset.py](infra/provision_superset.py), строки 328–342**

```python
328 |             chart.params = json.dumps(form, ensure_ascii=False)
329 |             chart.query_context = None
330 |             slices[key] = chart
331 |         for key, (title, members) in DASHBOARDS.items():
332 |             dash = (
333 |                 db.session.query(Dashboard)
334 |                 .filter_by(uuid=uid("dashboard", key))
335 |                 .one_or_none()
336 |             )
337 |             if dash is None:
338 |                 dash = Dashboard(uuid=uid("dashboard", key))
339 |                 db.session.add(dash)
340 |             dash.dashboard_title, dash.slug, dash.published = title, "lab3-" + key, True
341 |             dash.owners, dash.slices = [admin], [slices[x] for x in members]
342 |             ids = [slices[x].id for x in members]
```

**[infra/provision_superset.py](infra/provision_superset.py), строки 480–501**

```python
480 |             note = (
481 |                 f"**Срез: {context['as_of']} · {context['collection_id']}**\n\n"
482 |                 + NOTES[key]
483 |             )
484 |             dash.position_json = json.dumps(
485 |                 layout(title, dash.slices, note), ensure_ascii=False
486 |             )
487 |             dash.json_metadata = json.dumps(
488 |                 {
489 |                     "native_filter_configuration": filters,
490 |                     "color_scheme": "supersetColors",
491 |                     "refresh_frequency": 0,
492 |                     "expanded_slices": {},
493 |                     "label_colors": {},
494 |                 },
495 |                 ensure_ascii=False,
496 |             )
497 |         db.session.commit()
498 |         return {
499 |             "datasets": len(tables),
500 |             "charts": len(slices),
501 |             "dashboards": len(DASHBOARDS),
```

В финальном браузерном прогоне загрузились 29 графиков четырёх дашбордов, включая 14 на «Раннере». Проверки рейтинга показали, что поиск раннера сохраняет его общее место, а изменение минимального размера поля пересчитывает охват. [Протоколы и снимки](reports/lab3/runner_ranking10_20261127/)

## 5. Data Lineage в OpenMetadata

Граф начинается с исходных JSON и манифестов Speedrun.com, сохранённых в PostgreSQL в схеме `entity_v3`. Скрипт регистрирует таблицы и колонки по `information_schema` для ODS, детального слоя `speedrun_3nf` и опубликованных представлений `speedrun_marts`. Четыре DAG регистрируются как pipeline. Для перехода из ODS сопоставление задано в коде, поскольку часть исходных данных разбирается Python-загрузчиком. Зависимости витрин заданы отдельно. Для таблиц, на которые ссылается виртуальный SQL, добавляются связи с дашбордами.

**[src/speedrun_etl/analytics_metadata.py](src/speedrun_etl/analytics_metadata.py), строки 197–226**

```python
197 |     catalog.put(
198 |         "services/pipelineServices",
199 |         {"name": "speedrun_airflow", "serviceType": "Airflow"},
200 |     )
201 |     pipelines = {}
202 |     for name in (
203 |         "normalize_entity_v3",
204 |         "check_data_quality",
205 |         "refresh_marts",
206 |         "sync_analytics_metadata",
207 |     ):
208 |         pipelines[name] = catalog.put(
209 |             "pipelines",
210 |             {
211 |                 "name": name,
212 |                 "service": "speedrun_airflow",
213 |                 "sourceUrl": "http://localhost:8080/dags/" + name,
214 |                 "description": "Код: dags/; без запуска сбора из источника.",
215 |             },
216 |         )
217 |     tables = {}
218 |     with pg_connect() as conn:
219 |         build = conn.execute(
220 |             "SELECT build_id,as_of,collection_id FROM speedrun_marts.context"
221 |         ).fetchone()
222 |         rows = conn.execute("""
223 |             SELECT table_schema,table_name,column_name,data_type,udt_name,ordinal_position
224 |             FROM information_schema.columns WHERE table_schema IN ('entity_v3','speedrun_3nf','speedrun_marts')
225 |             ORDER BY table_schema,table_name,ordinal_position
226 |         """).fetchall()
```

**[src/speedrun_etl/analytics_metadata.py](src/speedrun_etl/analytics_metadata.py), строки 267–296**

```python
267 |     def lineage(
268 |         source, destination, pipeline="refresh_marts", columns=None, expression=""
269 |     ):
270 |         nonlocal edges
271 |         a, b = tables[source], tables[destination]
272 |         details = {
273 |             "pipeline": reference(pipelines[pipeline], "pipeline"),
274 |             "description": expression
275 |             or "Воспроизводимое преобразование из репозитория ЛР3.",
276 |         }
277 |         if columns:
278 |             details["columnsLineage"] = [
279 |                 {
280 |                     "fromColumns": [a["fullyQualifiedName"] + "." + x for x in origins],
281 |                     "toColumn": b["fullyQualifiedName"] + "." + target,
282 |                     "function": formula,
283 |                 }
284 |                 for origins, target, formula in columns
285 |             ]
286 |         catalog.put(
287 |             "lineage",
288 |             {
289 |                 "edge": {
290 |                     "fromEntity": reference(a, "table"),
291 |                     "toEntity": reference(b, "table"),
292 |                     "lineageDetails": details,
293 |                 }
294 |             },
295 |         )
296 |         edges += 1
```

**[src/speedrun_etl/analytics_metadata.py](src/speedrun_etl/analytics_metadata.py), строки 298–325**

```python
298 |     # Table lineage for DDS is explicit rather than pretending SQL parses JSON loaders.
299 |     for key in sorted(tables):
300 |         if not key.startswith("speedrun_3nf."):
301 |             continue
302 |         name = key.split(".")[1]
303 |         if "game" in name or name.startswith(
304 |             ("dim_category", "dim_variable", "dim_value", "dim_platform")
305 |         ):
306 |             source = "games"
307 |         elif "player" in name:
308 |             source = "players"
309 |         elif "wr_" in name:
310 |             source = "wr_history"
311 |         elif "leaderboard" in name:
312 |             source = "checks"
313 |         elif "run" in name:
314 |             source = "runs"
315 |         elif name in ("snapshot_coverage", "analytics_batch"):
316 |             source = "manifests"
317 |         else:
318 |             source = "checks"
319 |         if "entity_v3." + source in tables:
320 |             lineage(
321 |                 "entity_v3." + source,
322 |                 key,
323 |                 "normalize_entity_v3",
324 |                 expression="Decode entity-v3 raw dictionary, preserve source ID and observed_at; src/speedrun_etl/dds.py and analytics_dds.py.",
325 |             )
```

На примере медалей маршрут проходит от `entity_v3.checks` через `speedrun_3nf.leaderboard_observation` и `speedrun_marts.country_contribution` к `speedrun_marts.country_medals`. Страна участника добавляется по отдельной ветке профилей. На последнем переходе из `source_place` выводятся `gold` и `medal_score`. Запись ребра содержит ID сущностей, pipeline и формулу. Скрипт затем читает граф по API и проверяет наличие отображений колонок.

**[src/speedrun_etl/analytics_metadata.py](src/speedrun_etl/analytics_metadata.py), строки 348–359**

```python
348 |         "pb": ["run"],
349 |         "field": ["scope", "pb"],
350 |         "target": ["field", "pb"],
351 |         "goal": ["pb", "target"],
352 |         "country_contribution": [
353 |             "speedrun_3nf.leaderboard_observation",
354 |             "speedrun_3nf.fact_run_version",
355 |             "speedrun_3nf.run_observation",
356 |             "player",
357 |             "country",
358 |         ],
359 |         "country_medals": ["country_contribution", "country"],
```

**[src/speedrun_etl/analytics_metadata.py](src/speedrun_etl/analytics_metadata.py), строки 466–484**

```python
466 |         (
467 |             "speedrun_marts.country_contribution",
468 |             "speedrun_marts.country_medals",
469 |             [
470 |                 (["source_place"], "gold", "count(*) FILTER(WHERE source_place=1)"),
471 |                 (["source_place"], "medal_score", "G*B^2+S*B+Br; B=1+max(S,Br)"),
472 |             ],
473 |             "One source podium run per represented country.",
474 |         ),
475 |     ):
476 |         lineage(
477 |             source,
478 |             destination,
479 |             "normalize_entity_v3"
480 |             if source.startswith("entity_v3")
481 |             else "refresh_marts",
482 |             mappings,
483 |             formula,
484 |         )
```

**[src/speedrun_etl/analytics_metadata.py](src/speedrun_etl/analytics_metadata.py), строки 512–547**

```python
512 |         chart_sources = {item[0]: item[2] for item in assets.charts()}
513 |         sources = set()
514 |         for chart_key in chart_keys:
515 |             query = assets.datasets()[chart_sources[chart_key]]
516 |             sources.update(re.findall(r"speedrun_marts\.([a-z_]+)", query))
517 |         for source in sorted(sources):
518 |             catalog.put(
519 |                 "lineage",
520 |                 {
521 |                     "edge": {
522 |                         "fromEntity": reference(
523 |                             tables["speedrun_marts." + source], "table"
524 |                         ),
525 |                         "toEntity": reference(dashboard, "dashboard"),
526 |                         "lineageDetails": {
527 |                             "pipeline": reference(
528 |                                 pipelines["sync_analytics_metadata"], "pipeline"
529 |                             ),
530 |                             "description": "Superset virtual SQL: infra/superset_assets.py; stable dataset UUIDs from infra/provision_superset.py.",
531 |                         },
532 |                     }
533 |                 },
534 |             )
535 |             edges += 1
536 |     proof = catalog.request(
537 |         "GET",
538 |         "lineage/table/"
539 |         + tables["speedrun_marts.country_medals"]["id"]
540 |         + "?upstreamDepth=3&downstreamDepth=2",
541 |     )
542 |     column_edges = sum(
543 |         len(edge.get("lineageDetails", {}).get("columnsLineage", []))
544 |         for edge in proof.get("upstreamEdges", [])
545 |     )
546 |     if column_edges < 2:
547 |         raise ValueError("Readback did not confirm medal column lineage")
```

В протоколе 27 ноября зарегистрированы 83 таблицы и представления, 4 pipeline, 4 дашборда и 158 связей. Обратное чтение подтвердило четыре отображения колонок во входном графе `country_medals`. [Протокол синхронизации](reports/lab3/runner_ranking10_20261127/checks.json) · [Снимок графа при приёмке](reports/lab3/acceptance_20261126/screenshots/lineage.png)

## 6. Отчёт, исходники и демонстрация

[Отчёт ЛР3](reports/lab3/REPORT.md) содержит архитектуру, методику, результаты и ограничения интерпретации. [Сценарий защиты](reports/lab3/DEMO.md) содержит команды запуска, адреса интерфейсов и проверенные примеры. Исходные файлы доступны целиком.

| Что показать | Где находится исходный код |
| --- | --- |
| Базовая схема DDS и дополнения ЛР3 | [entity_v3_3nf.sql](docs/design/entity_v3_3nf.sql), [lab3_dds.sql](sql/lab3_dds.sql), [загрузчик наблюдений](src/speedrun_etl/analytics_dds.py) |
| Формулы витрин | [Все SQL-скрипты](sql/marts/), [рейтинг раннеров](infra/runner_ranking.sql), [реконструкция событий](src/speedrun_etl/record_history.py), [расчёт кривых](src/speedrun_etl/cohort_survival.py) |
| ETL и контроль качества | [CLI и сборка](src/speedrun_etl/marts.py), [нормализация](dags/dds/normalize_entity_v3.py), [проверка DDS](dags/dds/check_data_quality.py), [публикация](dags/marts/refresh_marts.py), [синхронизация](dags/marts/sync_analytics_metadata.py) |
| Интерфейсы и каталог | [Compose](compose.yaml), [конфигурация графиков](infra/superset_assets.py), [provisioning](infra/provision_superset.py), [публикация lineage](src/speedrun_etl/analytics_metadata.py) |

CLI позволяет подготовить DDS, дополнить его наблюдениями и сформировать витрины на сохранённых данных. Для защиты можно показать успешную цепочку DAG в Airflow, выбрать раннера в Superset, пройти от медальной витрины к исходным наблюдениям в OpenMetadata и выполнить запрос к опубликованным представлениям в PostgreSQL.

**[src/speedrun_etl/marts.py](src/speedrun_etl/marts.py), строки 72–97**

```python
 72 | def main():
 73 |     parser = argparse.ArgumentParser(description=__doc__)
 74 |     parser.add_argument(
 75 |         "command", choices=["prepare-dds", "prepare-analytics", "build"]
 76 |     )
 77 |     parser.add_argument("--collection", default=DEFAULT_COLLECTION)
 78 |     parser.add_argument("--as-of")
 79 |     args = parser.parse_args()
 80 |     logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
 81 |     if args.command == "prepare-dds":
 82 |         result = prepare_dds(args.collection)
 83 |     elif args.command == "prepare-analytics":
 84 |         from .analytics_dds import load_analytics_batch
 85 |
 86 |         batches = collection_batches(args.collection)
 87 |         for number, batch in enumerate(batches, 1):
 88 |             LOG.info(
 89 |                 "Analytics DDS %s/%s %s",
 90 |                 number,
 91 |                 len(batches),
 92 |                 load_analytics_batch(batch),
 93 |             )
 94 |         result = {"batches": len(batches)}
 95 |     else:
 96 |         result = build_marts(args.collection, args.as_of)
 97 |     print(json.dumps(result, default=str), flush=True)
```

Пример запросов для демонстрации в PostgreSQL.

```sql
SELECT build_id, as_of, collection_id
FROM speedrun_marts.context;

SELECT country_name, gold, silver, bronze, overall_rank
FROM speedrun_marts.country_medals
ORDER BY overall_rank, country_name
LIMIT 10;
```

На приёмке 26 ноября подтверждены успешный повтор всей аналитической цепочки, пять интеграционных тестов ЛР3, откат ошибочной сборки и отсутствие дублей при повторе. Обновление рейтинга 27 ноября прошло три SQL-теста и два сценария фильтров. [Приёмка исходной цепочки](reports/lab3/acceptance_20261126/checks.json) · [Приёмка рейтинга](reports/lab3/runner_ranking10_20261127/checks.json)
