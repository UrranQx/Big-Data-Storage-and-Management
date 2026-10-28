# Структура рабочего проекта ЛР1

Текущая модель — entity-v2: игры, раны, игроки и история WR. Правила модели —
DATA_MODEL.md; фактическое состояние автора — docs/HANDOFF.md; переносимый
запуск с нуля — docs/TEAM_HANDOFF.md. Стек и три DAG уже реализованы.

| Место | Назначение |
|---|---|
| compose.yaml, infra/ | Docker Compose, один образ Airflow, init, локальная генерация секретов |
| dags/ods/ | Три тонких DAG games_metadata, runs, players |
| src/speedrun_etl/source.py | Общий HTTP-клиент, до 4 страниц в пакете, pacing, retries, gzip, телеметрия |
| http_probe.py, http_transport.py | Ограниченные исследования GET/keep-alive/POST и окон, не стартовая команда сбора |
| v1_research.py | Ограниченный публичный /runs v1 через общий limiter, только исследование, не источник действующих DAG |
| collect.py, catalog.py, ranking.py | Метаданные, разделы ранов, WR, каталог и рейтинг |
| date_partitions.py, repair.py | Явный repair неполных разделов, диапазоны дат, сохранение base и подтверждение очереди |
| entity_ods.py, entity_runtime.py | Версии полных сущностей, checks, упаковка и профили участников |
| entity_storage.py | Актуальный DDL entity_v2, пакетные PG/Mongo-загрузки, S3-блоки, readback |
| entity_workflow.py, workflow.py | Задачи и зависимости Airflow, подтверждение трёх загрузок |
| batch.py | Переносимая очередь top-N / all / выборка четырёх групп, сохранение прогресса в control PG |
| benchmark.py | Актуальные замеры entity-v2 в трёх изолированных пространствах, без HTTP источника |
| storage_workloads.py | Дополнительные фиксированные опыты запросов/индексов/DDL/CRUD/COPY только в test_wl_*, не DDS |
| forecast.py | Прогноз ресурсов по завершённой выборке из четырёх групп; не обещает полноту сайта |
| cli.py, progress.py | Ручной старт, доказательства, replay, чтение сущности и мониторинг |
| tests/ | Фикстуры API, история, пагинация, limiter, контроль очереди, реальные три ODS |
| reports/lab1/ | Отчёт, методика, сохранённые небольшие доказательства и результаты |
| sql/ods.sql | Прежняя схема четырёх технических наборов, не текущий production DDL |
| archive/ | Исследовательские прототипы; не подключаются к DAG или рабочему образу |
| data/, exports/ | Локальные данные и пакеты передачи, исключены из Git |

Исходники legacy ods.py/storage.py и прототип repack.py сохранены для истории
проверок; текущие DAG их не используют для ODS-загрузки. Старый queue_driver
отключён; прежний benchmark.py заменён реализацией entity-v2. fresh_cycle.py — одноразовая
последовательность на стенде автора, не команда для компьютера коллеги.

Состояние и raw находятся в named Docker volumes. В Git и пакет передачи
не включаются .env, infra/garage.local.toml, volumes, полный staging и архив
archive/backups/data.zip. .env.example содержит только безопасные заглушки.

ЛР2 (DDS/SCD/Data Quality) и ЛР3 (витрины/Superset/OpenMetadata) остаются
следующими этапами. Их заготовки в структуре не означают развёрнутые сервисы.
GitHub: https://github.com/UrranQx/Big-Data-Storage-and-Management, main.
Первичная публикация выполнена; новые локальные изменения не считаются
опубликованными до отдельного успешного push.
