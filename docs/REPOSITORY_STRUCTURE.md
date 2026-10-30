# Структура рабочего проекта ЛР1

Текущая модель — entity-v3 (чтение entity-v2 сохраняется): игры, раны, игроки и история WR. Правила модели —
DATA_MODEL.md; фактическое состояние автора — docs/HANDOFF.md; переносимый
запуск с нуля — docs/TEAM_HANDOFF.md. Стек и четыре DAG уже реализованы.

| Место | Назначение |
|---|---|
| compose.yaml, infra/ | Docker Compose, один образ Airflow, init, локальная генерация секретов |
| infra/cold_backup.py, backup_inventory.py, verify_cold_backup.py | Холодная копия шести volumes/кода/образов на другой диск, baseline и реальное изолированное восстановление; без wipe и source HTTP |
| dags/ods/ | Четыре тонких DAG games_metadata, runs, wr_history, players |
| model.py, wr.py | Два явных контрактаODS, читаемыеIDзапусков, отдельный сбор WR/coverage |
| src/speedrun_etl/source.py | Общий HTTP-клиент, до 4 страниц в пакете, pacing, retries, gzip, телеметрия |
| http_probe.py, http_transport.py | Ограниченные исследования GET/keep-alive/POST и окон, не стартовая команда сбора |
| v1_research.py | Ограниченный публичный /runs v1 через общий limiter, только исследование, не источник действующих DAG |
| collect.py, catalog.py, ranking.py | Метаданные, разделы ранов, WR, каталог и рейтинг |
| date_partitions.py, repair.py | Явный repair неполных разделов, диапазоны дат, сохранение base и подтверждение очереди |
| entity_ods.py, entity_runtime.py | Версии полных сущностей, checks, упаковка и профили участников |
| entity_storage.py | Актуальный DDL entity_v2, пакетные PG/Mongo-загрузки, S3-блоки, readback |
| entity_workflow.py, workflow.py | Задачи и зависимости Airflow, подтверждение трёх загрузок |
| batch.py | Очередь top-N / all / выборка четырёх групп, прогресс в control PG, мягкая pause/resume с сохранёнными настройками |
| keep_running.py | Продолжение существующей очереди после штатных лимитов до явного дедлайна; ошибки и ручную паузу не отменяет |
| benchmark.py, experiment_inputs.py | Сравнение entity-v2/v3 (три/четыре входа), замороженные подтверждённые пакеты и изолированный стенд, без HTTP источника |
| storage_workloads.py | Дополнительные фиксированные опыты запросов/индексов/DDL/CRUD/COPY только в test_wl_*, не DDS |
| plot_storage_comparison.py | CSV-статистика, таблицы и восемь PNG/SVG графиков из действительных измерений |
| analytics_input.py, analytics_benchmark.py, analytics_sizes.py | Read-only срез подтверждённых снимков, шесть временных связанных наборов и полный аналитический опыт на 1k/10k/100k/968899, размеры после обновлений; не production DDS |
| plot_analytics_benchmark.py | Проверка завершённости/digest, медианы/min–max/IQR, CSV и шесть PNG/SVG расширенного сравнения |
| infra/compose.analytics.yaml, run_analytics_benchmark.py, sample_analytics_resources.py, export_analytics_results.py | Изолированный state/сеть, контроль безопасной паузы и прежнего deadline, мониторинг, экспорт только завершённых результатов |
| infra/compose.comparison.yaml, configure_comparison.py, freeze_comparison.py | Отдельная внутренняя сеть, новые credentials/volumes и согласованные копии входа сравнения |
| infra/run_storage_comparison.py, export_comparison.py | Контроль паузы/resume, нулевого source HTTP и выгрузка результатов без сырых SQLite |
| forecast.py | Прогноз ресурсов по завершённой выборке из четырёх групп; не обещает полноту сайта |
| cli.py, progress.py | Ручной старт, доказательства, replay, чтение сущности и мониторинг |
| tests/ | Фикстуры API, история, пагинация, limiter, контроль очереди, реальные три ODS |
| reports/lab1/ | Краткий README и рабочий REPORT; это ещё не окончательный отчёт |
| docs/research/evidence/lab1_history_20261030/ | Побайтово сохранённые исторические доказательства, методика, демонстрация, CSV/JSON/графики и MOVES.json с SHA-256 |
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
