# Speedrun.com ETL — лабораторные работы

**Текущий статус: новый сбор entity-v2 после полного разрешённого сброса стенда.**
Основные наборы — игры, раны, игроки и WR-история. Версии исходного содержимого
и компактные checks сохраняют историю без миллионов повторов профилей.
PostgreSQL/MongoDB используют пакетные операции, Garage — сжатые JSONL-блоки.
После проверки Half-Life удалены и заново созданы шесть volumes проекта;
старые raw/checkpoints/история Airflow больше недоступны. Архив data.zip,
исходники, секреты и отчёты сохранены. Подробности — [DATA_MODEL.md](DATA_MODEL.md)
и [HANDOFF](docs/HANDOFF.md).

Запущен `half-life-v2-fresh-20261016-001`; фоновый `fresh_cycle` после успеха
запустит `catalog-v2-fresh-20261016-001`, затем `minecraft-v2-fresh-20261016-001`.
Он не запускает top-5 и останавливается при ошибке DAG. Журнал в state-volume:
`/opt/airflow/state/fresh-cycle.log`. Повторно эти ID не создавать.
Исторические ID в примерах ниже отсутствуют после сброса; для текущей
демонстрации использовать свежие ID.

Текущий этап — каталог игр и Minecraft после проверенных Portal e2e и Half-Life.
Срок ЛР1: 2026-10-17 05:00 МСК. Полный сбор ранов всех игр отложен;
для приёмки нужны завершённые ETL, три ODS, сравнительные замеры и отчёт.
Утверждены Airflow 3.3.2, PostgreSQL 16, MongoDB 8.0 и Garage 2.4.1.
Код: `src/speedrun_etl`, три DAG: `dags/ods`, стенд: `compose.yaml`.
Фактические проверки и ограничения — [отчёт](reports/lab1/REPORT.md).
Архивные сборщики не подключаются к рабочему ETL.

## Передача и запуск на другом компьютере

Точная инструкция с нуля — [docs/TEAM_HANDOFF.md](docs/TEAM_HANDOFF.md).
Коллеге не нужны наши volumes или данные: драйвер сам получает свежий каталог,
выбирает топ-10 по счётчику verified full-game и собирает раны, WR и профили.

```powershell
docker compose exec -d scheduler python -u -m speedrun_etl.batch run --collection team-top10-001 --top 10 --log
docker compose exec scheduler python -m speedrun_etl.batch status --collection team-top10-001
```

Для полного доступного каталога использовать новый ID и `--all` вместо `--top 10`.
Очередь сохраняется в control DB; повтор команды пропускает готовые игры.
Через `--hours 8` прекращается запуск новых игр, активная завершается.
Прогресс страниц/категорий/профилей виден в Airflow collect logs; HTTP-телеметрия
показывает фактическую скорость отдельно от настроенного pacing.
Gzip включён. `ETL_MAX_RPM=60` по умолчанию, экспериментально допускается до 90;
после 420/429 действует устойчивое снижение и Retry-After. Это не обещание
скорости API v2. На больших ответах Minecraft измерено ~3,7 запроса/мин из-за
~15 с на ответ, поэтому один лишь больший лимит такую задержку не устраняет.

## Запуск малого e2e

Нужен запущенный Docker Desktop с Linux-контейнерами и примерно 8 ГиБ RAM.
Из корня репозитория, PowerShell:

```powershell
python infra/configure.py  # только один раз, не перезаписывает существующие секреты
docker compose config --quiet
docker compose build init
docker compose up -d
docker compose ps -a
docker compose exec scheduler python -m unittest discover -s /opt/project/tests -v
docker compose exec -e ETL_INTEGRATION=1 scheduler python -m unittest discover -s /opt/project/tests -p test_entity_storage_integration.py -v
docker compose exec scheduler python -m speedrun_etl.cli start-e2e --collection portal-e2e-001
docker compose exec scheduler python -m speedrun_etl.cli status --collection portal-e2e-001
```

После просмотра малого e2e пользователь отдельно разрешил полный сбор одной
игры Half-Life. Запуск ограничен 300 HTTP-попытками и отдельным снимком:

```powershell
docker compose exec scheduler python -m speedrun_etl.cli start-half-life --collection half-life-full-001 --request-budget 300
docker compose exec scheduler python -m speedrun_etl.cli status --collection half-life-full-001
```

UI: http://localhost:8080, пользователь `admin`, пароль `AIRFLOW_ADMIN_PASSWORD`
в локальном `.env`. `.env` не публиковать. Самостоятельные services API/frontend
не создаются; это штатный UI Airflow. API-ключ Speedrun.com v1 не используется.

Новые разрешённые запуски (ID повторно не запускать, если он уже есть):

```powershell
docker compose up -d --no-build
docker compose exec scheduler python -m speedrun_etl.cli start-catalog --collection catalog-20261016-001
docker compose exec scheduler python -m speedrun_etl.cli start-minecraft --collection minecraft-20261016-001
docker compose exec scheduler python -m speedrun_etl.progress catalog-20261016-001 minecraft-20261016-001
```

Каталог собирает полные карточки `GetGameList` без фильтра типа игры и
завершает `games_metadata`; запросы подробностей каждой игры не выполняются.
Minecraft (`mc`) проходит все full-game категории и статусы, затем собирает
профили участников из встроенных ответов. Новые режимы испытывают общий
последовательный pacing 30 → 45 → 60/мин. После 420/429 — минимум 60 секунд
и Retry-After, затем 20/мин; это наш эксперимент, не гарантия лимита v2.
Измерять нужно фактическую скорость: длительные ответы снижают её.

Цепочка для игры: games_metadata → runs → players. Малый тест Portal/Inbounds: до двух
страниц на статус, stats до/после, первая WR-страница, до 30 HTTP-попыток
с общим pacing. Первый запрос при неизвестном limiter ждёт 60 секунд.
Повтор того же collection ID не является свежим сбором; для продолжения после
сбоя очистить failed task в UI, сохраняя conf. Не создавать новый collection ID
для обхода исчерпанного бюджета. Малый e2e уже просмотрен пользователем.

```powershell
docker compose exec scheduler python -m speedrun_etl.cli evidence --collection portal-e2e-001
docker compose exec scheduler python -m speedrun_etl.cli replay --collection portal-e2e-001
```

`replay` загружает прежний пакет и не обращается к источнику. Данные/checkpoints/
limiter находятся в named volume `speedrun-lab1_etl-state`, ODS — в отдельных
постоянных volumes. `docker compose stop` останавливает сервисы с сохранением
данных. Не использовать `down -v` для обычной остановки.

Следующему агенту: прочитать [утверждённый план](docs/LAB1_PLAN.md),
[HANDOFF](docs/HANDOFF.md) и отчёт. Стек и ODS повторно не согласовывать;
остановка после первого e2e уже выполнена. Работать одним агентом.

- [Задание всех трёх лабораторных](Лабораторные_работы_Технологии_хранения_больших_данных_-1.md).
- [Правила для агентов](AGENTS.md).
- [Передача контекста](docs/HANDOFF.md).
- [Структура репозитория](docs/REPOSITORY_STRUCTURE.md).

## Материалы исследования

- [DATA_MODEL.md](DATA_MODEL.md) — ссылка на утверждённый ODS и исторический
  проект будущего DDS (DDS не реализуется).
- [DAGs.md](DAGs.md) — приблизительные схемы трёх процессов ЛР1.
- [SOURCE_CONTRACT.md](SOURCE_CONTRACT.md) — изученные свойства и ограничения API;
  универсальная полнота и семантика источника не доказаны.
- [COLLECTION_PLAN.md](COLLECTION_PLAN.md) — история решений и результатов сборов.
- [Небольшие отчёты и доказательства](docs/research/evidence/README.md) — копии
  для Git; полные raw/SQLite/CSV остаются локально в исключённом `data/`.
- [Архив прототипов](archive/research/README.md) и
  [прежние инструкции](archive/research/LEGACY_USAGE.md).
- [ANALYTICS_IDEAS.md](ANALYTICS_IDEAS.md) — идеи аналитики и расширений,
  не обязательный объём реализации.

Старые команды `python collect_all.py ...` в исторических документах относятся
к прежнему корню. Для совместимости запускать `python archive/research/<имя>.py`
из текущего корня, предварительно прочитав конкретный скрипт и README архива.
Некоторые исследования выполняют сетевые запросы при импорте.

Локальные тесты нового кода без API (Linux flock-проверки — внутри Docker):

```powershell
$env:PYTHONPATH = 'src'
python -m unittest discover -s tests -v
```

## Границы работ

| Этап | Результат |
|---|---|
| ЛР1 | Airflow в Docker Compose, ≥3 логически разных DAG, исходные данные в ODS трёх сервисов, сравнение хранилищ, отчёт и демонстрация |
| ЛР2 | DDS, нормализация, очистка, SCD, отдельный DAG Data Quality и зависимости ODS → DDS |
| ЛР3 | Витрины и DAG, Superset с ≥5 визуализациями, OpenMetadata с lineage |

Пользовательский сайт не требуется ни в одной лабораторной. Twitch/YouTube,
эмбеддинги, рекомендации, уведомления и другие расширения сейчас не реализуем.
Модель ODS, DAG и стек утверждены в `docs/LAB1_PLAN.md`.
Защита темы и таблица занятых источников не входят в текущие действия.

Основная область ранов: выбранные игры, полные категории, все доступные статусы,
включая obsolete. Orphaned сохраняются в ODS; level runs и отдельные Category
Extensions исключены из основной области. Для новых запросов действует общий
консервативный бюджет 20/мин. Полнота всего сайта не обещается.

Последнее уточнение: целевой охват — все доступные игры в согласованной области,
со свежим сбором через итоговый ETL. Ограничение списка игр нужно для проверки,
а не для сужения конечной цели. Старые профили и кэш не заменяют новый сбор.
Очистка накопленных данных обсуждается; точный состав удаления ещё не утверждён.

Повторный запуск итогового ETL не должен создавать дубли сущностей или
неизменных payload. При этом изменившиеся исходные состояния сохраняются
для будущих исследований уже в ЛР1. Модель — четыре логических набора;
требования — «История без дублей» в [SOURCE_CONTRACT.md](SOURCE_CONTRACT.md).

Ключ берётся из `SPEEDRUNCOM_API_KEY`, если нужен endpoint v1. Секреты не печатать
и не сохранять. API v2 не получает ключ v1. Данные, `.env`, логи, volumes
и локальный ZIP исключены из Git; архив и данные — из Docker build context.

Локальный Git инициализируется при подготовке проекта. Удалённый репозиторий,
публикация на GitHub и push выполняются отдельным действием по запросу пользователя.
