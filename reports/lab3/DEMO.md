# Запуск и демонстрация ЛР3

Нужны Docker Desktop, существующие `.env` и PostgreSQL ODS. Команды выполняются из корня проекта. Новый сбор не требуется.

## Подготовка

```powershell
python infra/configure_lab3.py
docker compose --profile lab3 build init superset-init
docker compose up -d postgres
docker compose run --rm --no-deps --user airflow --entrypoint python init -m speedrun_etl.marts prepare-dds
docker compose run --rm --no-deps --user airflow --entrypoint python init -m speedrun_etl.marts prepare-analytics
docker compose --profile lab3 up -d superset openmetadata
docker compose run --rm --no-deps --user airflow --entrypoint python init -m speedrun_etl.marts build
docker compose exec -T superset /app/.venv/bin/python /app/lab3/provision_superset.py
docker compose run --rm --no-deps --user airflow --entrypoint python init -m speedrun_etl.analytics_metadata
```

Скрипт генерации дописывает отсутствующие секреты в локальный `.env`, не выводя значения. Пароли веб-интерфейсов находятся в `SUPERSET_ADMIN_PASSWORD` и `OPENMETADATA_ADMIN_PASSWORD`. Superset: пользователь `admin`; OpenMetadata: `admin@open-metadata.org`.

Перед запуском планировщика оставить DAG сбора на паузе и проверить отсутствие незавершённых запусков:

```powershell
docker compose run --rm --no-deps --user airflow --entrypoint airflow init dags pause --treat-dag-id-as-regex --yes '^(games_metadata|runs|wr_history|players)$'
docker compose up -d api-server dag-processor scheduler
```

Не использовать `down -v`. История, ODS, checkpoints и прежняя ручная пауза сохраняются. Первичная нормализация большого архива может занять значительное время; повтор уже обработанных пакетов пропускается.

## Что показать

Дополнение: [Рейтинг раннеров](http://localhost:8088/superset/dashboard/lab3-ranking/).
В экспериментальной ветке основной топ учитывает все игры с убывающими весами
(0–1500): качество результата, плотность топ-10/25 и прошлое удержание топ-3.
Выберите любую строку ниже первого места и найдите этого раннера фильтром:
его общее место должно сохраниться.
Поменяйте минимум участников с 10 на 100 и сравните охват. Подробная
[методика](../../docs/analytics/RUNNER_RANKING.md) объясняет поправку малых полей,
ничьи и отсутствие бонуса за длительность. В таблице вкладов выберите
`Niftski [qjne76wj]`: Red Ball имеет четвёртое место и сохранённый исторический
бонус, а неизвестная история SMB показана отдельно. Все три прежних дашборда доступны.

Для подготовки к защите достаточно начать с [короткой памятки](DEFENSE_SHORT.md).

1. [Airflow](http://localhost:8080): `normalize_entity_v3`, `check_data_quality`, `refresh_marts`, `sync_analytics_metadata`. Объяснить Assets, проверку качества и атомарную публикацию. Ручной повтор `refresh_marts` использует те же входы и не запускает источник.
2. [Раннер](http://localhost:8088/superset/dashboard/lab3-runner/): выбрать профиль и сопоставимое поле, сравнить цель и распределение мест, посмотреть новые игры и основания рекомендаций. Затем показать когорты, форму и паутинку.
3. [Борьба за рекорды](http://localhost:8088/superset/dashboard/lab3-records/): изменить окно 7/30/90 дней, найти плато, открыть выбранную категорию в истории и разборе видео. Показать исходные события WR отдельно от реконструкции.
4. [Страны](http://localhost:8088/superset/dashboard/lab3-countries/): объяснить медальный порядок и числовые баллы. Изменить игру, затем выбрать страну и проверить сохранение её глобального места в выбранном поле.
5. [OpenMetadata](http://localhost:8585): найти `speedrun_postgres.speedrun_ods.speedrun_marts.runner_result`, открыть Lineage и пройти через `runner_tenure` и `run` к DDS и ODS. Повторить для `country_medals` с lineage колонок.

Ключевые проверки: массовые ничьи Break Dirt; один PB вместо нескольких публикаций игрока; смена платформенного поля в Super Mario 64; неизвестные страны и малые когорты; отсутствие удвоения после повторной сборки.

Таблица исходной истории WR показывает максимум 10000 строк запроса; для
детального разбора выберите игру. Этот предел интерфейса не ограничивает
сохранённую историю и расчёты витрин.

Проверенные примеры для демонстрации:

- `RealXones` (профиль по умолчанию): при переключении цели top10 → top50 место раннера
  остаётся 196; требуемое улучшение меняется с 310 до 240 секунд в исходном поле.
- `Chexhuman [5j5o2pzx]`: есть все шесть осей паутинки. Для остальных профилей
  таблица рядом показывает, каких наблюдений не хватает; пропуск не равен нулю.
- `iTaKu [48gvyr28]`, поле `Celeste · Any% · Все платформы · igt`, цель top10:
  кривая достижения для достаточных когорт. Ось X — календарные дни от первой
  видимой публикации; это не оценка часов тренировки.
- «Страны»: выбор России сохраняет 4-е место общего зачёта. Выбор игры Celeste
  и категории Any% пересчитывает медали по выбранной совокупности.

Полный повтор аналитической цепочки: в Airflow запустите `normalize_entity_v3`
с конфигурацией `{"collection":"lab1-popular500-20261030"}`. Он перечитывает
подтверждённые сохранённые receipts; затем Assets запускают DQ, витрины и каталог.
Уже обработанные пакеты и существующая сборка пропускаются. DAG источника
для этого включать не нужно.

## Проверка UI

Установленный Microsoft Edge используется в отдельном headless-сеансе:

```powershell
python -m venv exports/lab3-tools/venv
exports/lab3-tools/venv/Scripts/python.exe -m pip install -r infra/requirements-lab3-demo.txt
exports/lab3-tools/venv/Scripts/python.exe infra/verify_lab3_ui.py
exports/lab3-tools/venv/Scripts/python.exe infra/verify_runner_ranking_ui.py
```

Сценарий сохраняет снимки дашбордов и статусы запросов графиков. Токены, cookies и пароли в отчёт не сохраняются.

Запускайте сценарии последовательно. Историческая приёмка трёх дашбордов лежит в
`reports/lab3/acceptance_20261126/`, новая проверка четырёх дашбордов и рейтинга —
в `reports/lab3/runner_ranking_20261127/`, обновление до десяти игр —
в `reports/lab3/runner_ranking10_20261127/`. Чтобы сохранить исторические снимки,
не запускайте прежние filter/services сценарии с путями по умолчанию поверх
приёмки 26 ноября. Для переноса на другой ПК сначала прочитайте
[инструкцию по секретам и данным](../../docs/LAB3_TRANSFER.md): pull не переносит
служебные базы, ключи и checkpoints.
