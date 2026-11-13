# Переход после ЛР1: PostgreSQL, передача базы и временный staging

ЛР1 сдана; по решению команды основной и единственный рабочий ODS — PostgreSQL 16.
Сохраняется Airflow и цепочка `games_metadata → runs → wr_history → players`.
Здесь описан переход перед ЛР2. DDS, SCD и Data Quality ещё не реализованы.
Отчёт и замеры трёх сервисов в `reports/lab1/` описывают завершённую ЛР1.

## Что изменено

Применена ветка коллеги `origin/refactor/post-lab1` (`25424f2`): текущие подключения
отделены от legacy, старые форматы и импорты совместимы. Исправлена регрессия
переноса `collect.restore`, обнаруженная существующими тестами.
Рабочая ветка для совместной разработки — `codex/postgres-only`.
Получение кода и первый запуск с готовой базой описаны в
[COLLEAGUE_START.md](COLLEAGUE_START.md). На момент проверки и упаковки дампов
изменения ещё были локальными; соответствующие поля evidence относятся к тому срезу.

Основной `compose.yaml` содержит PostgreSQL, init и три процесса Airflow.
Он использует существующие имена volumes и базы `speedrun_ods`, `speedrun_control`,
`airflow`: менять проект или создавать пустые volumes при обновлении не нужно.
`ETL_STORES=postgres` — значение по умолчанию. Пакеты MongoDB/boto3 вынесены
в optional dependencies `[lab1]`, подключения к ним не нужны для рабочего ETL.

## Временные snapshots

`etl-state/snapshots` — рабочий staging, а не единственная копия сырых данных.
ODS PostgreSQL уже содержит исходные ответы (`responses` и `blocks`), версии
сущностей, checks, manifests и метаданные запросов. Исходные JSON восстанавливаются
без запросов к Speedrun.com. Очистка не удаляет записи ODS и не превращает его в DDS.

При `ETL_STAGING_CLEANUP=1` (по умолчанию) завершающая задача `cleanup` DAG players:

1. Проверяет завершение всех этапов и receipt PostgreSQL для каждого пакета.
2. Сохраняет сжатый точный состав пакетов в `speedrun_control.staging_archives`.
3. Перечитывает все соответствующие строки PostgreSQL и сверяет counts/digests.
4. После проверки всей цепочки удаляет только `requests/` и `entity-package/`
   этой игры. Небольшие config, extracted, verified, retired и checkpoints остаются.

Сбор каталога и рейтинга остаётся pinned как общий вход очереди. Незавершённые
цепочки, partial/budget-stopped runs и smoke сохраняются для разбора и продолжения.
WR partial/unavailable сохраняется в raw/manifests и не переименовывается в полное покрытие.
Metadata-only игра очищается после своего finalize. Отключить автоматическую
очистку можно `ETL_STAGING_CLEANUP=0`.

`cli evidence`, `cli history`, `cli replay` понимают очищенный staging. Для replay
точный пакет временно восстанавливается из PG и удаляется после чтения;
постоянное накопление пакетов не возвращается. Старые исследовательские утилиты,
которые напрямую открывают SQLite, требуют исходный пакет и не являются рабочим
способом чтения после очистки. `EntityPackage` откажется создать пустой пакет
на месте явно очищенного снимка.

Разовая проверка/очистка старых завершённых игр при остановленных цепочках:

```powershell
docker compose run --rm --no-deps --entrypoint python scheduler -m speedrun_etl.staging --all-completed
docker compose run --rm --no-deps --entrypoint python scheduler -m speedrun_etl.staging --all-completed --apply
```

Вторая команда дополнительно проверяет отсутствие активных DagRun в Airflow.
Ни одна из команд не запускает scheduler, сбор или batch resume. Ручная пауза
старой очереди сохраняется. Не удаляйте целиком volume `etl-state`: в нём также
находятся бюджеты, ограничения источника, очередь, состояние паузы и авторизация UI.

## Передача коллеге через Google Диск

В `exports/` подготовлен ZIP со следующими файлами:

- `speedrun_ods.dump`, `speedrun_control.dump`: custom-format pg_dump без ACL и паролей.
- `manifest.json`, `verified.json`: SHA-256, количества и хеши всех строк таблиц,
  результат реального восстановления в изолированную БД без сети.
- `RESTORE.cmd`, `RESTORE.ps1`, `compose.transfer.yaml`, `postgres-init.sh` и
  `TRANSFER_README.md`: восстановление в Windows с установленным Docker Desktop.
- `project/`: снимок применённого кода без `.env`, volumes и `.git`.

Коллега скачивает и распаковывает ZIP, запускает `RESTORE.cmd`, затем подключается
к `localhost:5433`, база/пользователь `speedrun_ods`. Новые локальные пароли
создаются в `.env`; ваш файл `.env` не передаётся. Скрипт откажется писать в
непустые базы. В архиве есть примеры SQL; Airflow для просмотра данных не нужен.

Самостоятельное создание такого комплекта из корня проекта:

```powershell
docker compose up -d --wait postgres
python infra/postgres_transfer.py export --directory exports/NEW_TRANSFER
python infra/postgres_transfer.py verify --directory exports/NEW_TRANSFER
python infra/postgres_transfer.py pack --directory exports/NEW_TRANSFER
```

Директория должна быть новой. Export требует отсутствия активных DagRun;
верификация создаёт и удаляет только свой временный контейнер с проверяемой меткой.
`restore --directory <папка>` восстанавливает в пустой PostgreSQL текущего Compose,
не перезаписывая существующие данные. Используйте PostgreSQL 16 или совместимую
более новую версию; для текущего комплекта закреплён образ 16.
Это перенос ODS/control; Airflow history и файловые checkpoints старой очереди
не передаются. Старый сбор на компьютере коллеги сам не возобновится.

## Историческая ЛР1

Для повторения трёхстороннего опыта есть отдельный явный override:

```powershell
python infra/configure.py --lab1  # только в новой копии без .env
docker compose -f compose.yaml -f infra/compose.lab1.yaml up -d --build
```

Он возвращает три загрузчика, зависимости `[lab1]`, receipts трёх ODS и отключает
очистку. Для восстановления старой ЛР1 используйте её проверенный переносимый
комплект, описанный в `LAPTOP_DEMO.md`. Этот режим не нужен для ЛР2.

## Проверки

Результат перехода 13 ноября: 181 тест, без ошибок и пропусков. Snapshots
уменьшены с 16,62 до 4,91 ГиБ; сохранены незавершённая игра и общий каталог.
536 очищенных пакетов имеют точные составы в PG. Прежние рабочие Mongo/S3 volumes
удалены после полного восстановления финальных дампов и сверки всех строк.
Счётчики ODS/очередь/HTTP-бюджеты не изменились, новых source HTTP=0.
См. [evidence перехода](research/evidence/postgres_transition_20261113.json).

Существующие тесты не переписаны. `test_postgres_mode.py` добавляет проверки
единственного ODS, графа задач, полного цикла extract/load/finalize/cleanup/replay,
восстановления точного raw, повторной очистки и отказа при повреждении PG/receipt.
Сетевой transport в тестах синтетический; обращения к реальному источнику не нужны.
Старые интеграции включаются `ETL_INTEGRATION=1` в изолированном трёхсервисном
стенде; новые — `ETL_PG_INTEGRATION=1`. Указывать пропуски отдельно от успехов.
