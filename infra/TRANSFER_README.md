# Собранная база Speedrun.com

1. Установите и запустите Docker Desktop (Linux containers).
2. Распакуйте ZIP в отдельную локальную папку. Не запускайте скрипт прямо из ZIP.
3. В Windows откройте `RESTORE.cmd`. Он проверит SHA-256, запустит PostgreSQL 16
   и импортирует две базы. Python, Airflow, MongoDB, S3 и учётные данные автора не нужны.
   Первый запуск скачает образ PostgreSQL; само восстановление работает локально.

Подключение из DBeaver/DataGrip: `localhost:5433`, база и пользователь
`speedrun_ods`, пароль — значение `ODS_DB_PASSWORD` в созданном рядом `.env`.
Скрипт создаёт новые пароли локально. `.env` никому не отправляйте.
Если 5433 занят, добавьте `TRANSFER_PG_PORT=5434` в `.env` до запуска.

Открыть SQL без установки клиента:

```powershell
docker compose -f compose.transfer.yaml exec postgres psql -U speedrun_ods -d speedrun_ods
```

```sql
-- Основной ODS, исходные документы и история, не очищенный DDS.
SELECT count(DISTINCT source_id) FROM entity_v3.runs;
SELECT count(DISTINCT source_id) FROM entity_v3.players;
SELECT id, game_id, category_id, data FROM entity_v3.runs_current LIMIT 5;
SELECT source_id, document->'raw' FROM entity_v3.games
WHERE projection='game_detail' LIMIT 5;
```

Таблицы `entity_v3`: games, runs, players, wr_history, checks, responses, blocks,
manifests. `responses` вместе с `blocks` позволяют восстановить исходные JSON.
`checks` хранит происхождение и время наблюдений; `runs_current` — удобное представление.
WR partial/unavailable и неполнота отдельных разделов сохранены в manifests.
Успех загрузки не означает полного покрытия всего сайта.

Вторая база `speedrun_control` содержит receipts, очередь и архивные составы
пакетов. Сбор автоматически не запускается. Образ Airflow и старые служебные
запуски Airflow в комплект не входят. В папке `project/` лежит снимок кода
с применённым рефакторингом и переходом на PostgreSQL, без секретов и volumes.
Для совместной разработки используйте ветку `codex/postgres-only` репозитория
Big-Data-Storage-and-Management; порядок получения и запуска —
`docs/COLLEAGUE_START.md` в этой ветке. Копия `project/` фиксирует момент упаковки.
Для импорта в его ПУСТОЙ стенд: запустите только сервис postgres, затем
`python infra/postgres_transfer.py restore --directory <папка-комплекта>`.
Обычный `compose up` до импорта создаёт ODS-таблицы и помешает защите от перезаписи.

`manifest.json` — размеры, SHA-256 файлов и counts/digests всех строк каждой
таблицы обеих БД. `verified.json` — результат реального восстановления в
изолированный PostgreSQL без сети и полного сравнения строк с оригиналом.
Повторный RESTORE откажется писать в непустые базы; это защита от случайной перезаписи.
Для повторного просмотра просто `docker compose -f compose.transfer.yaml up -d`.
Остановка: `docker compose -f compose.transfer.yaml stop`. Не используйте `down -v`:
он удаляет базу. Локальные Docker volumes не нужно копировать или пересылать.
