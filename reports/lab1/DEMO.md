# Демонстрация ЛР1

Команды выполняются из корня проекта. Для текущих данных не нужны rebuild,
reset или новые запросы к сайту. Сначала проверить текущие процессы:

```powershell
docker compose ps -a
docker compose exec -T scheduler airflow dags list-import-errors
docker compose exec -T scheduler python -m speedrun_etl.progress --batch lab1-top10-fresh-20261028 --human
```

1. Открыть http://localhost:8080, войти как admin с локальным паролем из .env,
   не показывая его на записи. Показать три DAG и завершённые запуски Portal:
   `lab1-top10-fresh-20261028-g-4pd0n31e-{games_metadata,runs,players}`.
   Объяснить: первый DAG получает карточку/справочники, второй — раны/WR,
   третий — профили участников. Это разные результаты одного источника.
   У каждого DAG три ветви загрузки и общий этап подтверждения.
2. Показать манифесты, coverage и равные digest трёх ODS. Команда читает
   сохранённые результаты, Speedrun.com не опрашивает:

```powershell
docker compose exec -T scheduler python -m speedrun_etl.cli evidence --collection lab1-top10-fresh-20261028-g-4pd0n31e
```

3. Показать реальные версии рана и checks его истории в PostgreSQL, MongoDB
   и Garage. Этот ID включён в проверенные чтения benchmark:

```powershell
docker compose exec -T scheduler python -m speedrun_etl.cli history --collection lab1-top10-fresh-20261028-g-4pd0n31e --entity run --id y21veg5z
```

   Ключи содержимого стабильны; новые версии не затирают старые. Повтор
   загрузки пакета не создаёт новое наблюдение источника. Отсутствие ID/404
   не удаляет историю. Показать missing_run_ids_20261028.json: два404 и
   положительный контроль200. При необходимости проверить обычную SQL-выборку:

```sql
SELECT source_id, category_id, status, document->'raw'->>'text' AS raw_run
FROM entity_v2.runs
WHERE game_id='4pd0n31e' AND projection='leaderboard_run'
ORDER BY source_id LIMIT 3;
```

4. Показать полный исходный JSON и схему общих playerList-фрагментов через
   DATA_MODEL.md/REPORT.md. Отдельно показать sm64_date_gap_20261028.json:
   90 ранов с null-датой сохранены обычным обходом. Никакие даты не придуманы;
   контексты place/obsolete при фильтрах дат сохранены раздельно.
   sm64_auto_fallback_20261028.json подтверждает следующий свежий прогон:
   24 412 ID с датами → 24 502 без фильтра, все дополнительные 90 имеют
   performedAt=null; без repair base, 15 разделов проверены, три ODS согласованы.
5. Открыть `entity_v2_benchmark_20261028/measurements.csv`, `summary_all.csv`,
   `input.json`, `completed.json`, `load_time.png`, `storage_size.png`.
   Объяснить одинаковый вход, три повтора/чередование, readback/replay,
   различие методов учёта объёма и клиентскую фильтрацию S3. Результат выбора:
   PostgreSQL как основной ODS по истории/SQL, S3 как компактная копия исходных
   объектов, MongoDB как документная копия. Все три сервиса сохраняются.
6. Повтор загрузки уже проверен benchmark. Ресурсный пилот завершён;
   перед живым replay убедиться, что новых сборов/замеров нет:

```powershell
docker compose exec -T scheduler python -m speedrun_etl.cli replay --collection lab1-top10-fresh-20261028-g-4pd0n31e
```

   Для повторения эксперимента на другом компьютере нужен собственный готовый
   collection, затем benchmark из BENCHMARK_METHOD.md. Старые авторские ID
   не переносят исходные данные. Тестовые пространства test_* исключаются
   из объёма production ODS.
7. Показать ограничение полного охвата: top10 не весь сайт, WR coverage
   partial/unavailable, каталог — отдельный наблюдавшийся интервал. Сохранённый
   итог пилота: 19 complete, 1 not_applicable, 513 разделов, 60 снимков в трёх ODS:

```powershell
docker compose exec -T scheduler python -m speedrun_etl.progress --batch lab1-capacity-pilot-20261028 --human
```

   resource_forecast_20261028.json расширяет выборку по численности четырёх
   групп: около 1,12 млн HTTP и 38,9 суток полного цикла на этом стенде.
   Это оценка, не срок выполнения; весь сайт не входит в критерии ЛР1.

Пользовательский сайт/DDS/Superset для ЛР1 не требуются. Удаление данных и
бэкапов и запуск full допускаются только после отдельного подтверждения.
Существующие данные сохраняются при штатном docker compose stop.

## Дополнительная демонстрация запросов и операций с таблицами

Открыть раздел «Расширенное сравнение» в REPORT.md и папку
storage_workloads_20261028: query_scaling.png, index_effect.png,
table_operations.png, pg_bulk.png, summary.csv. Все входы/повторы прошли
сверку, новых Speedrun HTTP не было. Это тестовые рабочие проекции; production
ODS остаётся прежней. В примерах ниже указан реально сохранённый test namespace.
На другом компьютере выполнить свой опыт и заменить имя схемы/БД.

Число наблюдавшихся ранов по игре/статусу, соединение с карточками:

```sql
SELECT g.name, r.status, count(*) AS runs
FROM test_wl_20261028153333_100000_0.runs r
JOIN test_wl_20261028153333_100000_0.games g ON g.key=r.game_id
GROUP BY g.name,r.status
ORDER BY g.name,r.status;
```

Месячные counts и накопительная сумма. Записи без performedAt не участвуют
в этом временном окне; они сохранены и входят в обычные counts:

```sql
WITH m AS (
  SELECT game_id,status,source_month,count(*) AS n
  FROM test_wl_20261028153333_100000_0.runs
  WHERE source_month IS NOT NULL
  GROUP BY game_id,status,source_month
)
SELECT *,sum(n) OVER (PARTITION BY game_id,status ORDER BY source_month) AS cumulative
FROM m ORDER BY game_id,status,source_month;
```

Показать GIN-поиск по массиву участников и план с числом прочитанных строк:

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT key,source_month,time_ns
FROM test_wl_20261028153333_100000_0.runs
WHERE participant_keys @> ARRAY['j1npme6p:jmp14lej']
ORDER BY key;
```

MongoDB тоже умеет связь с профилями, не требуется предварительно встраивать
полный справочник игроков в каждый ран:

```javascript
db.getSiblingDB('test_wl_20261028153333_100000_0').runs.aggregate([
  {$match: {game_id:'j1npme6p', status:1}},
  {$limit:20},
  {$lookup: {from:'players', localField:'participant_keys', foreignField:'_id', as:'profiles'}},
  {$project: {key:1, 'profiles.name':1, 'profiles.area_id':1}}
]);
```

area_id остаётся исходным идентификатором; без проверенного справочника он
не объявляется страной. Полные исходные профили/ID есть в raw и ODS.

Операции с таблицей и откат DDL/DML — только на созданной экспериментом копии:

```sql
BEGIN;
ALTER TABLE test_wl_20261028153333_100000_0.clone_b ADD COLUMN demo_note text;
UPDATE test_wl_20261028153333_100000_0.clone_b
SET demo_note='checked'
WHERE key=(SELECT min(key) FROM test_wl_20261028153333_100000_0.clone_b);
SELECT count(*) FROM test_wl_20261028153333_100000_0.clone_b WHERE demo_note='checked';
ROLLBACK;
```

После ROLLBACK столбца demo_note нет. Не подставлять production entity_v2
в команды изменения: исходная история не редактируется этим способом.
Примеры реально выполнены: SQL counts суммируются в 100000, окно даёт 980
строк, Mongo JOIN — 20 документов, DDL/DML полностью откатились.
[Проверка команд](storage_workloads_20261028/demo_verified.json).
Для объяснения различий показать capabilities_100000_0_{postgres,mongo,garage}.json
и garage_additional_verified.json: unique/validator/FK, отказ Mongo standalone
от транзакции, частичные записи, реально отправленные If-Match/If-None-Match
и заменённые тела Garage. Все эти проверки относятся к текущему стенду.
