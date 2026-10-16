# Демонстрация ЛР1

Все команды выполняются из корня проекта. Docker Desktop должен работать.
Поднимать существующий стенд можно без повторной сборки:

```powershell
docker compose up -d --no-build
docker compose ps
docker compose exec scheduler airflow dags list-import-errors
```

1. Открыть http://localhost:8080, войти как `admin` с локальным паролем из `.env`.
   Показать графы `games_metadata`, `runs`, `players` и завершённую цепочку
   `half-life-v2-fresh-20261016-001`. У каждого DAG собственный результат: справочники,
   раны, профили участников. Три ветви загрузки направлены в разные сервисы.
2. Показать доказательства завершения: манифест, область, coverage, digest
   одинаковых записей в PostgreSQL, MongoDB и Garage. Команда ниже читает
   сохранённые доказательства; новые запросы к Speedrun.com не делает.

```powershell
docker compose exec scheduler python -m speedrun_etl.cli evidence --collection half-life-v2-fresh-20261016-001
```

3. Показать историю одного реального рана во всех трёх ODS. Например, рана
   Muty из сохранённого снимка Half-Life:

```powershell
docker compose exec scheduler python -m speedrun_etl.cli history --collection half-life-v2-fresh-20261016-001 --entity run --id y90kxxry
```

4. При необходимости продемонстрировать повторную загрузку пакетов. Выполнять
   вне сравнительных замеров. Ключи и digest остаются прежними; источник не
   опрашивается. Новое фактическое наблюдение источника создаётся отдельным
   collection ID, поэтому его не следует путать с replay.

```powershell
docker compose exec scheduler python -m speedrun_etl.cli replay --collection half-life-v2-fresh-20261016-001
```

5. Показать `measurements.csv`, `summary.csv`, `load_time.png` и
   `storage_size.png` **после завершения эксперимента**, а также его `input.json`.
   Методика — [BENCHMARK_METHOD.md](BENCHMARK_METHOD.md). Объяснить разницу
   серверной фильтрации PG/Mongo и клиентского чтения S3, границы измерений
   объёма и отсутствие гарантированно холодного кеша.
6. Показать расширенный сбор через `speedrun_etl.progress`. Завершённость
   каталога не означает сбор всех ранов сайта. Ограничения и незавершённые
   разделы остаются явно отмеченными; это не скрытые удаления данных.

```powershell
docker compose exec scheduler python -m speedrun_etl.progress catalog-v2-fresh-20261016-001 minecraft-v2-fresh-20261016-001
```

Секреты не показывать на демонстрации. Для штатной остановки использовать
`docker compose stop`: volumes и история сохраняются. Публикация GitHub
выполняется отдельно по запросу пользователя; локальная реализация сама
по себе не подтверждает выполнение требования о публикации.
