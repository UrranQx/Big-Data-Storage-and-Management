# Передача полного стенда ЛР3 коллеге

Цель — получить у коллеги отдельную рабочую копию текущего стенда: код, ODS,
DDS, витрины, очередь/checkpoints, Airflow, четыре дашборда Superset и каталог
OpenMetadata. Его прежняя ЛР2 сохраняется в своей папке и своих volumes.
Смешивать её БД и `.env` с новым комплектом не требуется.

Команды ниже рассчитаны на PowerShell 7, Git и Docker Desktop в режиме Linux
containers. Архитектура исходного стенда — `linux/x86_64`. Для ARM или иной
архитектуры этот сценарий физического переноса не проверен: потребуется отдельный
подбор образов и, возможно, логический dump/restore PostgreSQL.

Сам полный перенос на другой ПК ещё не выполнялся. Проверены текущий состав,
размеры, образы, SQL и интерфейсы; окончательная проверка восстановления находится
в шаге 6. Подготовка архива остановит стенд на время копирования. Эти команды
не запускаются автоматически при чтении инструкции.

## 1. Что передаём и что можно использовать от ЛР2

| Состав | Назначение |
|---|---|
| `lab3.bundle` + `COMMIT.txt` | Вся история, код ветки `lab3`, SQL, DAG, тесты, отчёты, инструкции и сохранённые доказательства. Можно также клонировать GitHub и перейти на тот же commit. |
| `private/.env` | Полный текущий набор паролей и ключей шифрования. |
| `private/keys/private_key.der`, `public_key.der` | Согласованная RSA-пара OpenMetadata. |
| `postgres-data.tar.gz` | Весь PostgreSQL 16: роли и БД `speedrun_ods`, `speedrun_control`, `airflow`, `superset`, `openmetadata`. Включает DDS и уже рассчитанные витрины. |
| `etl-state.tar.gz` | Checkpoints, сохранённое файловое состояние, ручная пауза, локальная аутентификация Airflow. Очередь и receipts также находятся в control PG. |
| `airflow-logs.tar.gz` | История журналов задач. |
| `lab3-search.tar.gz` | Поисковый индекс OpenMetadata, согласованный с его БД. |
| `images.tar`, `compose.transfer.yaml`, `IMAGES.json` | Все пять образов текущего стенда под отдельными тегами. Первый запуск не требует скачивать образы или устанавливать Python-зависимости. |
| `ARCHITECTURE.txt`, `CHECKSUMS.json` | Архитектура источника и контрольные суммы комплекта. |

От коллеги можно использовать установленный Docker Desktop, Git и редактор.
Его код ЛР2 уже является частью истории проекта, но локальные незакоммиченные
правки нужно сохранить отдельно и затем переносить выборочно. Его старая БД,
старые checkpoints и пароли не заменяют перечисленные файлы. Повторять сбор
Speedrun.com и заново строить DDS для этого способа переноса не требуется.

Сейчас четыре тома занимают примерно **29 GiB**: PostgreSQL 24 GiB, state 5 GiB,
логи 61 MiB, индекс 14 MiB. Образы исходного Docker занимают около 10 GB.
Размер gzip заранее не обещается. Для каталога комплекта разумно иметь запас
около 40 GB, на ПК коллеги — 70–80 GB свободного места под комплект, распаковку
Docker и дальнейшую работу. Для полного стенда ориентир — 10–12 GB доступной
Docker памяти; доступные ресурсы нужно проверить на его машине.

**Весь комплект приватный:** базы и state тоже содержат секреты. Передайте его
на зашифрованном носителе либо в зашифрованном архиве, пароль — отдельным каналом.
В Git и публичные ссылки комплект не отправлять. Подробнее о ключах —
[LAB3_TRANSFER.md](LAB3_TRANSFER.md).

## 2. На твоём ПК: подготовить код и образы

Сначала текущие изменения должны быть закоммичены и отправлены в `lab3`.
Пакет ниже требует чистого рабочего дерева, чтобы код не отстал от базы.
Выполнять из корня проекта. Путь `D:\Transfers` — пример: выбери диск с местом.

```powershell
$ErrorActionPreference = 'Stop'
function Invoke-CheckedDocker { & docker @args; if ($LASTEXITCODE -ne 0) { throw 'Docker command failed' } }

if ((git branch --show-current).Trim() -ne 'lab3') { throw 'Нужна ветка lab3' }
if (git status --porcelain) { throw 'Сначала сохранить изменения в Git' }
$revision = (git rev-parse HEAD).Trim()
$shortRevision = $revision.Substring(0, 12)
$transferRoot = "D:\Transfers\lab3-$shortRevision"
if (Test-Path -LiteralPath $transferRoot) { throw 'Выбери новый каталог комплекта' }
New-Item -ItemType Directory -Path "$transferRoot\private\keys" | Out-Null
$transferRoot = (Resolve-Path -LiteralPath $transferRoot).Path

git bundle create "$transferRoot\lab3.bundle" lab3
if ($LASTEXITCODE -ne 0) { throw 'Не создан Git bundle' }
$revision | Set-Content -Encoding utf8 "$transferRoot\COMMIT.txt"
Invoke-CheckedDocker info --format '{{.OSType}}/{{.Architecture}}' | Set-Content -Encoding utf8 "$transferRoot\ARCHITECTURE.txt"
Invoke-CheckedDocker compose --profile lab3 build init superset-init

Copy-Item -LiteralPath .env -Destination "$transferRoot\private\.env"
Copy-Item -LiteralPath data/lab3/keys/private_key.der -Destination "$transferRoot\private\keys"
Copy-Item -LiteralPath data/lab3/keys/public_key.der -Destination "$transferRoot\private\keys"

$originals = @{
  airflow = 'speedrun-lab1-airflow:local'
  superset = 'speedrun-lab3-superset:local'
  postgres = 'postgres:16@sha256:ca0bd484cb98bf4b24eb1010e73fb3fcbd6714d240fbc1a10eea5b7dbecb641d'
  search = 'docker.elastic.co/elasticsearch/elasticsearch:9.3.0@sha256:4f6bdcb742e892539c6ac49b0dd3e4e182e90218546e8c6a22db378c344acb60'
  metadata = 'docker.getcollate.io/openmetadata/server:2.0.9@sha256:8575ad1b61866f76075f096c40ec4e9ee19339dcab50169adec9b69bec380f98'
}
$tags = @{}
$imageManifest = foreach ($kind in $originals.Keys) {
  $tags[$kind] = "lab3-transfer-${kind}:$shortRevision"
  Invoke-CheckedDocker image tag $originals[$kind] $tags[$kind]
  $imageId = Invoke-CheckedDocker image inspect --format '{{.Id}}' $tags[$kind]
  [pscustomobject]@{ kind=$kind; tag=$tags[$kind]; id=$imageId }
}
$imageManifest | ConvertTo-Json | Set-Content -Encoding utf8 "$transferRoot\IMAGES.json"
$saveTags = @($tags.Values)
Invoke-CheckedDocker image save --output "$transferRoot\images.tar" @saveTags
@"
services:
  init: {image: '$($tags.airflow)'}
  scheduler: {image: '$($tags.airflow)'}
  api-server: {image: '$($tags.airflow)'}
  dag-processor: {image: '$($tags.airflow)'}
  lab3-init: {image: '$($tags.airflow)'}
  superset-init: {image: '$($tags.superset)'}
  superset: {image: '$($tags.superset)'}
  postgres: {image: '$($tags.postgres)'}
  elasticsearch: {image: '$($tags.search)'}
  openmetadata-migrate: {image: '$($tags.metadata)'}
  openmetadata: {image: '$($tags.metadata)'}
"@ | Set-Content -Encoding utf8 "$transferRoot\compose.transfer.yaml"
```

Отдельные теги нужны, чтобы `docker load` не заменил одноимённый локальный образ
Airflow, который коллега мог собрать для ЛР2. Override меняет только образы;
пароли остаются в `.env`, данные — в отдельных томах.

## 3. На твоём ПК: согласованная холодная копия

До остановки убедись, что аналитические задачи закончились и новых ручных
операций не будет. Проверка `dag_run` не видит посторонние процессы на хосте.

```powershell
$active = Invoke-CheckedDocker compose exec -T postgres psql -XAt -U postgres -d airflow -c "SELECT count(*) FROM dag_run WHERE state IN ('running','queued');"
if ([int]$active -ne 0) { throw 'Сначала дождись завершения задач; сбор не возобновлять' }
$sourcePaused = Invoke-CheckedDocker compose exec -T postgres psql -XAt -U postgres -d airflow -c "SELECT (count(*)=4 AND bool_and(is_paused))::int FROM dag WHERE dag_id IN ('games_metadata','runs','wr_history','players');"
if ([int]$sourcePaused -ne 1) { throw 'Сначала проверить и сохранить паузу четырёх DAG источника' }

Invoke-CheckedDocker compose --profile lab3 stop --timeout 120
$volumeNames = @('postgres-data','etl-state','airflow-logs','lab3-search')
foreach ($name in $volumeNames) {
  $volume = "speedrun-lab1_$name"
  Invoke-CheckedDocker volume inspect --format '{{.Name}}' $volume
  $users = Invoke-CheckedDocker ps --filter "volume=$volume" --format '{{.Names}}'
  if ($users) { throw "Том $volume всё ещё использует: $users" }
}
foreach ($name in $volumeNames) {
  $volume = "speedrun-lab1_$name"
  Invoke-CheckedDocker run --rm --network none --user 0 `
    --mount "type=volume,src=$volume,dst=/source,readonly" `
    --mount "type=bind,src=$transferRoot,dst=/backup" `
    --entrypoint tar $tags.postgres --numeric-owner -czpf "/backup/$name.tar.gz" -C /source .
  Invoke-CheckedDocker run --rm --network none --user 0 `
    --mount "type=bind,src=$transferRoot,dst=/backup,readonly" `
    --entrypoint tar $tags.postgres -tzf "/backup/$name.tar.gz" | Out-Null
}

$checksums = Get-ChildItem -LiteralPath $transferRoot -Recurse -File -Force | ForEach-Object {
  [pscustomobject]@{
    file=$_.FullName.Substring($transferRoot.Length + 1)
    bytes=$_.Length
    sha256=(Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash
  }
}
$checksums | ConvertTo-Json | Set-Content -Encoding utf8 "$transferRoot\CHECKSUMS.json"
```

Если проверка пользователей тома обнаружит `speedrun-lab3-worker`, это отдельный
временный контейнер проверки. После завершения его работы останови именно его
командой `docker stop speedrun-lab3-worker` и повтори проверки. Другие найденные
контейнеры сначала идентифицируй; не останавливай весь Docker без разбора.
При ошибке архивирования исходные тома остаются на месте. Не передавай неполный
комплект как готовый. Не используй `down -v`, `volume prune` или старый
`infra/cold_backup.py`: последний рассчитан на шесть томов прежней ЛР1.

После завершения можно вернуть ранее работавшие сервисы:

```powershell
Invoke-CheckedDocker compose --profile lab3 start postgres elasticsearch
Invoke-CheckedDocker compose --profile lab3 start superset openmetadata api-server dag-processor scheduler
Invoke-CheckedDocker compose --profile lab3 ps
```

`start` запускает существующие контейнеры с их прежними образами. Дождись healthy;
если сервис поднялся раньше своей зависимости и завершился, повтори `start`
для него после готовности PostgreSQL/Elasticsearch. Пауза DAG источника сохраняется.

## 4. У коллеги: проверить комплект и создать отдельную папку

Сначала сохранить свои незакоммиченные изменения ЛР2. Если её контейнеры занимают
8080, 8088 или 8585, штатно остановить старый проект из его папки. Не удалять
его volumes. Переносимый комплект распаковать, например, в `D:\Transfers\received`.
Не распаковывать его поверх старого проекта.

```powershell
$ErrorActionPreference = 'Stop'
function Invoke-CheckedDocker { & docker @args; if ($LASTEXITCODE -ne 0) { throw 'Docker command failed' } }
$transferRoot = (Resolve-Path 'D:\Transfers\received').Path
foreach ($entry in (Get-Content -Raw "$transferRoot\CHECKSUMS.json" | ConvertFrom-Json)) {
  $file = Join-Path $transferRoot $entry.file
  if ((Get-FileHash -LiteralPath $file -Algorithm SHA256).Hash -ne $entry.sha256) {
    throw "Не совпала контрольная сумма: $($entry.file)"
  }
}
$expectedArchitecture = (Get-Content "$transferRoot\ARCHITECTURE.txt").Trim()
$architecture = Invoke-CheckedDocker info --format '{{.OSType}}/{{.Architecture}}'
if ($architecture.Trim() -ne $expectedArchitecture) { throw 'Нужна отдельная проверка архитектуры' }

$newRepo = 'C:\Projects\ETL_practice_lab3'
if (Test-Path -LiteralPath $newRepo) { throw 'Нужна новая папка, не поверх ЛР2' }
git clone -b lab3 "$transferRoot\lab3.bundle" $newRepo
if ($LASTEXITCODE -ne 0) { throw 'Git clone failed' }
Set-Location -LiteralPath $newRepo
$revision = (Get-Content "$transferRoot\COMMIT.txt").Trim()
if ((git rev-parse HEAD).Trim() -ne $revision) { throw 'Код не совпадает с комплектом' }
git remote set-url origin https://github.com/UrranQx/Big-Data-Storage-and-Management.git

New-Item -ItemType Directory -Force data/lab3/keys,exports | Out-Null
Copy-Item -LiteralPath "$transferRoot\private\.env" -Destination .env
Copy-Item -LiteralPath "$transferRoot\private\keys\private_key.der" -Destination data/lab3/keys
Copy-Item -LiteralPath "$transferRoot\private\keys\public_key.der" -Destination data/lab3/keys
Copy-Item -LiteralPath "$transferRoot\compose.transfer.yaml" -Destination exports/compose.transfer.yaml
Invoke-CheckedDocker image load --input "$transferRoot\images.tar"
foreach ($entry in (Get-Content -Raw "$transferRoot\IMAGES.json" | ConvertFrom-Json)) {
  $id = Invoke-CheckedDocker image inspect --format '{{.Id}}' $entry.tag
  if ($id.Trim() -ne $entry.id.Trim()) { throw "Другой образ: $($entry.tag)" }
}

$transferProject = 'speedrun-lab3-colleague'
function Invoke-Lab3 {
  & docker compose -p $transferProject -f compose.yaml -f exports/compose.transfer.yaml --profile lab3 @args
  if ($LASTEXITCODE -ne 0) { throw 'Compose command failed' }
}
Invoke-Lab3 config --quiet
```

Не запускать `configure_lab3.py` вместо переноса `.env` и не генерировать новую
RSA-пару: служебные БД перенесены с прежними ключами. Не выводить обычный
`docker compose config` в сообщения: он разворачивает значения окружения.

## 5. У коллеги: восстановить четыре новых тома

Команды используют префикс `speedrun-lab3-colleague`, а не `speedrun-lab1`.
Проверка запрещает восстановление в любой уже существующий целевой том.
При прерванном восстановлении не запускай этот цикл поверх неполных томов:
выбери новое имя проекта и повтори весь цикл либо отдельно разбери состояние.

```powershell
$volumeNames = @('postgres-data','etl-state','airflow-logs','lab3-search')
$helperImage = (Get-Content -Raw "$transferRoot\IMAGES.json" | ConvertFrom-Json | Where-Object kind -eq 'postgres').tag
$existingVolumes = @(Invoke-CheckedDocker volume ls --format '{{.Name}}')
foreach ($name in $volumeNames) {
  if ($existingVolumes -contains "${transferProject}_$name") { throw "Целевой том уже существует: $name" }
}
foreach ($name in $volumeNames) {
  $volume = "${transferProject}_$name"
  Invoke-CheckedDocker volume create --label "com.docker.compose.project=$transferProject" --label "com.docker.compose.volume=$name" $volume
  Invoke-CheckedDocker run --rm --network none --user 0 `
    --mount "type=volume,src=$volume,dst=/target" `
    --mount "type=bind,src=$transferRoot,dst=/backup,readonly" `
    --entrypoint tar $helperImage --numeric-owner -xzpf "/backup/$name.tar.gz" -C /target
}
Invoke-Lab3 up -d --no-deps --no-build --pull never --wait --wait-timeout 180 postgres elasticsearch
```

Числовые владельцы файлов сохраняются; не делать `chmod -R 777` и не копировать
содержимое PostgreSQL вручную через Проводник. При проблемах с правами сначала
сверить образы, архив и владельцев в контейнере.

## 6. Проверить данные, затем запустить интерфейсы

```powershell
Invoke-Lab3 exec -T postgres psql -X -U postgres -d speedrun_ods -c "SELECT count(*) AS run_versions FROM entity_v3.runs; SELECT count(*) AS player_versions FROM entity_v3.players; SELECT * FROM speedrun_marts.context; SELECT * FROM speedrun_marts.validation;"
$sourcePaused = Invoke-Lab3 exec -T postgres psql -XAt -U postgres -d airflow -c "SELECT (count(*)=4 AND bool_and(is_paused))::int FROM dag WHERE dag_id IN ('games_metadata','runs','wr_history','players');"
$activeSource = Invoke-Lab3 exec -T postgres psql -XAt -U postgres -d airflow -c "SELECT count(*) FROM dag_run WHERE dag_id IN ('games_metadata','runs','wr_history','players') AND state IN ('running','queued');"
if ([int]$sourcePaused -ne 1 -or [int]$activeSource -ne 0) { throw 'Не запускать scheduler: проверить состояние источника' }

Invoke-Lab3 up -d --no-deps --no-build --pull never --wait --wait-timeout 300 superset openmetadata api-server dag-processor scheduler
Invoke-Lab3 ps
Invoke-Lab3 exec -T postgres psql -X -U postgres -d superset -c "SELECT count(*) AS dashboards FROM dashboards; SELECT count(*) AS charts FROM slices; SELECT count(*) AS datasets FROM tables;"
```

Ожидания текущего комплекта: **981599 версий ранов**, **280600 версий игроков**,
build `cef0cb6d0c3ec3fa535d95228cd8f1960028ec5c0759e5f124fe4b0111306e0d`,
дата среза 30 октября; все failures в validation равны нулю.
Superset: **4 дашборда, 29 графиков, 32 datasets**. В рейтинге по десяти играм
при минимальном поле 10: 135022 раннера с оценкой, лидер Zennan, 888,591 балла.
На очереди остаются 133 complete / 1 partial / 1 not_applicable / 365 pending.
Если комплект создан после следующих изменений, сверяться с его REPORT/HANDOFF.

Открыть:

- http://localhost:8080 — Airflow, пользователь `admin`, пароль из переданного `.env`.
- http://localhost:8088 — Superset, `admin`, `SUPERSET_ADMIN_PASSWORD`.
- http://localhost:8585 — OpenMetadata, `admin@open-metadata.org`, `OPENMETADATA_ADMIN_PASSWORD`.

Проверить четыре дашборда, смену фильтра рейтинга и lineage таблицы
`speedrun_postgres.speedrun_ods.speedrun_marts.country_medals`.
Успешный вход — только часть проверки: сравнить числа, проверить отсутствие ошибок
графиков и убедиться, что DAG источника всё ещё paused. Исходный комплект и старую
ЛР2 хранить до успешного восстановления. Если запуск упал, смотреть `Invoke-Lab3 logs
--tail 100 <service>`; не отправлять необработанные журналы, если в них есть секреты.

`--no-deps` здесь намерен: восстанавливается уже инициализированный стенд с теми же
версиями, поэтому повторные init/migrate при первом запуске не нужны. Для пустой БД
этот сценарий не подходит. Сбор источника и очереди самостоятельно не продолжать.

## 7. Как продолжать доработку

После проверки создать свою ветку от переданного commit:

```powershell
git switch -c colleague/lab3-work
```

Читайте [HANDOFF.md](HANDOFF.md), [REPORT](../reports/lab3/REPORT.md),
[DEMO](../reports/lab3/DEMO.md) и [методику рейтинга](analytics/RUNNER_RANKING.md).
Новый рейтинг: `infra/runner_ranking.sql`; графики и фильтры —
`infra/superset_assets.py`, `infra/provision_superset.py`. Расчёты базовых витрин —
`sql/marts/`, обработка и оркестрация — `src/speedrun_etl/` и `dags/`.

После изменения виртуального SQL/дашборда:

```powershell
Invoke-Lab3 exec -T superset /app/.venv/bin/python /app/lab3/provision_superset.py
Invoke-Lab3 run --rm --no-deps --user airflow --entrypoint python init -m speedrun_etl.analytics_metadata
Invoke-Lab3 run --rm --no-deps --user airflow -e ETL_LAB3_INTEGRATION=1 -e PYTHONPATH=/opt/project/src:/opt/project/tests --entrypoint python init -m unittest test_runner_ranking test_lab3_integration -v
```

Приведённые восемь тестов рассчитаны на текущую опубликованную сборку. После
изменения `sql/marts/` сначала осознанно перестроить витрины через `refresh_marts`
или команду `python -m speedrun_etl.marts build`; затем проверить повтор и каталог.
Один `git pull` не обновляет данные и сохранённые определения Superset. При смене
Python-зависимостей пересобрать `Invoke-Lab3 build init superset-init`; это уже может
потребовать интернет. Использовать Invoke-Lab3/те же `-p` и `-f` в новых терминалах,
иначе Compose выберет имя старого проекта из `compose.yaml`.

Не передавать в Git `.env`, keys, volumes, дампы, bundle/архивы данных и свой
`exports/compose.transfer.yaml`. Последний уже исключён правилом `exports/`.

Основание механики переноса: [Docker volumes — backup/restore](https://docs.docker.com/engine/storage/volumes/#back-up-restore-or-migrate-data-volumes),
[изоляция Compose через имя проекта](https://docs.docker.com/compose/how-tos/project-name/),
[docker image save](https://docs.docker.com/reference/cli/docker/image/save/).
