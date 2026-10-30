# Сценарий защиты ЛР1

Основной показ занимает около 8–10 минут и использует сохранённые данные.
Он не возобновляет очередь 500 игр и не обращается к Speedrun.com.
Исходники, Compose и [отчёт](REPORT.md) должны быть доступны локально.

## 1. Поднять существующий стенд

Сначала один раз восстановить приватный комплект по
[инструкции ноутбука](../../docs/LAPTOP_DEMO.md). В новом окне PowerShell
перейти в каталог проекта и определить правильные Compose-файлы:

```powershell
Set-Location C:\ETL_demo
$demoComposeFiles = @((Join-Path (Get-Location) 'compose.yaml'))
if (Test-Path -LiteralPath 'exports/portable-launch.json') {
    $demoLaunch = Get-Content -Raw exports/portable-launch.json | ConvertFrom-Json
    $demoComposeFiles += $demoLaunch.override
}
$demoCompose = @()
foreach ($composeFile in $demoComposeFiles) {
    $demoCompose += @('-f', $composeFile)
}

docker compose @demoCompose up -d --no-build --pull never
docker compose @demoCompose ps
```

На основном ПК нужно заменить только рабочий каталог. На переносимой копии
override обязателен: он привязывает именно восстановленные volumes и образы.
Для каждой новой PowerShell-сессии переменные задаются заново.
Нельзя заменять эту команду обычным `docker compose up` или `down -v`.

## 2. Показать Airflow — 2 минуты

Открыть [http://127.0.0.1:8080](http://127.0.0.1:8080). Пользователь `admin`,
пароль — локальное значение `AIRFLOW_ADMIN_PASSWORD` из `.env`.

Показать четыре DAG: `games_metadata`, `runs`, `wr_history`, `players`.
Выбрать завершённую цепочку Portal; названия запусков содержат game ID и имя.
На графе `runs` показать initialize/collect, три загрузчика, finalize
и `trigger_next`. В логах collect видны игра, категория, статус, страницы
и HTTP-бюджет. В логах загрузчика — количество и digest.

Объяснение: DAG имеют разные результаты и связываются конкретными snapshot ID.
Готовность пакета требует подтверждений всех трёх хранилищ.
Paused DAG в переносимой копии не мешает просмотру истории.

## 3. Показать данные трёх ODS — 2 минуты

В той же PowerShell-сессии:

```powershell
docker compose @demoCompose exec -T scheduler python -m speedrun_etl.cli history --collection lab1-popular500-20261030-g-4pd0n31e --entity run --id yv2r2oom |
    ForEach-Object {
        $entity = $_ | ConvertFrom-Json
        [PSCustomObject]@{
            Store = $entity.store
            Digest = $entity.digest
            Versions = $entity.versions.Count
            Checks = $entity.checks.Count
        }
    } | Format-List
```

Ожидаются postgres/mongo/garage, по одной версии и checks на этот снимок,
одинаковый полный digest. Команда действительно читает сервисы, не только
показывает готовый JSON. Она проверена на основном стенде:
[результат](evidence/demo_readonly_20261031.json).

Открыть [пример Portal](ods_example_20261030/README.md),
[исходный объект](ods_example_20261030/source_run.json) и
[метаданные с правилами](ods_example_20261030/game_and_rules.json).
Назвать ID/имя участника, исходное время 17:52.660, даты источника и наблюдения.
Показать, что `obsolete=true` сохранён в контексте, а отсутствующий `place`
не вычислялся. Страница сайта — ссылка; офлайн показывается сохранённый ответ.

## 4. Показать историю и восстановление — 1 минута

Открыть `check.json` и схему ODS. Различить source ID, версию содержимого,
наблюдение снимка, response ID и физический S3-чанк. В A → B → A две версии
содержимого, три наблюдения; retry использует прежние ключи.

Показать [доказательство восстановления ответа](ods_example_20261030/verification.json)
и [проверку переносимого комплекта](../../docs/research/evidence/refactor_portable_20261030.json).
Восстановление пакета и частичная загрузка проверены тестами; сам показ
сохранённой истории не называется свежим сетевым сбором.

## 5. Показать сравнение — 2–3 минуты

Открыть [таблицы ODS](comparison_20261030/TABLES.md) и
[полную аналитику](comparison_20261030/analytics/TABLES.md).

- ODS: одинаковый Portal, загрузка/replay, объём и история.
- 1k/10k/100k/968899: глобальный JOIN, ранги, рейтинг и обновление.
- PostgreSQL выбран для истории/связей/аналитики; Mongo выгодна для ряда
  документных операций; S3 — для raw и подготовленных экспортов.
- Общая подготовка 582 с отделена от конвейера 24,67/237,44/113,26 с.
- Частичный пересчёт не всегда быстрее: в S3 здесь 48,64 с против 41,12 с полного.
- Рейтинг экспериментальный, не официальный WR. Кеш не сбрасывался;
  показатели объёма сервисов имеют различный учёт.

Для офлайна открывать PNG из `figures/`; SVG подходят для масштабирования.
В конце [REPORT](REPORT.md) — 22 вопроса FAQ для обсуждения.

## 6. При необходимости показать новый небольшой ETL

Это дополнительный показ при интернете. Сейчас его **не запускать**:
пользователь остановил сбор для отчётности. Он не нужен для просмотра
существующих DAG и данных.

Перед живым запуском нужно согласовать, какой ПК обращается к источнику,
и запустить свежий host-disk watcher именно для этого Docker-стенда.
На переносимой копии прежний замер диска устарел; обходить guard удалением
файла или снижением резерва не следует.

Во втором окне PowerShell повторить настройку Compose-файлов из раздела 1.
Указать фактический существующий Docker VHDX и запустить монитор:

```powershell
$env:COMPOSE_PATH_SEPARATOR = ';'
$env:COMPOSE_FILE = $demoComposeFiles -join ';'
$demoVhdx = Join-Path $env:LOCALAPPDATA 'Docker\wsl\disk\docker_data.vhdx'
Get-Item -LiteralPath $demoVhdx
python infra/host_disk_watch.py --path $demoVhdx
```

Если Docker хранит диск в другом месте, подставить реальный путь.
Если `python` не установлен, использовать `python\python.exe` из локальной
папки приватного комплекта — тот же интерпретатор запускает `START.cmd`.
Watcher учитывает `COMPOSE_FILE` и пишет замер в восстановленный state.

В первом окне можно снять паузу четырёх DAG и запустить ограниченную
цепочку Portal в новом collection ID:

```powershell
foreach ($dagId in @('games_metadata','runs','wr_history','players')) {
    docker compose @demoCompose exec -T scheduler airflow dags unpause $dagId
}
$demoCollection = 'lab1-demo-' + (Get-Date -Format yyyyMMddTHHmmss)
docker compose @demoCompose exec -T scheduler python -m speedrun_etl.cli start-game --collection $demoCollection --game portal --smoke --smoke-category-id wk6pexd1 --request-budget 30
```

Это smoke: выбранная категория, не более двух страниц каждого статуса,
ограниченная WR-страница и встроенные профили. Он намеренно не доказывает
полноту Portal. `batch resume` для 500 игр здесь не используется.

## 7. Завершить показ

Пакетная очередь должна остаться paused. Если выполнялся живой smoke,
дождаться окончания его цепочки и остановить watcher во втором окне через
Ctrl+C. Если стенд нужно выключить:

```powershell
docker compose @demoCompose stop
```

Volumes сохраняются. Сохранённые результаты и статистика разных срезов
не должны совпадать численно: ноутбук 116 цепочек, аналитика 128, основной ПК 135.
