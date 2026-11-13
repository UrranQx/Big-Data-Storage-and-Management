# Продолжение работы на другом компьютере

Код берём из GitHub, собранную базу — из ZIP по ссылке Google Диска.
Актуальная ветка — `codex/postgres-only`; она уже включает рефакторинг
`refactor/post-lab1`, переход на PostgreSQL и очистку завершённых snapshots.

## Получить код

В существующем репозитории сначала сохраните свои незакоммиченные изменения,
затем выполните:

```powershell
git fetch origin
git switch --track origin/codex/postgres-only
```

Если локальная ветка уже создана, используйте `git switch codex/postgres-only`,
затем `git pull --ff-only`. Ветка `main` пока сохраняет прежнее состояние ЛР1.

Для первого запуска ниже используем новую копию проекта: она не затронет
настройки и данные прежнего стенда.

## Запустить проект с готовой базой

Нужны запущенный Docker Desktop (Linux containers), Git и Python 3.11.
Скачайте и распакуйте `postgres-handoff-20261113.zip`, например в
`C:\speedrun-transfer`. В этой папке должны лежать manifest.json и два .dump.

В PowerShell выполните команды по одной, дожидаясь успешного завершения каждой:

```powershell
git clone --branch codex/postgres-only https://github.com/UrranQx/Big-Data-Storage-and-Management.git speedrun-postgres
cd speedrun-postgres

python infra/configure.py
Add-Content .env 'COMPOSE_PROJECT_NAME=speedrun-colleague'

docker compose up -d --wait postgres
python infra/postgres_transfer.py restore --directory C:\speedrun-transfer

docker compose up -d --build
```

Замените `C:\speedrun-transfer` своим путём к распакованному архиву.
Отдельное имя Compose-проекта создаёт новые volumes. Для этого порядка не нужно
запускать `RESTORE.cmd`: он предназначен для отдельного просмотра базы без Airflow.
Импорт выполняется до первого полного `compose up`, пока ODS/control пусты.
Если команда сообщила об ошибке, следующие команды пока не запускайте.

Проверка данных:

```powershell
docker compose exec postgres psql -U speedrun_ods -d speedrun_ods -c "SELECT count(DISTINCT source_id) FROM entity_v3.runs;"
```

Ожидается **981592**. Airflow доступен по адресу http://localhost:8080.
Пользователь `admin`, пароль — `AIRFLOW_ADMIN_PASSWORD` в созданном `.env`.
Если порт 8080 занят старым стендом, сначала остановите его процессы Airflow.

## Продолжить разработку

Открывайте в редакторе Git-копию `speedrun-postgres`. Каталог `project/` внутри
ZIP — снимок исходников на момент передачи; для дальнейших обновлений используйте
ветку GitHub. Дампы не добавляются в Git, пароли генерируются на каждом компьютере.

Рабочий ODS — PostgreSQL, модель `entity_v3`. Сбор автоматически не запускается;
не возобновляйте старую очередь и не запускайте full/smoke без нового согласования.
Данные для следующей лабораторной уже находятся в ODS.

Перед ЛР2 прочитайте исходное задание, `DATA_MODEL.md`, `DAGs.md` и
`docs/POSTGRES_ONLY.md`. Следующая работа — проектирование DDS, очистки, SCD и
отдельного Data Quality DAG. Реализация этих частей в ветку ещё не добавлена.

Результаты перехода и проверок — `docs/HANDOFF.md` и
`docs/research/evidence/postgres_transition_20261113.json`. Поля `changes_local`
и `pushed` в этом evidence фиксируют момент проверки до публикации ветки.
