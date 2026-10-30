# Инфраструктура ЛР1

Compose в корне запускает Airflow, PostgreSQL, MongoDB и Garage. Локальные
секреты создаются `configure.py`, существующие настройки не перезаписываются.
Все сервисы Airflow используют один образ.

## Холодная резервная копия и проверка восстановления

30 октября рабочий стенд штатно остановлен перед переработкой. Нельзя
автоматически выполнять старые команды resume/keep_running. Текущий путь
бэкапа — exports/preservation-backup-path.txt, состояние — docs/HANDOFF.md.

Для нового бэкапа сначала `batch pause`, дождаться остановки driver/controller
и окончания цепочки игры. Не прерывать loader посередине ради копирования.
Сохранить baseline, пока сервисы ещё работают:

```powershell
$inventory = Get-Content -Raw infra/backup_inventory.py | docker compose exec -T scheduler python -u -
if ($LASTEXITCODE -ne 0) { throw 'Inventory failed' }
[IO.File]::WriteAllText((Join-Path $PWD 'exports/frozen-baseline.json'), ($inventory -join "`n"), [Text.UTF8Encoding]::new($false))
```

В baseline должно быть safe_to_stop=true. Остановить проектный host_disk_watch
после проверки его PID/командной строки, затем `docker compose stop --timeout 120`.
Далее из корня проекта на Windows-хосте:

```powershell
python -u infra/cold_backup.py --destination D:\ETL_backups\NEW_UNIQUE_FOLDER --evidence exports/frozen-baseline.json
python -u infra/verify_cold_backup.py --backup D:\ETL_backups\NEW_UNIQUE_FOLDER
```

Первая команда требует НОВУЮ абсолютную папку вне проекта и остановленные
сервисы. Копирует шесть volumes, рабочие исходники (включая незакоммиченные),
Git bundle и четыре локальных image ID для offline recovery. Проверяет gzip
CRC, структуру tar, ZIP CRC и SHA-256. Включает .env/garage.local.toml:
это частная локальная резервная копия, не публичный пакет передачи.

Вторая команда требует45ГиБ свободного места на диске Docker. Проверяет SHA,
восстанавливает все6volumes в новые уникальные temporary volumes, сравнивает
все файлы через tar compare, запускает ТОЛЬКО копии трёх хранилищ в internal
network без опубликованных портов. Сверяет количества, samples, receipts,
очереди и весь S3 listing плюс содержимое5объектов. Airflow scheduler не
запускает. В конце удаляет только свои копии с уникальной меткой;
production volumes и backup-архивы не удаляет и не подключает к этим сервисам.

Готовность архива подтверждает manifest.json/complete; реальную проверку
восстановления — restore_verified и restore-verification.json/verified.
Ошибка проверки не является разрешением удалить рабочие данные.

При потере исходного компьютера: перенести папку бэкапа, загрузить образы
`docker image load --input <backup>\docker-images.tar`, распаковать
project-and-local-config.zip в НОВЫЙ каталог проекта. Исходники включают
незакоммиченные изменения, repository.bundle отдельно сохраняет Git-историю.
Из распакованного проекта выполнить:

```powershell
python -u infra/verify_cold_backup.py --backup D:\ETL_backups\YOUR_BACKUP --keep-verified-volumes
docker compose -f compose.yaml -f D:\ETL_backups\YOUR_BACKUP\recovered-volumes.compose.yaml config --quiet
docker compose -f compose.yaml -f D:\ETL_backups\YOUR_BACKUP\recovered-volumes.compose.yaml up -d --no-build --pull never
```

Флаг сохраняет только успешно проверенные восстановленные volumes, закрывает
проверочные контейнеры и создаёт override с НОВЫМ project name/external volumes
и сохранёнными image ID. Основной стенд при этом должен быть остановлен
(тот же локальный порт8080). Оригинальные volumes не перезаписываются.
Пауза старой очереди остаётся в восстановленном state; мониторы/сборщики сами
не запускаются. До любого нового source HTTP восстановить host_disk_watch,
проверить progress и явно выбрать, какую очередь/версию ETL разрешено запускать.

Перед длительным сбором запустить **на хосте**, из корня проекта:

```powershell
python -u infra/host_disk_watch.py
```

Скрипт каждые 30 секунд измеряет свободное место на диске с Docker VHDX и
передаёт небольшой JSON в state volume. По умолчанию путь — обычное расположение
Docker Desktop в профиле Windows, резерв — 20 ГиБ. Для другого размещения
передать `--path <существующий VHDX или каталог диска Docker>`. На Linux указать
файловую систему Docker data root. Настройки Docker скрипт не меняет.

`batch --all` требует действующий монитор. Если места меньше резерва или
измерение старше 120 секунд, новые HTTP останавливаются без сброса checkpoints
и счётчика HTTP-попыток. Уже начатые ответы сохраняются до остановки. Защита
действует и для top-N при запущенном мониторе. Внутренняя проверка виртуальной
ФС Docker остаётся отдельной проверкой. После паузы сначала восстановить
монитор/свободное место, затем продолжить задачу Airflow с прежними checkpoints.
