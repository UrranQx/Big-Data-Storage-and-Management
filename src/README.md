# Рабочая логика ЛР1

Пакет `speedrun_etl` содержит HTTP-клиент, локальные пакеты entity-v2/v3,
загрузчики трёх ODS и управление очередью Airflow. Цепочка текущей модели:
`games_metadata → runs → wr_history → players`.

Карта модулей — [структура репозитория](../docs/REPOSITORY_STRUCTURE.md),
контракт исходных данных — [SOURCE_CONTRACT.md](../SOURCE_CONTRACT.md).
DDS и витрины относятся к следующим лабораторным.

Правила форматирования и безопасной проверки описаны в
[инструкции разработчика](../docs/DEVELOPMENT.md).
