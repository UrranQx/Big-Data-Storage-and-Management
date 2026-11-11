# Схемы и SQL

`ods.sql` и `control.sql` — DDL прежнего ODS и его receipts/status.
Они сохранены для совместимых загрузчиков в `speedrun_etl/legacy/storage.py`
и неизменённых интеграционных тестов. Это не текущая физическая модель ODS.

Текущие entity-v2/v3 таблицы создаёт `src/speedrun_etl/entity_storage.py`,
состояние очереди — `batch.py`. DDS и витрины относятся к следующим лабораторным.
