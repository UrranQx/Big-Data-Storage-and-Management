# Проверки рабочего ETL

Unit-тесты используют синтетические ответы и loopback HTTP fixture servers:
пагинация/дрейф, limiter/retry/budget, checkpoints/pause/resume, точные числа,
история A→A/A→B→A, восстановление raw, entity-v2/v3 и граф четырёх DAG.
Сетевых запросов к реальному источнику в них нет.

Проверки трёх реальных ODS включаются отдельно через `ETL_INTEGRATION=1`.
При `ETL_INTEGRATION=0` их пропуски ожидаемы и должны указываться в результатах.
Команды офлайн-проверки и ограничения — [DEVELOPMENT.md](../docs/DEVELOPMENT.md).

Архивные прототипы в `archive/research/` не входят в рабочий test discovery.
