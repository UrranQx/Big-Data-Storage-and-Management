"""Airflow dag ранов: trigger_next направляет entity-v3 в WR, entity-v2 в players."""

from speedrun_etl.workflow import make_dag

dag = make_dag("runs", "wr_history")
