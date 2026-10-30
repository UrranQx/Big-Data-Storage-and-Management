"""Airflow dag профилей: участники подтверждённых runs/WR без нового профильного HTTP."""

from speedrun_etl.workflow import make_dag

dag = make_dag("players")
