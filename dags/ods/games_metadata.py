"""Airflow dag метаданных: свежие карточки и три подтверждённые копии ODS."""

from speedrun_etl.workflow import make_dag

dag = make_dag("games_metadata", "runs")
