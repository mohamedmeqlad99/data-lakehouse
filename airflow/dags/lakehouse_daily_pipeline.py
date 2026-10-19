"""Airflow DAG for the lakehouse refresh and validation workflow."""

from __future__ import annotations

from typing import Any, Dict, List

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ModuleNotFoundError:  # pragma: no cover - fallback for local test environments
    class DAG:
        def __init__(self, dag_id: str, schedule: str, default_args: Dict[str, Any] | None = None):
            self.dag_id = dag_id
            self.schedule = schedule
            self.default_args = default_args or {}
            self.task_ids: List[str] = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return False

    class PythonOperator:
        def __init__(self, task_id: str, python_callable, **kwargs):
            self.task_id = task_id
            self.python_callable = python_callable
            self.kwargs = kwargs


def bootstrap_lakehouse() -> Dict[str, str]:
    return {
        "task": "bootstrap_lakehouse",
        "command": "docker compose exec spark python /home/iceberg/jobs/bootstrap.py",
        "status": "queued",
    }


def quality_check() -> Dict[str, str]:
    return {
        "task": "quality_check",
        "query": "SELECT * FROM gold.data_quality_summary ORDER BY dataset, metric;",
        "status": "queued",
    }


def analytics_snapshot() -> Dict[str, str]:
    return {
        "task": "analytics_snapshot",
        "query": "SELECT region, ROUND(SUM(revenue), 2) AS revenue, SUM(order_count) AS orders FROM gold.daily_revenue GROUP BY region ORDER BY revenue DESC;",
        "status": "queued",
    }


def build_dag(
    dag_id: str = "lakehouse_daily_pipeline",
    schedule: str = "@daily",
    default_args: Dict[str, Any] | None = None,
):
    """Return a DAG-like object describing the daily lakehouse refresh flow."""

    if default_args is None:
        default_args = {"owner": "data-team", "depends_on_past": False}

    with DAG(dag_id=dag_id, schedule=schedule, default_args=default_args) as dag:
        PythonOperator(task_id="bootstrap_lakehouse", python_callable=bootstrap_lakehouse)
        PythonOperator(task_id="quality_check", python_callable=quality_check)
        PythonOperator(task_id="analytics_snapshot", python_callable=analytics_snapshot)

    dag.task_ids = ["bootstrap_lakehouse", "quality_check", "analytics_snapshot"]
    return dag


__all__ = ["build_dag", "bootstrap_lakehouse", "quality_check", "analytics_snapshot"]
