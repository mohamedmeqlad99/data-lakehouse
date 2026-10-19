"""Local airflow package used to model the lakehouse orchestration DAG."""

from __future__ import annotations

from typing import Any, Dict, List


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


__all__ = ["DAG"]
