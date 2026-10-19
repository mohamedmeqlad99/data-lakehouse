"""Minimal PythonOperator stub for local DAG validation."""

from __future__ import annotations

from typing import Any, Callable


class PythonOperator:
    def __init__(self, task_id: str, python_callable: Callable[..., Any], **kwargs):
        self.task_id = task_id
        self.python_callable = python_callable
        self.kwargs = kwargs

    def __rshift__(self, other):
        return (self, other)
