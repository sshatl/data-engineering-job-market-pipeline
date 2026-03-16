from __future__ import annotations

import os
from typing import Any

import requests


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise ValueError(f"Required environment variable is not set: {name}")
    return value


def notify_telegram_on_failure(context: dict[str, Any]) -> None:
    bot_token = env("TELEGRAM_BOT_TOKEN")
    chat_id = env("TELEGRAM_CHAT_ID")

    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown_dag"

    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown_task"

    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else "unknown_run"

    exception = context.get("exception")
    exception_text = repr(exception) if exception else "No exception details"

    log_url = task_instance.log_url if task_instance else ""

    message = (
        "❌ Airflow task failed\n\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Run ID: {run_id}\n"
        f"Error: {exception_text}\n"
    )

    if log_url:
        message += f"Log: {log_url}\n"

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    response = requests.post(
        url,
        json={
            "chat_id": chat_id,
            "text": message,
        },
        timeout=20,
    )
    response.raise_for_status()