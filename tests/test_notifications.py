from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

ENV_VARS = {
    "TELEGRAM_BOT_TOKEN": "test-token-123",
    "TELEGRAM_CHAT_ID": "-100987654321",
}


def make_context(
    dag_id: str = "pipeline_jobs_daily",
    task_id: str = "dou_fetch_jobs",
    run_id: str = "manual__2026-04-20",
    exception: Exception | None = None,
    log_url: str = "http://airflow/log/1",
) -> dict:
    dag = MagicMock()
    dag.dag_id = dag_id

    task_instance = MagicMock()
    task_instance.task_id = task_id
    task_instance.log_url = log_url

    dag_run = MagicMock()
    dag_run.run_id = run_id

    return {
        "dag": dag,
        "task_instance": task_instance,
        "dag_run": dag_run,
        "exception": exception,
    }


@patch.dict("os.environ", ENV_VARS)
@patch("lib.common.notifications.requests.post")
def test_sends_message_to_correct_url(mock_post):
    """Uses the bot token from env to build the Telegram API URL."""
    mock_post.return_value.raise_for_status = MagicMock()

    from lib.common.notifications import notify_telegram_on_failure

    notify_telegram_on_failure(make_context())

    call_url = mock_post.call_args.args[0]
    assert "test-token-123" in call_url
    assert "sendMessage" in call_url


@patch.dict("os.environ", ENV_VARS)
@patch("lib.common.notifications.requests.post")
def test_message_contains_dag_task_run_info(mock_post):
    """Message body includes DAG id, task id, and run id."""
    mock_post.return_value.raise_for_status = MagicMock()

    from lib.common.notifications import notify_telegram_on_failure

    notify_telegram_on_failure(make_context(
        dag_id="my_dag",
        task_id="my_task",
        run_id="scheduled__2026-04-20",
    ))

    payload = mock_post.call_args.kwargs["json"]
    assert payload["chat_id"] == "-100987654321"
    text = payload["text"]
    assert "my_dag" in text
    assert "my_task" in text
    assert "scheduled__2026-04-20" in text


@patch.dict("os.environ", ENV_VARS)
@patch("lib.common.notifications.requests.post")
def test_message_includes_exception_repr(mock_post):
    """Exception details are included in the message text."""
    mock_post.return_value.raise_for_status = MagicMock()

    from lib.common.notifications import notify_telegram_on_failure

    notify_telegram_on_failure(make_context(exception=ValueError("S3 upload failed")))

    text = mock_post.call_args.kwargs["json"]["text"]
    assert "ValueError" in text
    assert "S3 upload failed" in text


@patch.dict("os.environ", ENV_VARS)
@patch("lib.common.notifications.requests.post")
def test_message_includes_log_url_when_present(mock_post):
    """Log URL is appended to the message when task_instance provides it."""
    mock_post.return_value.raise_for_status = MagicMock()

    from lib.common.notifications import notify_telegram_on_failure

    notify_telegram_on_failure(make_context(log_url="http://airflow/log/42"))

    text = mock_post.call_args.kwargs["json"]["text"]
    assert "http://airflow/log/42" in text


@patch.dict("os.environ", ENV_VARS)
@patch("lib.common.notifications.requests.post")
def test_no_exception_uses_fallback_text(mock_post):
    """When exception is None, message contains fallback text."""
    mock_post.return_value.raise_for_status = MagicMock()

    from lib.common.notifications import notify_telegram_on_failure

    notify_telegram_on_failure(make_context(exception=None))

    text = mock_post.call_args.kwargs["json"]["text"]
    assert "No exception details" in text


@patch.dict("os.environ", ENV_VARS)
@patch("lib.common.notifications.requests.post")
def test_missing_dag_context_uses_unknown_fallbacks(mock_post):
    """Missing dag/task_instance/dag_run keys fall back to 'unknown_*' strings."""
    mock_post.return_value.raise_for_status = MagicMock()

    from lib.common.notifications import notify_telegram_on_failure

    notify_telegram_on_failure({})

    text = mock_post.call_args.kwargs["json"]["text"]
    assert "unknown_dag" in text
    assert "unknown_task" in text
    assert "unknown_run" in text


@patch.dict("os.environ", ENV_VARS)
@patch("lib.common.notifications.requests.post")
def test_raises_on_http_error(mock_post):
    """raise_for_status propagates HTTP errors to the caller."""
    import requests as req_lib

    mock_post.return_value.raise_for_status.side_effect = req_lib.exceptions.HTTPError("403")

    from lib.common.notifications import notify_telegram_on_failure

    with pytest.raises(req_lib.exceptions.HTTPError):
        notify_telegram_on_failure(make_context())
