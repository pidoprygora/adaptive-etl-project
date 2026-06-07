"""Airflow monitoring helpers: DAG metrics + fail/retry alerts."""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime
from typing import Any
from urllib import error, request

from airflow.stats import Stats

logger = logging.getLogger(__name__)


def _normalize_metric_part(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", value)


def _duration_ms(started_at: datetime | None, ended_at: datetime | None) -> int | None:
    if not started_at or not ended_at:
        return None
    return max(int((ended_at - started_at).total_seconds() * 1000), 0)


def _emit_dagrun_metrics(context: dict[str, Any], state: str) -> None:
    dag_run = context.get("dag_run")
    dag_id = getattr(dag_run, "dag_id", "unknown_dag")
    dag_metric = _normalize_metric_part(dag_id)

    Stats.incr("custom.dagrun.completed")
    Stats.incr(f"custom.dagrun.state.{state}")
    Stats.incr(f"custom.dagrun.state.{state}.{dag_metric}")

    duration_ms = _duration_ms(getattr(dag_run, "start_date", None), getattr(dag_run, "end_date", None))
    if duration_ms is not None:
        Stats.timing("custom.dagrun.duration_ms", duration_ms)
        Stats.timing(f"custom.dagrun.duration_ms.{dag_metric}", duration_ms)


def _send_webhook_alert(payload: dict[str, str]) -> None:
    webhook_url = os.getenv("AIRFLOW_ALERT_WEBHOOK_URL", "").strip()
    if not webhook_url:
        return

    body = json.dumps(payload).encode("utf-8")
    req = request.Request(webhook_url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with request.urlopen(req, timeout=10):
            logger.info("Alert webhook sent")
    except (error.URLError, TimeoutError) as exc:
        logger.warning("Failed to send alert webhook: %s", exc)


def _send_email_alert(subject: str, body: str) -> None:
    recipients = [x.strip() for x in os.getenv("AIRFLOW_ALERT_EMAIL_TO", "").split(",") if x.strip()]
    if not recipients:
        return

    try:
        from airflow.utils.email import send_email  # noqa: PLC0415

        send_email(to=recipients, subject=subject, html_content=body)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to send alert email: %s", exc)


def _build_message(context: dict[str, Any], event: str) -> tuple[str, str]:
    ti = context.get("task_instance")
    dag_run = context.get("dag_run")
    exception = context.get("exception")

    dag_id = getattr(ti, "dag_id", getattr(dag_run, "dag_id", "unknown_dag"))
    task_id = getattr(ti, "task_id", "dag")
    run_id = getattr(dag_run, "run_id", context.get("run_id", "unknown_run"))
    try_number = getattr(ti, "try_number", "n/a")
    log_url = getattr(ti, "log_url", "n/a")

    title = f"[Airflow] {event.upper()}: {dag_id}.{task_id}"
    body = (
        f"{title}\n"
        f"run_id={run_id}\n"
        f"try_number={try_number}\n"
        f"log_url={log_url}\n"
        f"exception={exception if exception else 'n/a'}"
    )
    return title, body


def dagrun_success_metrics(context: dict[str, Any]) -> None:
    _emit_dagrun_metrics(context, "success")


def dagrun_failure_metrics(context: dict[str, Any]) -> None:
    _emit_dagrun_metrics(context, "failed")


def task_failure_alert(context: dict[str, Any]) -> None:
    Stats.incr("custom.task.failure")
    title, body = _build_message(context, "failure")
    _send_webhook_alert({"text": body})
    _send_email_alert(title, body.replace("\n", "<br/>"))


def task_retry_alert(context: dict[str, Any]) -> None:
    Stats.incr("custom.task.retry")
    title, body = _build_message(context, "retry")
    _send_webhook_alert({"text": body})
    _send_email_alert(title, body.replace("\n", "<br/>"))
