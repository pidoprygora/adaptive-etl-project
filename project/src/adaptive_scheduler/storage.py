"""S3 metrics/log storage helpers for adaptive scheduler feedback loop."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any

import boto3


def _aws_credentials_kwargs() -> dict[str, str]:
    """Explicit AWS credentials from env when provided."""
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "").strip()
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "").strip()
    session_token = os.getenv("AWS_SESSION_TOKEN", "").strip()
    if not access_key or not secret_key:
        return {}
    kwargs: dict[str, str] = {
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
    }
    if session_token:
        kwargs["aws_session_token"] = session_token
    return kwargs


class AdaptiveStorage:
    """Stores run metadata to S3 in raw/processed/metrics/logs layout."""

    def __init__(
        self,
        bucket: str,
        root_prefix: str = "bank_data",
        aws_region: str = "eu-north-1",
        local_root: str | None = None,
    ) -> None:
        self.bucket = bucket
        self.root_prefix = root_prefix.strip("/")
        self.client = boto3.client("s3", region_name=aws_region, **_aws_credentials_kwargs())
        default_local_root = Path(__file__).resolve().parents[2] / ".adaptive_runtime"
        self.local_root = Path(local_root) if local_root else default_local_root

    def _key(self, area: str, filename: str) -> str:
        clean_area = area.strip("/")
        return f"{clean_area}/{self.root_prefix}/{filename}"

    def _local_path(self, area: str, filename: str) -> Path:
        clean_area = area.strip("/")
        return self.local_root / clean_area / self.root_prefix / filename

    @staticmethod
    def _file_uri(path: Path) -> str:
        return f"file://{path.resolve()}"

    def put_json(self, area: str, filename: str, payload: dict[str, Any]) -> str:
        key = self._key(area=area, filename=filename)
        body = json.dumps(payload, ensure_ascii=True, indent=2).encode("utf-8")
        self.client.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType="application/json")
        return f"s3://{self.bucket}/{key}"

    def append_jsonl(self, area: str, filename: str, payload: dict[str, Any]) -> str:
        """
        Append JSON record to a JSONL file in S3.
        For simplicity and portability, file content is fetched and re-uploaded.
        """
        key = self._key(area=area, filename=filename)
        existing = b""
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
            existing = response["Body"].read()
        except self.client.exceptions.NoSuchKey:
            existing = b""

        line = (json.dumps(payload, ensure_ascii=True) + "\n").encode("utf-8")
        buffer = BytesIO(existing + line)
        self.client.upload_fileobj(buffer, self.bucket, key)
        return f"s3://{self.bucket}/{key}"

    def save_execution_log(self, run_id: str, payload: dict[str, Any]) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        filename = f"{run_id}_{timestamp}.json"
        return self.put_json(area="logs", filename=filename, payload=payload)

    def save_metrics_record(self, payload: dict[str, Any]) -> str:
        date_part = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        filename = f"etl_metrics_{date_part}.jsonl"
        return self.append_jsonl(area="metrics", filename=filename, payload=payload)

    def save_run_metrics_record(self, payload: dict[str, Any]) -> str:
        """Persist run-level metrics to a dedicated JSONL stream."""
        date_part = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        filename = f"etl_run_metrics_{date_part}.jsonl"
        return self.append_jsonl(area="metrics", filename=filename, payload=payload)

    def read_recent_metrics(self, limit_files: int = 10) -> list[dict[str, Any]]:
        """Read most recent JSONL metrics files from metrics area."""
        prefix = self._key(area="metrics", filename="")
        response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        contents = response.get("Contents", [])
        if not contents:
            return []
        sorted_keys = sorted(contents, key=lambda item: item.get("LastModified"), reverse=True)
        rows: list[dict[str, Any]] = []
        for entry in sorted_keys[:limit_files]:
            key = entry["Key"]
            body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read().decode("utf-8")
            for line in body.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return rows

    def read_task_time_history(self, task_id: str, limit_values: int = 20) -> list[float]:
        """Build time history for a specific task from persisted metrics."""
        rows = self.read_recent_metrics(limit_files=15)
        values: list[float] = []
        for row in rows:
            if row.get("task_id") != task_id:
                continue
            total_time = row.get("total_execution_time_sec")
            if isinstance(total_time, (int, float)):
                values.append(float(total_time))
        return values[:limit_values]

    def read_task_resource_history(
        self,
        task_id: str,
        limit_values: int = 20,
    ) -> list[dict[str, float]]:
        """
        Read historical measured/planned CPU and RAM utilization for a task.
        Returns newest-first records limited by `limit_values`.
        """
        rows = self.read_recent_metrics(limit_files=15)
        values: list[dict[str, float]] = []
        for row in rows:
            if row.get("task_id") != task_id:
                continue
            cpu = row.get("measured_cpu_utilization", row.get("cpu_utilization"))
            ram = row.get("measured_ram_utilization", row.get("ram_utilization"))
            planned_cpu = row.get("planned_cpu_utilization", row.get("cpu_utilization"))
            planned_ram = row.get("planned_ram_utilization", row.get("ram_utilization"))
            if not isinstance(cpu, (int, float)) or not isinstance(ram, (int, float)):
                continue
            values.append(
                {
                    "cpu_utilization": float(cpu),
                    "ram_utilization": float(ram),
                    "planned_cpu_utilization": float(planned_cpu) if isinstance(planned_cpu, (int, float)) else 0.0,
                    "planned_ram_utilization": float(planned_ram) if isinstance(planned_ram, (int, float)) else 0.0,
                }
            )
        return values[:limit_values]

    @classmethod
    def from_env(cls) -> "AdaptiveStorage":
        return cls(
            bucket=os.getenv("S3_BUCKET", "adaptive-etl-project-032896316649-eu-north-1-an"),
            root_prefix=os.getenv("ADAPTIVE_STORAGE_PREFIX", "bank_data"),
            aws_region=os.getenv("AWS_REGION", "eu-north-1"),
            local_root=os.getenv("ADAPTIVE_LOCAL_STORAGE_DIR"),
        )
