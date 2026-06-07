"""Resource metrics and adaptive parallelism rules."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from dataclasses import asdict

from .models import ResourceSnapshot

_HARDCODED_EC2_INSTANCE_ID = "i-065ca12bd726a5029"


def _aws_credentials_kwargs() -> dict[str, str]:
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


def cpu_utilization(cpu_used: float, cpu_total: float) -> float:
    """U_cpu = CPU_used / CPU_total."""
    if cpu_total <= 0:
        return 0.0
    return max(0.0, min(cpu_used / cpu_total, 1.0))


def ram_utilization(ram_used: float, ram_total: float) -> float:
    """U_ram = RAM_used / RAM_total."""
    if ram_total <= 0:
        return 0.0
    return max(0.0, min(ram_used / ram_total, 1.0))


def adapt_parallelism(current_parallelism: int, p_max: int, u_cpu: float, u_ram: float) -> int:
    """
    Adaptation rule:
    - If U_cpu > 0.8 or U_ram > 0.8 => P_new = max(1, P_i - 1)
    - If U_cpu < 0.5 and U_ram < 0.5 => P_new = min(P_max, P_i + 1)
    """
    safe_current = max(current_parallelism, 1)
    safe_max = max(p_max, 1)
    if u_cpu > 0.8 or u_ram > 0.8:
        return max(1, safe_current - 1)
    if u_cpu < 0.5 and u_ram < 0.5:
        return min(safe_max, safe_current + 1)
    return safe_current


def collect_local_resource_snapshot() -> ResourceSnapshot:
    """
    Collect lightweight host metrics.
    This snapshot is designed for scheduler heuristics, not detailed profiling.
    """
    cpu_total = float(os.cpu_count() or 1)
    load_avg = os.getloadavg()[0] if hasattr(os, "getloadavg") else cpu_total * 0.5
    cpu_used = min(max(load_avg, 0.0), cpu_total)

    try:
        import psutil  # type: ignore

        vm = psutil.virtual_memory()
        ram_total = float(vm.total)
        ram_used = float(vm.used)
    except Exception:
        # Fallback when psutil is unavailable.
        ram_total = 1.0
        ram_used = 0.5

    return ResourceSnapshot(cpu_used=cpu_used, cpu_total=cpu_total, ram_used=ram_used, ram_total=ram_total)


def _instance_id_from_imds(timeout_sec: float = 1.5) -> str | None:
    """Read EC2 instance id from IMDSv2 when running on EC2."""
    try:
        from urllib import request

        token_req = request.Request(
            "http://169.254.169.254/latest/api/token",
            data=b"",
            method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "300"},
        )
        with request.urlopen(token_req, timeout=timeout_sec) as token_resp:
            token = token_resp.read().decode("utf-8").strip()

        id_req = request.Request(
            "http://169.254.169.254/latest/meta-data/instance-id",
            headers={"X-aws-ec2-metadata-token": token},
        )
        with request.urlopen(id_req, timeout=timeout_sec) as id_resp:
            instance_id = id_resp.read().decode("utf-8").strip()
        return instance_id or None
    except Exception:
        return None


def _cloudwatch_metric_average(
    *,
    region: str,
    namespace: str,
    metric_name: str,
    instance_id: str,
    lookback_minutes: int = 5,
    period_sec: int = 60,
    extra_dimensions: list[dict[str, str]] | None = None,
) -> float | None:
    """Fetch average metric percent for the recent lookback window."""
    try:
        import boto3  # type: ignore
    except Exception:
        return None

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=max(lookback_minutes, 1))
    period = max(period_sec, 60)
    dimensions = [{"Name": "InstanceId", "Value": instance_id}]
    if extra_dimensions:
        dimensions.extend(extra_dimensions)
    try:
        client = boto3.client("cloudwatch", region_name=region, **_aws_credentials_kwargs())
        response = client.get_metric_statistics(
            Namespace=namespace,
            MetricName=metric_name,
            Dimensions=dimensions,
            StartTime=start_time,
            EndTime=end_time,
            Period=period,
            Statistics=["Average"],
        )
        datapoints = response.get("Datapoints", [])
        if not datapoints:
            return None
        latest = sorted(datapoints, key=lambda p: p.get("Timestamp"))[-1]
        value = latest.get("Average")
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def collect_cloudwatch_resource_snapshot(
    *,
    aws_region: str | None = None,
    namespace: str | None = None,
    cpu_metric_name: str = "cpu_usage_active",
    ram_metric_name: str = "mem_used_percent",
    lookback_minutes: int = 5,
    period_sec: int = 60,
    instance_id: str | None = None,
) -> ResourceSnapshot | None:
    """
    Collect host resource snapshot from CloudWatch Agent metrics.
    Returns None when metrics are unavailable.
    """
    region = aws_region or os.getenv("AWS_REGION", "eu-north-1")
    cw_namespace = namespace or os.getenv("ADAPTIVE_CLOUDWATCH_NAMESPACE", "AdaptiveETL/Host")
    host_id = (
        instance_id
        or os.getenv("EC2_INSTANCE_ID")
        or _HARDCODED_EC2_INSTANCE_ID
        or _instance_id_from_imds()
    )
    if not host_id:
        return None

    cpu_dimension_value = os.getenv("ADAPTIVE_CLOUDWATCH_CPU_DIMENSION", "cpu-total")
    cpu_percent = _cloudwatch_metric_average(
        region=region,
        namespace=cw_namespace,
        metric_name=cpu_metric_name,
        instance_id=host_id,
        lookback_minutes=lookback_minutes,
        period_sec=period_sec,
        extra_dimensions=[{"Name": "cpu", "Value": cpu_dimension_value}],
    )
    ram_percent = _cloudwatch_metric_average(
        region=region,
        namespace=cw_namespace,
        metric_name=ram_metric_name,
        instance_id=host_id,
        lookback_minutes=lookback_minutes,
        period_sec=period_sec,
    )
    if cpu_percent is None or ram_percent is None:
        return None

    cpu_total = float(os.cpu_count() or 1)
    try:
        import psutil  # type: ignore

        ram_total = float(psutil.virtual_memory().total)
    except Exception:
        ram_total = 1.0

    cpu_used = max(0.0, min(cpu_total, cpu_total * (cpu_percent / 100.0)))
    ram_used = max(0.0, min(ram_total, ram_total * (ram_percent / 100.0)))
    return ResourceSnapshot(cpu_used=cpu_used, cpu_total=cpu_total, ram_used=ram_used, ram_total=ram_total)


def collect_preferred_resource_snapshot() -> tuple[ResourceSnapshot, str]:
    """Collect CloudWatch snapshot; raises when unavailable."""
    lookback = int(os.getenv("ADAPTIVE_CLOUDWATCH_LOOKBACK_MINUTES", "5"))
    period_sec = int(os.getenv("ADAPTIVE_CLOUDWATCH_PERIOD_SEC", "60"))
    snapshot = collect_cloudwatch_resource_snapshot(
        aws_region=os.getenv("AWS_REGION"),
        namespace=os.getenv("ADAPTIVE_CLOUDWATCH_NAMESPACE", "AdaptiveETL/Host"),
        lookback_minutes=max(lookback, 1),
        period_sec=max(period_sec, 60),
    )
    if not snapshot:
        raise RuntimeError("CloudWatch resource snapshot is unavailable")
    return snapshot, "cloudwatch"


def collect_cloudwatch_runtime_utilization(
    *,
    started_at: datetime,
    finished_at: datetime,
    aws_region: str | None = None,
    namespace: str | None = None,
    cpu_metric_name: str = "cpu_usage_active",
    ram_metric_name: str = "mem_used_percent",
    period_sec: int = 60,
    instance_id: str | None = None,
) -> tuple[float | None, float | None]:
    """Return average CPU/RAM utilization percentages for the provided interval."""
    region = aws_region or os.getenv("AWS_REGION", "eu-north-1")
    cw_namespace = namespace or os.getenv("ADAPTIVE_CLOUDWATCH_NAMESPACE", "AdaptiveETL/Host")
    host_id = (
        instance_id
        or os.getenv("EC2_INSTANCE_ID")
        or _HARDCODED_EC2_INSTANCE_ID
        or _instance_id_from_imds()
    )
    if not host_id:
        return None, None
    start = started_at.astimezone(timezone.utc)
    end = finished_at.astimezone(timezone.utc)
    if end <= start:
        return None, None
    try:
        import boto3  # type: ignore

        client = boto3.client("cloudwatch", region_name=region, **_aws_credentials_kwargs())
        points: dict[str, float | None] = {}
        for metric_name, out_key in ((cpu_metric_name, "cpu"), (ram_metric_name, "ram")):
            response = client.get_metric_statistics(
                Namespace=cw_namespace,
                MetricName=metric_name,
                Dimensions=[{"Name": "InstanceId", "Value": host_id}],
                StartTime=start,
                EndTime=end,
                Period=max(period_sec, 60),
                Statistics=["Average"],
            )
            datapoints = response.get("Datapoints", [])
            if not datapoints:
                points[out_key] = None
                continue
            avg = sum(float(dp.get("Average", 0.0)) for dp in datapoints) / len(datapoints)
            points[out_key] = avg
        return points["cpu"], points["ram"]
    except Exception:
        return None, None


def snapshot_to_dict(snapshot: ResourceSnapshot) -> dict[str, float]:
    return asdict(snapshot)
