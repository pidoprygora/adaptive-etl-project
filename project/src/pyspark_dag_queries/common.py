"""Common helpers for running standalone ETL Spark SQL jobs."""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter

from pyspark.sql import SparkSession

CURRENT_DIR = Path(__file__).resolve().parent
SRC_ROOT = CURRENT_DIR.parent
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from adaptive_scheduler.storage import AdaptiveStorage
from adaptive_scheduler.resource_monitor import collect_cloudwatch_runtime_utilization
from adaptive_scheduler.time_prediction import update_prediction_with_smoothing

INSERT_INTO_TABLE_RE = re.compile(
    r"INSERT\s+INTO\s+([`\"]?)([\w.]+)\1",
    re.IGNORECASE,
)

ATHENA_DATE_ADD_RE = re.compile(
    r"DATE_ADD\(\s*'day'\s*,\s*([-\d]+)\s*,\s*CURRENT_DATE(?:\s*\(\s*\))?\s*\)",
    re.IGNORECASE,
)
ATHENA_CURRENT_DATE_RE = re.compile(r"\bCURRENT_DATE\b(?!\s*\()", re.IGNORECASE)
GLUE_METASTORE_FACTORY = (
    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
)
# Pods get AWS_* from Helm; Glue uses the default chain. S3A must match (not IMDS-only).
AWS_CREDENTIALS_PROVIDER = "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"

GLUE_CLIENT_JAR_NAMES = (
    "aws-glue-datacatalog-spark-client.jar",
    "aws-glue-datacatalog-hive3-client.jar",
)

PATCHED_HIVE_JAR_NAMES = (
    "hive-exec-2.3.9-core.jar",
    "hive-common-2.3.9.jar",
)


def resolve_java_truststore_path() -> str | None:
    """Return JVM truststore path for HTTPS calls to AWS Glue/S3."""
    candidates: list[str] = []
    env_path = os.getenv("JAVA_SSL_TRUSTSTORE", "")
    if env_path:
        candidates.append(env_path)
    java_home = os.getenv("JAVA_HOME", "")
    if java_home:
        candidates.append(str(Path(java_home) / "lib/security/cacerts"))
    candidates.extend(
        [
            "/opt/spark-conf/cacerts",
            "/etc/pki/java/cacerts",
            "/usr/lib/jvm/java-17-amazon-corretto/lib/security/cacerts",
            "/usr/lib/jvm/java-17-amazon-corretto.x86_64/lib/security/cacerts",
        ]
    )
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return candidate
    return None


def _resolve_spark_warehouse_dir() -> str | None:
    """Warehouse dir from env or spark-defaults (Glue DBs may have empty locationUri)."""
    warehouse = os.getenv("SPARK_WAREHOUSE", "").strip()
    if warehouse:
        return warehouse
    defaults_path = Path(os.getenv("SPARK_CONF_DIR", "/opt/spark-conf")) / "spark-defaults.conf"
    if not defaults_path.is_file():
        return None
    for line in defaults_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("spark.sql.warehouse.dir="):
            value = stripped.split("=", 1)[1].strip()
            return value or None
    return None


def spark_java_extra_options() -> str:
    """JVM opts for Glue metastore client + TLS trust anchors in containers."""
    opts = f"-Dhive.metastore.client.factory.class={GLUE_METASTORE_FACTORY}"
    truststore = resolve_java_truststore_path()
    if truststore:
        opts += (
            f" -Djavax.net.ssl.trustStore={truststore}"
            " -Djavax.net.ssl.trustStorePassword=changeit"
            " -Djavax.net.ssl.trustStoreType=JKS"
        )
    return opts


def _glue_client_jar_candidates() -> tuple[str, ...]:
    env_candidate = os.getenv("SPARK_GLUE_CLIENT_JAR", "")
    candidates: list[str] = []
    if env_candidate:
        candidates.append(env_candidate)
    for jar_name in GLUE_CLIENT_JAR_NAMES:
        candidates.append(f"/opt/spark-jars/{jar_name}")
        candidates.append(f"/tmp/{jar_name}")
    return tuple(candidates)


def _default_s3_bucket() -> str:
    return os.getenv("S3_BUCKET", "adaptive-etl-project-032896316649-eu-north-1-an")


def _processed_table_s3_uri(table_name: str) -> str:
    """S3 LOCATION for processed target tables (matches Glue DDL layout)."""
    short_name = table_name.split(".")[-1]
    return f"s3://{_default_s3_bucket()}/processed/bank_data/{short_name}/"


def _purge_s3_prefix_data(s3_uri: str) -> int:
    """Remove all objects under a processed-table prefix (idempotent re-runs)."""
    try:
        import boto3
        from urllib.parse import urlparse
    except ImportError:
        return 0

    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        return 0
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix = f"{prefix}/"
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "eu-north-1"
    client = boto3.client("s3", region_name=region)
    deleted = 0
    continuation: str | None = None
    while True:
        kwargs: dict[str, object] = {"Bucket": bucket, "Prefix": prefix}
        if continuation:
            kwargs["ContinuationToken"] = continuation
        response = client.list_objects_v2(**kwargs)
        keys = [item["Key"] for item in response.get("Contents", []) if item.get("Key")]
        if keys:
            client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": key} for key in keys], "Quiet": True},
            )
            deleted += len(keys)
        if not response.get("IsTruncated"):
            break
        continuation = response.get("NextContinuationToken")
    return deleted


def _ensure_s3_prefix_exists(s3_uri: str) -> None:
    """Create empty folder marker so Spark INSERT does not fail with PATH_NOT_FOUND."""
    try:
        import boto3
        from botocore.exceptions import ClientError
        from urllib.parse import urlparse
    except ImportError:
        return

    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        return
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if not key.endswith("/"):
        key = f"{key}/"
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "eu-north-1"
    client = boto3.client("s3", region_name=region)
    try:
        client.head_object(Bucket=bucket, Key=key)
        return
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code", "")
        if code not in {"404", "NoSuchKey", "NotFound"}:
            raise
    client.put_object(Bucket=bucket, Key=key, Body=b"")


def ensure_insert_target_prefix(sql_text: str) -> str | None:
    """Ensure S3 prefix exists for INSERT INTO target (Glue external table)."""
    match = INSERT_INTO_TABLE_RE.search(sql_text)
    if not match:
        return None
    table_name = match.group(2)
    _ensure_s3_prefix_exists(_processed_table_s3_uri(table_name))
    return table_name


def adapt_athena_sql_to_spark(sql_text: str) -> str:
    """Apply small compatibility fixes from Athena SQL to Spark SQL."""
    result = ATHENA_CURRENT_DATE_RE.sub("current_date()", sql_text)
    result = ATHENA_DATE_ADD_RE.sub(r"date_add(current_date(), \1)", result)
    result = re.sub(
        r"CAST\((.*?)\s+AS\s+VARCHAR\)",
        r"CAST(\1 AS STRING)",
        result,
        flags=re.IGNORECASE | re.DOTALL,
    )
    # Processed targets are full snapshots; overwrite avoids duplicate rows on DAG re-runs.
    result = INSERT_INTO_TABLE_RE.sub(r"INSERT OVERWRITE \1\2\1", result, count=1)
    return result


@dataclass(frozen=True)
class RuntimeOptions:
    """Runtime options passed from Adaptive Scheduler / Airflow."""

    task_id: str
    dataset_size: str
    executors: int
    shuffle_partitions: int
    predicted_time_sec: float
    dag_run_id: str
    cpu_utilization: float
    ram_utilization: float
    task_load: float = 0.0
    avg_task_load: float = 0.0
    balance_coefficient: float = 1.0
    speedup: float = 0.0
    efficiency: float = 0.0
    amdahl_speedup: float = 0.0
    priority_score: float = 0.0
    priority_rank: int = 0
    execution_wave: int = 0
    critical_path_time_sec: float = 0.0
    etl_time_sec: float = 0.0


def _safe_path_component(value: str) -> str:
    """Make task/run identifiers safe for filesystem paths."""
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._") or "default"


def _env_flag(name: str, default: bool = False) -> bool:
    """Parse common truthy/falsey env flag values."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def resolve_glue_client_jar_path() -> str:
    """Return the first existing AWS Glue Data Catalog client JAR on this host."""
    for candidate in _glue_client_jar_candidates():
        if candidate and Path(candidate).is_file():
            return candidate
    return f"/opt/spark-jars/{GLUE_CLIENT_JAR_NAMES[0]}"


def _pyspark_jars_dir() -> Path:
    return Path("/opt/pyspark-packages/pyspark/jars")


def _patched_hive_overlay_ready() -> bool:
    """Patched Hive jars must live in pyspark/jars (not child-first extraClassPath)."""
    jars_dir = _pyspark_jars_dir()
    return all((jars_dir / name).is_file() for name in PATCHED_HIVE_JAR_NAMES)


def spark_support_jar_paths() -> list[str]:
    """Extra jars for S3A only; patched Hive belongs in pyspark/jars overlay."""
    jars_dir = Path("/opt/spark-jars")
    if not jars_dir.is_dir():
        return []

    paths: list[str] = []
    for pattern in (
        "aws-glue-datacatalog-spark-client.jar",
        "hadoop-aws-*.jar",
        "aws-java-sdk-bundle-*.jar",
    ):
        for jar_path in sorted(jars_dir.glob(pattern)):
            jar_str = str(jar_path)
            if jar_path.is_file() and jar_str not in paths:
                paths.append(jar_str)

    glue_jar = resolve_glue_client_jar_path()
    if glue_jar not in paths and Path(glue_jar).is_file():
        paths.append(glue_jar)
    return paths


def glue_extra_classpath(glue_jar_path: str | None = None) -> str:
    """Build driver/executor classpath for Glue + S3A (never /opt/spark-jars/*)."""
    paths = spark_support_jar_paths()
    if paths:
        return ":".join(paths)
    glue_jar = glue_jar_path or resolve_glue_client_jar_path()
    return glue_jar


def _configure_catalog(builder: SparkSession.Builder) -> SparkSession.Builder:
    """Configure catalog explicitly so Spark does not silently fall back to Derby."""
    use_glue_catalog = _env_flag("SPARK_USE_GLUE_CATALOG", default=True)
    if not use_glue_catalog:
        return builder

    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "eu-north-1"
    warehouse_dir = _resolve_spark_warehouse_dir()

    # userClassPathFirst=true splits Glue factory and Hive interfaces across loaders.
    builder = (
        builder.config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.hive.metastore.sharedPrefixes", "com.amazonaws.glue,com.amazonaws")
        .config("spark.driver.userClassPathFirst", "false")
        .config("spark.executor.userClassPathFirst", "false")
        .config("spark.driver.extraJavaOptions", spark_java_extra_options())
        .config("spark.executor.extraJavaOptions", spark_java_extra_options())
        .config("spark.hive.imetastoreclient.factory.class", GLUE_METASTORE_FACTORY)
        .config("hive.metastore.client.factory.class", GLUE_METASTORE_FACTORY)
        .config("spark.hadoop.hive.metastore.client.factory.class", GLUE_METASTORE_FACTORY)
        .config("spark.hadoop.aws.glue.endpoint", f"https://glue.{region}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", AWS_CREDENTIALS_PROVIDER)
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.endpoint.region", region)
        # Glue table locations use s3://; route through S3A (hadoop-aws on classpath).
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.aws.credentials.provider", AWS_CREDENTIALS_PROVIDER)
        .config("spark.hadoop.fs.s3.endpoint", f"s3.{region}.amazonaws.com")
        .config("spark.hadoop.fs.s3.endpoint.region", region)
    )

    support_jars = spark_support_jar_paths()
    if support_jars:
        classpath = ":".join(support_jars)
        builder = (
            builder.config("spark.jars", ",".join(support_jars))
            .config("spark.driver.extraClassPath", classpath)
            .config("spark.executor.extraClassPath", classpath)
        )
    elif Path(resolve_glue_client_jar_path()).is_file():
        glue_jar = resolve_glue_client_jar_path()
        classpath = glue_extra_classpath(glue_jar)
        builder = (
            builder.config("spark.jars", glue_jar)
            .config("spark.driver.extraClassPath", classpath)
            .config("spark.executor.extraClassPath", classpath)
        )
    else:
        fallback_cp = glue_extra_classpath()
        if fallback_cp:
            builder = (
                builder.config("spark.driver.extraClassPath", fallback_cp)
                .config("spark.executor.extraClassPath", fallback_cp)
            )

    if warehouse_dir:
        builder = builder.config("spark.sql.warehouse.dir", warehouse_dir)
    return builder


def _configure_parquet_read(builder: SparkSession.Builder) -> SparkSession.Builder:
    """PyArrow/pandas Parquet may store timestamps as INT64; disable vectorized reader."""
    return builder.config("spark.sql.parquet.enableVectorizedReader", "false")


def build_spark(app_name: str, runtime_options: RuntimeOptions | None = None) -> SparkSession:
    """Create SparkSession configured for SQL ETL jobs."""
    builder = SparkSession.builder.appName(app_name).enableHiveSupport()
    builder = _configure_catalog(builder)
    builder = _configure_parquet_read(builder)
    use_isolated_metastore = _env_flag("SPARK_ISOLATE_LOCAL_METASTORE", default=False)
    if use_isolated_metastore:
        metastore_key = app_name
        if runtime_options is not None:
            metastore_key = f"{runtime_options.dag_run_id}_{runtime_options.task_id}"

        metastore_dir = Path("/tmp/spark-metastore") / _safe_path_component(metastore_key)
        metastore_dir.mkdir(parents=True, exist_ok=True)
        metastore_url = f"jdbc:derby:;databaseName={metastore_dir / 'metastore_db'};create=true"

        # Keep this opt-in only: forcing a per-run embedded Derby metastore
        # hides shared/external catalogs and can make all Hive tables invisible.
        builder = (
            builder.config("javax.jdo.option.ConnectionURL", metastore_url)
            .config("spark.hadoop.javax.jdo.option.ConnectionURL", metastore_url)
            .config("derby.system.home", str(metastore_dir))
        )
    if runtime_options is not None:
        builder = (
            builder.config("spark.executor.instances", str(max(runtime_options.executors, 1)))
            .config("spark.sql.shuffle.partitions", str(max(runtime_options.shuffle_partitions, 1)))
            .config("spark.default.parallelism", str(max(runtime_options.executors * 2, 1)))
        )
    return builder.getOrCreate()


def parse_runtime_options(argv: list[str] | None = None) -> RuntimeOptions:
    """Parse CLI args passed from Airflow SparkSubmitOperator."""
    parser = argparse.ArgumentParser(description="Runtime options for adaptive PySpark job")
    parser.add_argument("--task-id", default="adhoc_task")
    parser.add_argument("--dataset-size", default=os.getenv("DATASET_SIZE", "medium"))
    parser.add_argument("--executors", type=int, default=1)
    parser.add_argument("--shuffle-partitions", type=int, default=4)
    parser.add_argument("--pred-time", type=float, default=0.0)
    parser.add_argument("--dag-run-id", default=f"manual_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}")
    parser.add_argument("--cpu-utilization", type=float, default=0.0)
    parser.add_argument("--ram-utilization", type=float, default=0.0)
    parser.add_argument("--task-load", type=float, default=0.0)
    parser.add_argument("--avg-task-load", type=float, default=0.0)
    parser.add_argument("--balance-coeff", type=float, default=1.0)
    parser.add_argument("--speedup", type=float, default=0.0)
    parser.add_argument("--efficiency", type=float, default=0.0)
    parser.add_argument("--amdahl-speedup", type=float, default=0.0)
    parser.add_argument("--priority-score", type=float, default=0.0)
    parser.add_argument("--priority-rank", type=int, default=0)
    parser.add_argument("--execution-wave", type=int, default=0)
    parser.add_argument("--critical-path-time-sec", type=float, default=0.0)
    parser.add_argument("--etl-time-sec", type=float, default=0.0)
    args = parser.parse_args(argv)
    return RuntimeOptions(
        task_id=args.task_id,
        dataset_size=args.dataset_size,
        executors=max(int(args.executors), 1),
        shuffle_partitions=max(int(args.shuffle_partitions), 1),
        predicted_time_sec=max(float(args.pred_time), 0.0),
        dag_run_id=args.dag_run_id,
        cpu_utilization=max(min(float(args.cpu_utilization), 1.0), 0.0),
        ram_utilization=max(min(float(args.ram_utilization), 1.0), 0.0),
        task_load=max(float(args.task_load), 0.0),
        avg_task_load=max(float(args.avg_task_load), 0.0),
        balance_coefficient=max(float(args.balance_coeff), 0.0),
        speedup=max(float(args.speedup), 0.0),
        efficiency=max(float(args.efficiency), 0.0),
        amdahl_speedup=max(float(args.amdahl_speedup), 0.0),
        priority_score=max(float(args.priority_score), 0.0),
        priority_rank=max(int(args.priority_rank), 0),
        execution_wave=max(int(args.execution_wave), 0),
        critical_path_time_sec=max(float(args.critical_path_time_sec), 0.0),
        etl_time_sec=max(float(args.etl_time_sec), 0.0),
    )


def _build_metrics_payload(
    app_name: str,
    runtime_options: RuntimeOptions,
    execution_time_sec: float,
    measured_cpu_utilization: float | None = None,
    measured_ram_utilization: float | None = None,
    resource_signal_source: str = "planned_only",
) -> dict[str, object]:
    parallel_tasks_count = max(runtime_options.executors, 1)
    sequential_fallback = runtime_options.predicted_time_sec if runtime_options.predicted_time_sec > 0 else execution_time_sec
    speedup = runtime_options.speedup or ((sequential_fallback / execution_time_sec) if execution_time_sec > 0 else 0.0)
    efficiency = runtime_options.efficiency or (speedup / parallel_tasks_count if parallel_tasks_count > 0 else 0.0)
    amdahl_q = 0.85
    amdahl_speedup = runtime_options.amdahl_speedup or (1.0 / ((1.0 - amdahl_q) + amdahl_q / parallel_tasks_count))

    planned_cpu_utilization = runtime_options.cpu_utilization
    planned_ram_utilization = runtime_options.ram_utilization
    effective_cpu_utilization = (
        measured_cpu_utilization if measured_cpu_utilization is not None else planned_cpu_utilization
    )
    effective_ram_utilization = (
        measured_ram_utilization if measured_ram_utilization is not None else planned_ram_utilization
    )

    return {
        "metric_id": int(datetime.now(timezone.utc).timestamp()),
        "task_id": runtime_options.task_id,
        "dag_run_id": runtime_options.dag_run_id,
        "execution_date": datetime.now(timezone.utc).isoformat(),
        "dataset_size": runtime_options.dataset_size,
        "rows_clients": 0,
        "rows_transactions": 0,
        "rows_clickstream": 0,
        "rows_offers": 0,
        "rows_client_offers": 0,
        "extract_time_sec": execution_time_sec * 0.2,
        "transform_time_sec": execution_time_sec * 0.6,
        "load_time_sec": execution_time_sec * 0.2,
        "total_execution_time_sec": execution_time_sec,
        "parallel_tasks_count": parallel_tasks_count,
        "task_load": runtime_options.task_load,
        "avg_task_load": runtime_options.avg_task_load,
        "cpu_utilization": effective_cpu_utilization,
        "ram_utilization": effective_ram_utilization,
        "planned_cpu_utilization": planned_cpu_utilization,
        "planned_ram_utilization": planned_ram_utilization,
        "measured_cpu_utilization": measured_cpu_utilization,
        "measured_ram_utilization": measured_ram_utilization,
        "resource_signal_source": resource_signal_source,
        "speedup": speedup,
        "efficiency": efficiency,
        "amdahl_speedup": amdahl_speedup,
        "critical_path_time_sec": runtime_options.critical_path_time_sec or execution_time_sec,
        "etl_time_sec": runtime_options.etl_time_sec or execution_time_sec,
        "load_balance_coeff": runtime_options.balance_coefficient,
        "priority_score": runtime_options.priority_score,
        "priority_rank": runtime_options.priority_rank,
        "execution_wave": runtime_options.execution_wave,
        "predicted_time_old_sec": runtime_options.predicted_time_sec,
        "predicted_time_new_sec": update_prediction_with_smoothing(
            actual_time_sec=execution_time_sec,
            predicted_old_sec=runtime_options.predicted_time_sec,
            smoothing_lambda=0.3,
        ),
        "spark_app_name": app_name,
        "spark_shuffle_partitions": runtime_options.shuffle_partitions,
        "status": "success",
    }


def run_sql(
    sql_text: str,
    app_name: str,
    runtime_options: RuntimeOptions | None = None,
    persist_metrics: bool = True,
) -> None:
    """Run one standalone SQL string as a Spark job."""
    options = runtime_options or RuntimeOptions(
        task_id=app_name,
        dataset_size=os.getenv("DATASET_SIZE", "medium"),
        executors=1,
        shuffle_partitions=4,
        predicted_time_sec=0.0,
        dag_run_id=f"manual_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        cpu_utilization=0.0,
        ram_utilization=0.0,
    )
    spark = build_spark(app_name, runtime_options=options)
    started_at_utc = datetime.now(timezone.utc)
    started_at = perf_counter()
    try:
        target_table = ensure_insert_target_prefix(sql_text)
        if target_table:
            target_s3_uri = _processed_table_s3_uri(target_table)
            _purge_s3_prefix_data(target_s3_uri)
            # INSERT OVERWRITE still requires the table LOCATION prefix to exist in S3.
            _ensure_s3_prefix_exists(target_s3_uri)
        spark_sql = adapt_athena_sql_to_spark(sql_text)
        spark.sql(spark_sql)
    finally:
        finished_at_utc = datetime.now(timezone.utc)
        execution_time_sec = round(perf_counter() - started_at, 3)
        measured_cpu_utilization, measured_ram_utilization = collect_cloudwatch_runtime_utilization(
            started_at=started_at_utc,
            finished_at=finished_at_utc,
        )
        resource_signal_source = (
            "cloudwatch_runtime"
            if measured_cpu_utilization is not None and measured_ram_utilization is not None
            else "planned_only"
        )
        if persist_metrics:
            payload = _build_metrics_payload(
                app_name=app_name,
                runtime_options=options,
                execution_time_sec=execution_time_sec,
                measured_cpu_utilization=measured_cpu_utilization,
                measured_ram_utilization=measured_ram_utilization,
                resource_signal_source=resource_signal_source,
            )
            storage = AdaptiveStorage.from_env()
            storage.save_metrics_record(payload)
            storage.save_execution_log(
                run_id=options.dag_run_id,
                payload={
                    "app_name": app_name,
                    "runtime_options": asdict(options),
                    "metrics": payload,
                },
            )
        spark.stop()
