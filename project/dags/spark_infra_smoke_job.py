"""Minimal PySpark smoke job for infrastructure validation."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Spark infrastructure smoke check")
    parser.add_argument("--task-id", default="spark_infra_smoke_job")
    parser.add_argument("--dataset-size", default="small")
    parser.add_argument("--executors", type=int, default=1)
    parser.add_argument("--shuffle-partitions", type=int, default=4)
    parser.add_argument(
        "--dag-run-id",
        default=f"manual_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    # Delayed import keeps this file harmless during Airflow DAG parsing.
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder.appName("spark_infra_smoke_check")
        .config("spark.executor.instances", str(max(args.executors, 1)))
        .config("spark.sql.shuffle.partitions", str(max(args.shuffle_partitions, 1)))
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        numbers_df = spark.range(1, 1001)
        grouped_df = numbers_df.withColumn("bucket", F.col("id") % F.lit(10)).groupBy("bucket").count()
        total_rows = grouped_df.agg(F.sum("count").alias("rows")).collect()[0]["rows"]

        if int(total_rows) != 1000:
            raise RuntimeError(f"Smoke check failed: expected 1000 rows, got {total_rows}")

        print(
            "[spark-smoke] success",
            f"task_id={args.task_id}",
            f"run_id={args.dag_run_id}",
            f"spark_version={spark.version}",
            f"default_parallelism={spark.sparkContext.defaultParallelism}",
            f"rows={total_rows}",
        )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
