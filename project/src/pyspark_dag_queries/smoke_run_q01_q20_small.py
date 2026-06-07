"""Integration smoke-run: sequentially execute q01..q20 on small dataset."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

Q_SCRIPTS = [
    "q01_credit_campaign_target_audience.py",
    "q02_deposit_campaign_target_audience.py",
    "q03_insurance_cross_sell_audience.py",
    "q04_premium_upgrade_audience.py",
    "q05_retention_inactive_clients.py",
    "q06_client_profile_scoring.py",
    "q07_client_best_channel.py",
    "q08_campaign_performance_dashboard.py",
    "q09_product_conversion_analysis.py",
    "q10_delivery_failure_analysis.py",
    "q11_app_behavior_offer_recommendation.py",
    "q12_transaction_behavior_segmentation.py",
    "q13_high_value_clients_top1000.py",
    "q14_duplicate_client_offers.py",
    "q15_expired_offer_cleanup_candidate.py",
    "q16_mailing_base.py",
    "q17_mailing_schedule_optimization.py",
    "q18_etl_metrics_aggregation.py",
    "q19_adaptive_parallelism_recommendation.py",
    "q20_campaign_readiness_check.py",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke-run q01..q20 on small dataset")
    parser.add_argument("--spark-submit", default="spark-submit", help="Path to spark-submit executable")
    parser.add_argument(
        "--jobs-dir",
        default=str(Path(__file__).resolve().parent),
        help="Directory containing q01..q20 scripts",
    )
    parser.add_argument("--executors", type=int, default=1)
    parser.add_argument("--shuffle-partitions", type=int, default=4)
    parser.add_argument("--pred-time", type=float, default=0.0)
    parser.add_argument("--cpu-utilization", type=float, default=0.0)
    parser.add_argument("--ram-utilization", type=float, default=0.0)
    parser.add_argument(
        "--dag-run-id",
        default=f"smoke_small_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        help="Shared run id for all qXX jobs in this smoke run",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    jobs_dir = Path(args.jobs_dir).resolve()
    if not jobs_dir.exists():
        print(f"[smoke-run] Jobs directory does not exist: {jobs_dir}", file=sys.stderr)
        return 2

    common_args = [
        "--dataset-size",
        "small",
        "--executors",
        str(max(args.executors, 1)),
        "--shuffle-partitions",
        str(max(args.shuffle_partitions, 1)),
        "--pred-time",
        str(max(args.pred_time, 0.0)),
        "--dag-run-id",
        args.dag_run_id,
        "--cpu-utilization",
        str(min(max(args.cpu_utilization, 0.0), 1.0)),
        "--ram-utilization",
        str(min(max(args.ram_utilization, 0.0), 1.0)),
    ]

    env = os.environ.copy()
    env["DATASET_SIZE"] = "small"

    print(f"[smoke-run] Starting q01..q20 with dag_run_id={args.dag_run_id}")
    for index, script_name in enumerate(Q_SCRIPTS, start=1):
        task_id = script_name.removesuffix(".py")
        script_path = jobs_dir / script_name
        if not script_path.exists():
            print(f"[smoke-run] Missing script: {script_path}", file=sys.stderr)
            return 2

        cmd = [
            args.spark_submit,
            str(script_path),
            "--task-id",
            task_id,
            *common_args,
        ]
        print(f"[smoke-run] ({index:02d}/20) Running {task_id}")
        # check=True => fail-fast on first failing qXX integration step.
        subprocess.run(cmd, check=True, env=env)

    print("[smoke-run] SUCCESS: q01..q20 completed on small dataset")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
