"""Централізована конфігурація проєкту ETL."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
METRICS_FILE = BASE_DIR / "metrics.csv"


@dataclass(frozen=True)
class AppConfig:
    """Налаштування доступу до S3 та префіксів зберігання."""

    aws_region: str
    s3_bucket: str
    raw_prefix: str
    processed_prefix: str
    metrics_prefix: str

    @classmethod
    def from_env(cls) -> "AppConfig":
        return cls(
            aws_region=os.getenv("AWS_REGION", "eu-north-1"),
            s3_bucket=os.getenv("S3_BUCKET", "adaptive-etl-project-032896316649-eu-north-1-an"),
            raw_prefix=os.getenv("RAW_PREFIX", "raw"),
            processed_prefix=os.getenv("PROCESSED_PREFIX", "processed"),
            metrics_prefix=os.getenv("METRICS_PREFIX", "metrics"),
        )


def normalize_prefix(prefix: str) -> str:
    """Прибирає зайві розділювачі, щоб ключі S3 були консистентні."""
    return prefix.strip().strip("/")


def build_s3_key(prefix: str, filename: str) -> str:
    """Будує шлях до об'єкта в S3."""
    clean_prefix = normalize_prefix(prefix)
    if clean_prefix:
        return f"{clean_prefix}/{filename}"
    return filename
