"""Конфігурація генератора синтетичних банківських даних."""

from __future__ import annotations

import os
from pathlib import Path

# Налаштування AWS і Athena
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
S3_BUCKET = os.getenv("S3_BUCKET", "adaptive-etl-project-032896316649-eu-north-1-an")
S3_PREFIX = f"{os.getenv('RAW_PREFIX', 'raw').strip('/')}/bank_data"
DATABASE_NAME = "adaptive_etl_bank"

# Поточний розмір датасету: small | medium | large
DATASET_SIZE = "large"

# Базові директорії проєкту
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
PARQUET_DIR = DATA_DIR / "parquet"
CSV_DIR = DATA_DIR / "csv"
SQL_DIR = PROJECT_ROOT / "sql"

# Розміри основних таблиць за профілем навантаження
DATASET_SIZES: dict[str, dict[str, int]] = {
    "small": {
        "clients": 10_000,
        "transactions": 50_000,
        "app_clickstream": 50_000,
    },
    "medium": {
        "clients": 150_000,
        "transactions": 1_000_000,
        "app_clickstream": 1_000_000,
    },
    "large": {
        "clients": 250_000,
        "transactions": 2_000_000,
        "app_clickstream": 2_000_000,
    },
}

# Сталі розміри довідників і правил генерації зв'язків
PRODUCTS_COUNT = 30
OFFERS_COUNT = 300
CAMPAIGNS_COUNT = 50

CLIENT_PRODUCTS_PER_CLIENT = (1, 4)
CLIENT_OFFERS_PER_CLIENT = (1, 5)
MAILING_EVENTS_PER_CLIENT_OFFER = (1, 4)
MAILING_AUDIENCE_PER_CLIENT = (1, 3)

# Кількість записів метрик ETL-запуску
ETL_METRICS_COUNT = 20

