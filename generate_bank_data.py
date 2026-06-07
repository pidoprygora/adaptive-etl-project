"""Генерація тестових банківських даних з підтримкою масштабів і S3."""

from __future__ import annotations

import argparse
import random
import time
from typing import Any

import numpy as np
import pandas as pd
from faker import Faker

from config import AppConfig, DATA_DIR, build_s3_key

OFFERS_COUNT = 500

SCALE_TO_CLIENTS = {
    "SMALL": 10_000,
    "MEDIUM": 50_000,
    "LARGE": 100_000,
    "XLARGE": 500_000,
}

RAW_FILES = ("clients.csv", "offers.csv", "client_offers.csv", "mailing_events.csv")

CLIENT_SEGMENTS = ["mass", "premium", "vip", "salary", "pension"]
PRODUCT_TYPES = ["credit_card", "cash_loan", "deposit", "insurance", "savings_account"]
CHANNELS = ["sms", "email", "push", "viber"]
OFFER_STATUSES = ["prepared", "sent", "delivered", "opened", "accepted", "rejected", "expired"]
EVENT_TYPES = ["send_attempt", "delivered", "opened", "clicked", "accepted", "failed"]
DELIVERY_STATUSES = ["success", "failed", "pending"]
ERROR_CODES = ["SMS_GATEWAY_TIMEOUT", "EMAIL_BOUNCE", "PUSH_TOKEN_INVALID", "VIBER_API_ERROR"]


def _ensure_data_directory() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _get_counts(scale: str) -> dict[str, int]:
    normalized_scale = scale.upper()
    if normalized_scale not in SCALE_TO_CLIENTS:
        raise ValueError(f"Непідтримуваний scale: {scale}. Доступно: {', '.join(SCALE_TO_CLIENTS)}")

    clients_count = SCALE_TO_CLIENTS[normalized_scale]
    multiplier = clients_count / SCALE_TO_CLIENTS["MEDIUM"]
    client_offers_count = int(200_000 * multiplier)
    mailing_events_count = int(300_000 * multiplier)

    return {
        "clients": clients_count,
        "offers": OFFERS_COUNT,
        "client_offers": client_offers_count,
        "mailing_events": mailing_events_count,
    }


def _random_dates(
    rng: np.random.Generator,
    start_date: str,
    end_date: str,
    size: int,
) -> pd.Series:
    start_ts = pd.Timestamp(start_date).value // 10**9
    end_ts = pd.Timestamp(end_date).value // 10**9
    random_seconds = rng.integers(start_ts, end_ts, size=size)
    return pd.Series(pd.to_datetime(random_seconds, unit="s"))


def _build_campaign_name(rng: np.random.Generator, fake: Faker, product_type: str) -> str:
    themes = ["Весняний", "Літній", "Осінній", "Зимовий", "Персональний", "Преміум", "Швидкий"]
    goals = ["старт", "бонус", "кешбек", "прибуток", "довіра", "план", "резерв"]
    return f"{rng.choice(themes)} {product_type} {rng.choice(goals)} {fake.random_int(min=100, max=999)}"


def generate_clients(fake: Faker, rng: np.random.Generator, clients_count: int) -> pd.DataFrame:
    client_ids = np.arange(1, clients_count + 1)
    birth_dates = _random_dates(rng, "1945-01-01", "2006-12-31", clients_count).dt.strftime("%Y-%m-%d")
    registration_dates = _random_dates(rng, "2015-01-01", "2026-01-01", clients_count).dt.strftime("%Y-%m-%d")

    return pd.DataFrame(
        {
            "client_id": client_ids,
            "person_code": [f"PC{client_id:08d}{rng.integers(0, 10)}" for client_id in client_ids],
            "full_name": [fake.name() for _ in range(clients_count)],
            "birth_date": birth_dates,
            "gender": rng.choice(["male", "female"], size=clients_count, p=[0.47, 0.53]),
            "city": [fake.city() for _ in range(clients_count)],
            "registration_date": registration_dates,
            "client_segment": rng.choice(
                CLIENT_SEGMENTS,
                size=clients_count,
                p=[0.55, 0.15, 0.05, 0.15, 0.10],
            ),
            "is_active": rng.choice(["true", "false"], size=clients_count, p=[0.88, 0.12]),
        }
    )


def generate_offers(fake: Faker, rng: np.random.Generator, offers_count: int) -> pd.DataFrame:
    offer_ids = np.arange(1, offers_count + 1)
    product_types = rng.choice(PRODUCT_TYPES, size=offers_count)
    start_dates = _random_dates(rng, "2022-01-01", "2026-01-01", offers_count)
    durations = rng.integers(14, 180, size=offers_count)
    end_dates = (start_dates + pd.to_timedelta(durations, unit="D")).dt.strftime("%Y-%m-%d")

    return pd.DataFrame(
        {
            "offer_id": offer_ids,
            "offer_name": [
                f"{product_type.replace('_', ' ').title()} Offer {rng.integers(1000, 9999)}"
                for product_type in product_types
            ],
            "product_type": product_types,
            "campaign_name": [_build_campaign_name(rng, fake, product_type) for product_type in product_types],
            "priority": rng.integers(1, 6, size=offers_count),
            "channel": rng.choice(CHANNELS, size=offers_count),
            "start_date": start_dates.dt.strftime("%Y-%m-%d"),
            "end_date": end_dates,
            "is_active": rng.choice(["true", "false"], size=offers_count, p=[0.75, 0.25]),
        }
    )


def generate_client_offers(
    rng: np.random.Generator,
    clients_count: int,
    offers_count: int,
    client_offers_count: int,
) -> pd.DataFrame:
    client_offer_ids = np.arange(1, client_offers_count + 1)
    assigned_dates = _random_dates(rng, "2023-01-01", "2026-05-01", client_offers_count)
    offer_statuses = rng.choice(
        OFFER_STATUSES,
        size=client_offers_count,
        p=[0.20, 0.25, 0.18, 0.14, 0.08, 0.08, 0.07],
    )

    sent_times = assigned_dates + pd.to_timedelta(rng.integers(1, 72, size=client_offers_count), unit="h")
    sent_at_series = pd.Series(np.where(offer_statuses == "prepared", "", sent_times.dt.strftime("%Y-%m-%d %H:%M:%S")))

    return pd.DataFrame(
        {
            "client_offer_id": client_offer_ids,
            "client_id": rng.integers(1, clients_count + 1, size=client_offers_count),
            "offer_id": rng.integers(1, offers_count + 1, size=client_offers_count),
            "assigned_date": assigned_dates.dt.strftime("%Y-%m-%d"),
            "score": np.round(rng.uniform(0.01, 1.00, size=client_offers_count), 2),
            "offer_status": offer_statuses,
            "sent_at": sent_at_series,
        }
    )


def generate_mailing_events(rng: np.random.Generator, client_offers_count: int, mailing_events_count: int) -> pd.DataFrame:
    event_ids = np.arange(1, mailing_events_count + 1)
    event_times = _random_dates(rng, "2023-01-01", "2026-05-25", mailing_events_count)
    delivery_statuses = rng.choice(DELIVERY_STATUSES, size=mailing_events_count, p=[0.78, 0.14, 0.08])
    error_codes = np.full(mailing_events_count, "", dtype=object)
    failed_mask = delivery_statuses == "failed"
    error_codes[failed_mask] = rng.choice(ERROR_CODES, size=failed_mask.sum())

    return pd.DataFrame(
        {
            "event_id": event_ids,
            "client_offer_id": rng.integers(1, client_offers_count + 1, size=mailing_events_count),
            "event_type": rng.choice(EVENT_TYPES, size=mailing_events_count),
            "event_time": event_times.dt.strftime("%Y-%m-%d %H:%M:%S"),
            "delivery_status": delivery_statuses,
            "error_code": error_codes,
        }
    )


def upload_raw_data_to_s3(config: AppConfig | None = None) -> dict[str, str]:
    import boto3

    config = config or AppConfig.from_env()
    s3_client = boto3.client("s3", region_name=config.aws_region)
    uploaded: dict[str, str] = {}

    for filename in RAW_FILES:
        local_path = DATA_DIR / filename
        if not local_path.exists():
            raise FileNotFoundError(f"Не знайдено файл для завантаження: {local_path}")

        s3_key = build_s3_key(config.raw_prefix, filename)
        s3_client.upload_file(str(local_path), config.s3_bucket, s3_key)
        uploaded[filename] = f"s3://{config.s3_bucket}/{s3_key}"

    return uploaded


def generate_all_data(scale: str = "MEDIUM", seed: int = 42, auto_upload_to_s3: bool = True) -> dict[str, Any]:
    start_time = time.perf_counter()

    random.seed(seed)
    np.random.seed(seed)
    Faker.seed(seed)
    fake = Faker("uk_UA")
    rng = np.random.default_rng(seed)

    counts = _get_counts(scale)
    _ensure_data_directory()

    clients_df = generate_clients(fake, rng, counts["clients"])
    offers_df = generate_offers(fake, rng, counts["offers"])
    client_offers_df = generate_client_offers(
        rng,
        clients_count=counts["clients"],
        offers_count=counts["offers"],
        client_offers_count=counts["client_offers"],
    )
    mailing_events_df = generate_mailing_events(
        rng,
        client_offers_count=counts["client_offers"],
        mailing_events_count=counts["mailing_events"],
    )

    clients_df.to_csv(DATA_DIR / "clients.csv", index=False)
    offers_df.to_csv(DATA_DIR / "offers.csv", index=False)
    client_offers_df.to_csv(DATA_DIR / "client_offers.csv", index=False)
    mailing_events_df.to_csv(DATA_DIR / "mailing_events.csv", index=False)

    uploaded: dict[str, str] = {}
    if auto_upload_to_s3:
        uploaded = upload_raw_data_to_s3()

    elapsed_seconds = round(time.perf_counter() - start_time, 3)
    stats: dict[str, Any] = {
        "scale": scale.upper(),
        "rows_clients": len(clients_df),
        "rows_offers": len(offers_df),
        "rows_client_offers": len(client_offers_df),
        "rows_events": len(mailing_events_df),
        "generation_time_sec": elapsed_seconds,
        "uploaded": uploaded,
    }

    print("Генерацію завершено.")
    print(f"- Масштаб: {stats['scale']}")
    print(f"- clients.csv: {stats['rows_clients']}")
    print(f"- offers.csv: {stats['rows_offers']}")
    print(f"- client_offers.csv: {stats['rows_client_offers']}")
    print(f"- mailing_events.csv: {stats['rows_events']}")
    print(f"- Час генерації: {stats['generation_time_sec']} с")
    if uploaded:
        print("- Дані завантажено в S3:")
        for file_name, file_uri in uploaded.items():
            print(f"  - {file_name}: {file_uri}")

    return stats


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Генерація тестових банківських даних та завантаження в S3.")
    parser.add_argument("--scale", default="MEDIUM", help="SMALL | MEDIUM | LARGE | XLARGE")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--no-upload", action="store_true", help="Не завантажувати дані в S3 після генерації")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    generate_all_data(scale=args.scale, seed=args.seed, auto_upload_to_s3=not args.no_upload)
