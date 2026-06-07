"""Генерація синтетичних банківських даних для дипломного проєкту."""

from __future__ import annotations

import logging
import random
import string
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Callable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker

from config import (
    CAMPAIGNS_COUNT,
    MAILING_AUDIENCE_PER_CLIENT,
    CLIENT_PRODUCTS_PER_CLIENT,
    CSV_DIR,
    DATASET_SIZE,
    DATASET_SIZES,
    ETL_METRICS_COUNT,
    MAILING_EVENTS_PER_CLIENT_OFFER,
    OFFERS_COUNT,
    PARQUET_DIR,
    PRODUCTS_COUNT,
)

faker = Faker("uk_UA")
random.seed(42)
Faker.seed(42)

# У локалі uk_UA не в усіх версіях Faker є administrative_unit(),
# тому використовуємо явний список регіонів для стабільної генерації.
UK_REGIONS = [
    "Kyiv",
    "Vinnytsia",
    "Volyn",
    "Dnipropetrovsk",
    "Donetsk",
    "Zhytomyr",
    "Zakarpattia",
    "Zaporizhzhia",
    "Ivano-Frankivsk",
    "Kyivska",
    "Kirovohrad",
    "Luhansk",
    "Lviv",
    "Mykolaiv",
    "Odesa",
    "Poltava",
    "Rivne",
    "Sumy",
    "Ternopil",
    "Kharkiv",
    "Kherson",
    "Khmelnytskyi",
    "Cherkasy",
    "Chernivtsi",
    "Chernihiv",
]


def configure_logging() -> None:
    """Налаштовує базове логування для зручного моніторингу генерації."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def ensure_directories() -> None:
    """Створює директорії для результатів, якщо їх ще немає."""
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    CSV_DIR.mkdir(parents=True, exist_ok=True)


def to_timestamp(dt: datetime) -> datetime:
    """Повертає timezone-naive UTC datetime для Athena."""
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def random_date(start: date, end: date) -> date:
    """Генерує випадкову дату в заданому діапазоні."""
    delta_days = (end - start).days
    return start + timedelta(days=random.randint(0, max(delta_days, 0)))


def random_timestamp(start: datetime, end: datetime) -> datetime:
    """Генерує випадковий timestamp в заданому діапазоні."""
    delta_seconds = int((end - start).total_seconds())
    return to_timestamp(start + timedelta(seconds=random.randint(0, max(delta_seconds, 0))))


def save_table(table_name: str, df: pd.DataFrame) -> None:
    """Зберігає таблицю локально в Parquet і CSV."""
    from glue_parquet_types import coerce_dataframe_to_glue_schema

    parquet_dir = PARQUET_DIR / table_name
    parquet_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = parquet_dir / "part-000.parquet"
    csv_path = CSV_DIR / f"{table_name}.csv"

    df = coerce_dataframe_to_glue_schema(table_name, df)

    # Spark 3.5 reads TIMESTAMP_MICROS; avoid TIMESTAMP(NANOS) from datetime64[ns].
    arrays: list[pa.Array] = []
    fields: list[pa.Field] = []
    for col in df.columns:
        series = df[col]
        if pd.api.types.is_datetime64_any_dtype(series):
            series = series.astype("datetime64[us]")
            arrays.append(pa.array(series, type=pa.timestamp("us")))
            fields.append(pa.field(col, pa.timestamp("us")))
        else:
            arrays.append(pa.array(series))
            fields.append(pa.field(col, arrays[-1].type))
    pq.write_table(pa.Table.from_arrays(arrays, schema=pa.schema(fields)), parquet_path, compression="snappy")
    df.to_csv(csv_path, index=False)


def build_clients(clients_count: int) -> pd.DataFrame:
    """Формує таблицю клієнтів."""
    today = date.today()
    records: list[dict] = []
    for client_id in range(1, clients_count + 1):
        birth = random_date(today - timedelta(days=80 * 365), today - timedelta(days=18 * 365))
        registration = random_date(date(2010, 1, 1), today)
        created_at = random_timestamp(datetime(2018, 1, 1), datetime.utcnow())
        records.append(
            {
                "client_id": client_id,
                "person_code": f"{random.randint(1000000000, 9999999999)}",
                "full_name": faker.name(),
                "birth_date": birth,
                "gender": random.choice(["male", "female"]),
                "city": faker.city(),
                "region": random.choice(UK_REGIONS),
                "registration_date": registration,
                "is_active": random.random() < 0.85,
                "created_at": created_at,
            }
        )
    return pd.DataFrame(records)


def build_client_contacts(clients_df: pd.DataFrame) -> pd.DataFrame:
    """Формує контактні дані клієнтів (1 запис на клієнта)."""
    channels = ["sms", "email", "push", "viber"]
    records: list[dict] = []
    for idx, client_id in enumerate(clients_df["client_id"], start=1):
        records.append(
            {
                "contact_id": idx,
                "client_id": int(client_id),
                "phone": faker.phone_number(),
                "email": faker.email(),
                "viber_id": f"viber_{uuid.uuid4().hex[:12]}",
                "push_token": uuid.uuid4().hex,
                "preferred_channel": random.choice(channels),
                "is_verified": random.random() < 0.92,
                "updated_at": random_timestamp(datetime(2021, 1, 1), datetime.utcnow()),
            }
        )
    return pd.DataFrame(records)


def build_client_segments(clients_df: pd.DataFrame) -> pd.DataFrame:
    """Формує сегментацію клієнтів (1 запис на клієнта)."""
    segment_names = ["mass", "premium", "vip", "salary", "pension", "student"]
    income_groups = ["low", "medium", "high"]
    risk_levels = ["low", "medium", "high"]
    records: list[dict] = []
    for idx, client_id in enumerate(clients_df["client_id"], start=1):
        records.append(
            {
                "segment_id": idx,
                "client_id": int(client_id),
                "segment_name": random.choice(segment_names),
                "segment_score": round(random.uniform(0.0, 1.0), 4),
                "income_group": random.choice(income_groups),
                "risk_level": random.choice(risk_levels),
                "updated_at": random_timestamp(datetime(2021, 1, 1), datetime.utcnow()),
            }
        )
    return pd.DataFrame(records)


def build_products() -> pd.DataFrame:
    """Формує довідник банківських продуктів."""
    product_types = [
        "credit_card",
        "cash_loan",
        "deposit",
        "mortgage",
        "insurance",
        "savings_account",
        "debit_card",
    ]
    categories = ["daily", "lending", "investment", "protection"]
    records: list[dict] = []
    for product_id in range(1, PRODUCTS_COUNT + 1):
        product_type = random.choice(product_types)
        records.append(
            {
                "product_id": product_id,
                "product_name": f"{product_type}_{product_id}",
                "product_type": product_type,
                "product_category": random.choice(categories),
                "is_active": random.random() < 0.9,
                "created_at": random_timestamp(datetime(2018, 1, 1), datetime.utcnow()),
            }
        )
    return pd.DataFrame(records)


def build_client_products(clients_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    """Формує зв'язку клієнтів із продуктами."""
    statuses = ["active", "closed", "blocked"]
    product_ids = products_df["product_id"].tolist()
    min_p, max_p = CLIENT_PRODUCTS_PER_CLIENT
    records: list[dict] = []
    current_id = 1
    for client_id in clients_df["client_id"]:
        product_count = random.randint(min_p, max_p)
        for product_id in random.sample(product_ids, k=product_count):
            open_date = random_date(date(2015, 1, 1), date.today())
            status = random.choice(statuses)
            close_date = None
            if status == "closed":
                close_date = random_date(open_date, date.today())
            last_activity_date = random_date(open_date, date.today())
            records.append(
                {
                    "client_product_id": current_id,
                    "client_id": int(client_id),
                    "product_id": int(product_id),
                    "open_date": open_date,
                    "close_date": close_date,
                    "status": status,
                    "balance": round(random.uniform(0, 350_000), 2),
                    "credit_limit": round(random.uniform(5_000, 500_000), 2),
                    "last_activity_date": last_activity_date,
                }
            )
            current_id += 1
    return pd.DataFrame(records)


def build_transactions(client_products_df: pd.DataFrame, transactions_count: int) -> pd.DataFrame:
    """Формує транзакції за клієнтськими продуктами."""
    transaction_types = ["purchase", "atm", "transfer", "salary", "cashback", "payment"]
    merchant_categories = [
        "groceries",
        "fuel",
        "travel",
        "electronics",
        "restaurants",
        "pharmacy",
        "utilities",
        "beauty",
        "education",
    ]
    statuses = ["successful", "failed", "reversed"]
    currencies = ["UAH", "USD", "EUR"]
    client_product_ids = client_products_df["client_product_id"].tolist()

    records: list[dict] = []
    for transaction_id in range(1, transactions_count + 1):
        transaction_type = random.choice(transaction_types)
        amount = round(random.uniform(5, 35_000), 2)
        if transaction_type == "salary":
            amount = round(random.uniform(8_000, 90_000), 2)
        elif transaction_type == "cashback":
            amount = round(random.uniform(1, 2_000), 2)
        records.append(
            {
                "transaction_id": transaction_id,
                "client_product_id": int(random.choice(client_product_ids)),
                "transaction_date": random_timestamp(datetime(2020, 1, 1), datetime.utcnow()),
                "transaction_type": transaction_type,
                "merchant_category": random.choice(merchant_categories),
                "amount": amount,
                "currency": random.choice(currencies),
                "city": faker.city(),
                "status": random.choices(statuses, weights=[0.9, 0.08, 0.02], k=1)[0],
            }
        )
    return pd.DataFrame(records)


def build_campaigns() -> pd.DataFrame:
    """Формує маркетингові кампанії."""
    campaign_types = [
        "credit_card_sale",
        "cash_loan_offer",
        "deposit_promo",
        "insurance_cross_sell",
        "retention_campaign",
        "premium_upgrade",
    ]
    statuses = ["planned", "active", "completed", "cancelled"]
    records: list[dict] = []
    for campaign_id in range(1, CAMPAIGNS_COUNT + 1):
        start_date = random_date(date(2023, 1, 1), date(2026, 12, 31))
        end_date = random_date(start_date, date(2027, 12, 31))
        records.append(
            {
                "campaign_id": campaign_id,
                "campaign_name": f"campaign_{campaign_id}",
                "campaign_type": random.choice(campaign_types),
                "start_date": start_date,
                "end_date": end_date,
                "status": random.choice(statuses),
                "created_at": random_timestamp(datetime(2022, 1, 1), datetime.utcnow()),
            }
        )
    return pd.DataFrame(records)


def build_offers(products_df: pd.DataFrame) -> pd.DataFrame:
    """Формує офери, пов'язані з продуктами."""
    product_ids = products_df["product_id"].tolist()
    records: list[dict] = []
    for offer_id in range(1, OFFERS_COUNT + 1):
        records.append(
            {
                "offer_id": offer_id,
                "product_id": int(random.choice(product_ids)),
                "offer_name": f"offer_{offer_id}",
                "offer_description": faker.sentence(nb_words=10),
                "interest_rate": round(random.uniform(0.01, 0.55), 4),
                "limit_amount": round(random.uniform(10_000, 1_500_000), 2),
                "min_income": round(random.uniform(4_000, 120_000), 2),
                "is_active": random.random() < 0.88,
            }
        )
    return pd.DataFrame(records)


def build_campaign_offers(campaigns_df: pd.DataFrame, offers_df: pd.DataFrame) -> pd.DataFrame:
    """Формує зв'язку кампаній і оферів."""
    offer_ids = offers_df["offer_id"].tolist()
    records: list[dict] = []
    current_id = 1
    for campaign_id in campaigns_df["campaign_id"]:
        linked_offers = random.sample(offer_ids, k=random.randint(3, min(12, len(offer_ids))))
        for priority, offer_id in enumerate(linked_offers, start=1):
            records.append(
                {
                    "campaign_offer_id": current_id,
                    "campaign_id": int(campaign_id),
                    "offer_id": int(offer_id),
                    "priority": priority,
                    "created_at": random_timestamp(datetime(2023, 1, 1), datetime.utcnow()),
                }
            )
            current_id += 1
    return pd.DataFrame(records)


def build_mailing_audience(
    clients_df: pd.DataFrame,
    client_segments_df: pd.DataFrame,
    campaign_offers_df: pd.DataFrame,
) -> pd.DataFrame:
    """Формує кандидатів для підготовки персональних оферів і розсилок."""
    channels = ["sms", "email", "push", "viber"]
    min_a, max_a = MAILING_AUDIENCE_PER_CLIENT
    campaign_offer_pairs = (
        campaign_offers_df[["campaign_id", "offer_id"]].drop_duplicates().to_dict("records")
    )
    segment_map = dict(
        zip(client_segments_df["client_id"].astype(int), client_segments_df["segment_name"])
    )
    exclusion_reasons = [
        "low_score",
        "do_not_contact",
        "invalid_channel",
        "already_in_campaign",
    ]

    records: list[dict] = []
    current_id = 1
    for client_id in clients_df["client_id"]:
        candidates_count = random.randint(min_a, max_a)
        for _ in range(candidates_count):
            selected_pair = random.choice(campaign_offer_pairs)
            propensity_score = round(random.uniform(0.0, 1.0), 4)
            is_eligible = propensity_score >= 0.25 and random.random() < 0.9
            records.append(
                {
                    "audience_id": current_id,
                    "client_id": int(client_id),
                    "campaign_id": int(selected_pair["campaign_id"]),
                    "offer_id": int(selected_pair["offer_id"]),
                    "segment_name": segment_map.get(int(client_id), "mass"),
                    "propensity_score": propensity_score,
                    "recommended_channel": random.choice(channels),
                    "planned_send_date": random_date(date(2023, 1, 1), date.today()),
                    "is_eligible": is_eligible,
                    "exclusion_reason": None if is_eligible else random.choice(exclusion_reasons),
                    "created_at": random_timestamp(datetime(2023, 1, 1), datetime.utcnow()),
                }
            )
            current_id += 1
    return pd.DataFrame(records)


def build_client_offers(mailing_audience_df: pd.DataFrame) -> pd.DataFrame:
    """Формує персональні офери для клієнтів."""
    offer_statuses = ["prepared", "sent", "delivered", "opened", "accepted", "rejected", "expired"]
    channels = ["sms", "email", "push", "viber"]
    eligible_audience = mailing_audience_df[mailing_audience_df["is_eligible"] == True]  # noqa: E712
    if eligible_audience.empty:
        return pd.DataFrame(
            columns=[
                "client_offer_id",
                "client_id",
                "offer_id",
                "campaign_id",
                "assigned_date",
                "score",
                "offer_status",
                "channel",
                "sent_at",
                "valid_until",
            ]
        )

    records: list[dict] = []
    current_id = 1
    for row in eligible_audience.itertuples(index=False):
        assigned_date = row.planned_send_date
        sent_at = random_timestamp(datetime.combine(assigned_date, datetime.min.time()), datetime.utcnow())
        valid_until = assigned_date + timedelta(days=random.randint(7, 120))
        records.append(
            {
                "client_offer_id": current_id,
                "client_id": int(row.client_id),
                "offer_id": int(row.offer_id),
                "campaign_id": int(row.campaign_id),
                "assigned_date": assigned_date,
                "score": round(float(row.propensity_score), 4),
                "offer_status": random.choice(offer_statuses),
                "channel": row.recommended_channel if random.random() < 0.8 else random.choice(channels),
                "sent_at": sent_at,
                "valid_until": valid_until,
            }
        )
        current_id += 1
    return pd.DataFrame(records)


def build_delivery_statuses() -> pd.DataFrame:
    """Формує довідник статусів доставки."""
    statuses = [
        (1, "success", "Message delivered successfully", True),
        (2, "failed", "Message delivery failed", False),
        (3, "pending", "Message is still pending", False),
        (4, "blocked", "Message blocked by recipient settings", False),
        (5, "invalid_contact", "Message rejected due to invalid contact", False),
    ]
    return pd.DataFrame(
        [
            {
                "delivery_status_id": status_id,
                "status_name": name,
                "status_description": description,
                "is_success": is_success,
            }
            for status_id, name, description, is_success in statuses
        ]
    )


def build_mailing_events(client_offers_df: pd.DataFrame, delivery_statuses_df: pd.DataFrame) -> pd.DataFrame:
    """Формує події взаємодії з розсилками."""
    event_types = ["send_attempt", "delivered", "opened", "clicked", "accepted", "rejected", "failed"]
    min_e, max_e = MAILING_EVENTS_PER_CLIENT_OFFER
    delivery_ids = delivery_statuses_df["delivery_status_id"].tolist()

    records: list[dict] = []
    current_id = 1
    for client_offer_id in client_offers_df["client_offer_id"]:
        events_count = random.randint(min_e, max_e)
        for _ in range(events_count):
            event_type = random.choice(event_types)
            has_error = event_type in {"failed", "rejected"} and random.random() < 0.8
            records.append(
                {
                    "event_id": current_id,
                    "client_offer_id": int(client_offer_id),
                    "event_type": event_type,
                    "event_time": random_timestamp(datetime(2023, 1, 1), datetime.utcnow()),
                    "delivery_status_id": int(random.choice(delivery_ids)),
                    "error_code": random.choice(["E101", "E202", "E303"]) if has_error else None,
                    "error_message": "Delivery processing error" if has_error else None,
                }
            )
            current_id += 1
    return pd.DataFrame(records)


def random_session_id() -> str:
    """Генерує короткий ідентифікатор сесії."""
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=20))
    return f"sess_{suffix}"


def build_app_clickstream(clients_df: pd.DataFrame, products_df: pd.DataFrame, events_count: int) -> pd.DataFrame:
    """Формує події клієнтів у мобільному застосунку."""
    screen_names = [
        "main_screen",
        "cards_screen",
        "loans_screen",
        "deposits_screen",
        "insurance_screen",
        "payments_screen",
        "offers_screen",
    ]
    action_names = ["view", "click", "open_offer", "apply", "calculator", "close"]
    device_types = ["ios", "android", "web"]
    client_ids = clients_df["client_id"].tolist()
    product_ids = products_df["product_id"].tolist()

    records: list[dict] = []
    for app_event_id in range(1, events_count + 1):
        records.append(
            {
                "app_event_id": app_event_id,
                "client_id": int(random.choice(client_ids)),
                "event_time": random_timestamp(datetime(2021, 1, 1), datetime.utcnow()),
                "screen_name": random.choice(screen_names),
                "action_name": random.choice(action_names),
                "product_id": int(random.choice(product_ids)),
                "session_id": random_session_id(),
                "device_type": random.choice(device_types),
            }
        )
    return pd.DataFrame(records)


def build_etl_execution_metrics(
    clients_count: int,
    transactions_count: int,
    clickstream_count: int,
    offers_count: int,
    client_offers_count: int,
) -> pd.DataFrame:
    """Формує метрики виконання ETL-пайплайна."""
    statuses = ["success", "failed", "warning"]
    records: list[dict] = []
    for metric_id in range(1, ETL_METRICS_COUNT + 1):
        extract_time = round(random.uniform(10, 120), 2)
        transform_time = round(random.uniform(20, 180), 2)
        load_time = round(random.uniform(15, 160), 2)
        records.append(
            {
                "metric_id": metric_id,
                "task_id": f"synthetic_task_{metric_id % 5}",
                "dag_run_id": f"bank_etl_run_{metric_id:05d}",
                "execution_date": random_timestamp(datetime(2024, 1, 1), datetime.utcnow()),
                "dataset_size": DATASET_SIZE,
                "rows_clients": clients_count,
                "rows_transactions": transactions_count,
                "rows_clickstream": clickstream_count,
                "rows_offers": offers_count,
                "rows_client_offers": client_offers_count,
                "extract_time_sec": extract_time,
                "transform_time_sec": transform_time,
                "load_time_sec": load_time,
                "total_execution_time_sec": round(extract_time + transform_time + load_time, 2),
                "parallel_tasks_count": random.randint(2, 24),
                "task_load": round(random.uniform(100, 1000), 2),
                "avg_task_load": round(random.uniform(100, 800), 2),
                "cpu_utilization": round(random.uniform(0.35, 0.92), 4),
                "ram_utilization": round(random.uniform(0.3, 0.9), 4),
                "planned_cpu_utilization": round(random.uniform(0.35, 0.92), 4),
                "planned_ram_utilization": round(random.uniform(0.3, 0.9), 4),
                "measured_cpu_utilization": round(random.uniform(0.3, 0.95), 4),
                "measured_ram_utilization": round(random.uniform(0.25, 0.92), 4),
                "resource_signal_source": random.choice(
                    ["cloudwatch", "spark_metrics", "synthetic"]
                ),
                "speedup": round(random.uniform(1.0, 8.0), 3),
                "efficiency": round(random.uniform(0.3, 1.0), 3),
                "amdahl_speedup": round(random.uniform(1.0, 6.0), 3),
                "critical_path_time_sec": round(random.uniform(40, 400), 2),
                "etl_time_sec": round(random.uniform(40, 400), 2),
                "load_balance_coeff": round(random.uniform(1.0, 2.0), 3),
                "predicted_time_old_sec": round(random.uniform(40, 350), 2),
                "predicted_time_new_sec": round(random.uniform(40, 350), 2),
                "spark_app_name": f"etl_q{metric_id % 21:02d}",
                "spark_shuffle_partitions": random.randint(4, 96),
                "status": random.choices(statuses, weights=[0.8, 0.1, 0.1], k=1)[0],
            }
        )
    return pd.DataFrame(records)


def generate_dataset() -> dict[str, pd.DataFrame]:
    """Генерує повний набір таблиць і повертає їх у вигляді словника."""
    if DATASET_SIZE not in DATASET_SIZES:
        allowed = ", ".join(DATASET_SIZES.keys())
        raise ValueError(f"Unknown DATASET_SIZE '{DATASET_SIZE}'. Allowed values: {allowed}")

    sizes = DATASET_SIZES[DATASET_SIZE]
    clients_count = sizes["clients"]
    transactions_count = sizes["transactions"]
    clickstream_count = sizes["app_clickstream"]

    generated: dict[str, pd.DataFrame] = {}

    def timed_build(table_name: str, builder: Callable[[], pd.DataFrame]) -> None:
        started = time.perf_counter()
        generated[table_name] = builder()
        elapsed = time.perf_counter() - started
        logging.info("Таблиця %s згенерована за %.2f сек", table_name, elapsed)

    timed_build("clients", lambda: build_clients(clients_count))
    timed_build("client_contacts", lambda: build_client_contacts(generated["clients"]))
    timed_build("client_segments", lambda: build_client_segments(generated["clients"]))
    timed_build("products", build_products)
    timed_build(
        "client_products",
        lambda: build_client_products(generated["clients"], generated["products"]),
    )
    timed_build(
        "transactions",
        lambda: build_transactions(generated["client_products"], transactions_count),
    )
    timed_build("campaigns", build_campaigns)
    timed_build("offers", lambda: build_offers(generated["products"]))
    timed_build(
        "campaign_offers",
        lambda: build_campaign_offers(generated["campaigns"], generated["offers"]),
    )
    timed_build(
        "mailing_audience",
        lambda: build_mailing_audience(
            generated["clients"], generated["client_segments"], generated["campaign_offers"]
        ),
    )
    timed_build(
        "client_offers",
        lambda: build_client_offers(generated["mailing_audience"]),
    )
    timed_build("delivery_statuses", build_delivery_statuses)
    timed_build(
        "mailing_events",
        lambda: build_mailing_events(generated["client_offers"], generated["delivery_statuses"]),
    )
    timed_build(
        "app_clickstream",
        lambda: build_app_clickstream(generated["clients"], generated["products"], clickstream_count),
    )
    timed_build(
        "etl_execution_metrics",
        lambda: build_etl_execution_metrics(
            clients_count=clients_count,
            transactions_count=transactions_count,
            clickstream_count=clickstream_count,
            offers_count=len(generated["offers"]),
            client_offers_count=len(generated["client_offers"]),
        ),
    )

    return generated


def save_dataset(dataset: dict[str, pd.DataFrame]) -> None:
    """Зберігає всі таблиці в локальне сховище."""
    for table_name, df in dataset.items():
        started = time.perf_counter()
        save_table(table_name, df)
        elapsed = time.perf_counter() - started
        logging.info("Таблиця %s збережена локально за %.2f сек", table_name, elapsed)


def print_generation_stats(dataset: dict[str, pd.DataFrame]) -> None:
    """Виводить підсумкову статистику кількості рядків."""
    print(f"Generated clients: {len(dataset['clients'])}")
    print(f"Generated client_contacts: {len(dataset['client_contacts'])}")
    print(f"Generated client_segments: {len(dataset['client_segments'])}")
    print(f"Generated products: {len(dataset['products'])}")
    print(f"Generated client_products: {len(dataset['client_products'])}")
    print(f"Generated transactions: {len(dataset['transactions'])}")
    print(f"Generated campaigns: {len(dataset['campaigns'])}")
    print(f"Generated campaign_offers: {len(dataset['campaign_offers'])}")
    print(f"Generated mailing_audience: {len(dataset['mailing_audience'])}")
    print(f"Generated offers: {len(dataset['offers'])}")
    print(f"Generated client_offers: {len(dataset['client_offers'])}")
    print(f"Generated mailing_events: {len(dataset['mailing_events'])}")
    print(f"Generated delivery_statuses: {len(dataset['delivery_statuses'])}")
    print(f"Generated app_clickstream: {len(dataset['app_clickstream'])}")
    print(f"Generated etl_execution_metrics: {len(dataset['etl_execution_metrics'])}")


def main() -> None:
    """Точка входу: генерація, завантаження в S3 та створення DDL."""
    configure_logging()
    ensure_directories()

    total_started = time.perf_counter()
    dataset = generate_dataset()
    save_dataset(dataset)
    print_generation_stats(dataset)

    # Використовуємо стандартну авторизацію AWS через профіль/змінні середовища.
    from generate_athena_ddl import generate_athena_ddl
    from upload_to_s3 import upload_all_tables_to_s3

    upload_all_tables_to_s3(list(dataset.keys()))
    print("S3 upload completed.")

    generate_athena_ddl()
    print("Athena DDL generated.")

    total_elapsed = time.perf_counter() - total_started
    logging.info("Повний процес завершено за %.2f сек", total_elapsed)


if __name__ == "__main__":
    main()

