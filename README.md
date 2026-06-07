# Adaptive Parallel ETL Diploma Project

Проєкт моделює банківський ETL-процес для маркетингових кампаній і готує основу для модуля адаптивної паралелізації на базі історичних метрик виконання.

## Технології

- Python 3.12+
- Faker (`uk_UA`)
- Pandas
- Apache Airflow
- AWS S3
- Boto3

## Структура

- `generate_bank_data.py` — генерація `clients.csv`, `offers.csv`, `client_offers.csv`, `mailing_events.csv` + завантаження в `s3://<bucket>/raw/`.
- `etl_pipeline.py` — S3-based ETL (`extract_data`, `transform_data`, `load_data`, `validate_data`) + запис `metrics.csv` локально й у `s3://<bucket>/metrics/`.
- `dags/bank_etl_dag.py` — Airflow DAG з етапами:
  `generate_fake_bank_data >> upload_raw_data_to_s3 >> extract_data >> transform_data >> load_results_to_s3 >> validate_results`.
- `config.py` — централізована конфігурація (`AWS_REGION`, `S3_BUCKET`, `RAW_PREFIX`, `PROCESSED_PREFIX`, `METRICS_PREFIX`).
- `data/` — локальні raw CSV після генерації.
- `output/` — локальні копії processed CSV після етапу load.

## Налаштування

Перед запуском задайте змінні оточення:

```bash
export AWS_REGION=eu-central-1
export S3_BUCKET=adaptive-etl-project
export RAW_PREFIX=raw
export PROCESSED_PREFIX=processed
export METRICS_PREFIX=metrics
```

## Масштаби генерації (для adaptive scheduler)

- `SMALL` — 10 000 клієнтів
- `MEDIUM` — 50 000 клієнтів
- `LARGE` — 100 000 клієнтів
- `XLARGE` — 500 000 клієнтів

`offers` фіксовано 500, а `client_offers` і `mailing_events` масштабуються пропорційно.

## Локальний запуск

1. Встановити залежності:

```bash
python3 -m pip install -r requirements.txt
```

2. Згенерувати дані та завантажити в S3:

```bash
python3 generate_bank_data.py --scale MEDIUM
```

3. Запустити ETL:

```bash
python3 etl_pipeline.py
```

## Очікувані результати в S3

- `s3://adaptive-etl-project/raw/`
  - `clients.csv`
  - `offers.csv`
  - `client_offers.csv`
  - `mailing_events.csv`
- `s3://adaptive-etl-project/processed/`
  - `mailing_base.csv`
  - `offers_by_channel.csv`
  - `offers_by_product.csv`
  - `clients_by_segment.csv`
  - `offer_conversion.csv`
  - `delivery_errors.csv`
- `s3://adaptive-etl-project/metrics/`
  - `metrics.csv`
