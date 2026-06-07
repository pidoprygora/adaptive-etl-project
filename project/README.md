# Synthetic Bank Data Generator

Проєкт для генерації синтетичних банківських даних у контексті дипломної роботи:
**"Розробка системи адаптивної паралелізації ETL-процесів на основі аналізу характеристик навантаження"**.

## Можливості

- Генерує 15 пов'язаних таблиць банківського домену.
- Зберігає кожну таблицю локально у форматах Parquet та CSV.
- Завантажує Parquet-файли в Amazon S3 для подальшого використання в Athena.
- Генерує DDL-скрипт `sql/create_athena_tables.sql` для створення зовнішніх таблиць Athena.

## Структура

```text
project/
├── data/
│   ├── parquet/
│   └── csv/
├── sql/
│   └── create_athena_tables.sql
├── src/
│   ├── config.py
│   ├── generate_bank_data.py
│   ├── upload_to_s3.py
│   └── generate_athena_ddl.py
├── requirements.txt
└── README.md
```

## Передумови

- Python 3.12+
- Налаштовані AWS credentials (через `aws configure`, AWS profile або змінні середовища)
- Доступ на запис до бакета `s3://adaptive-etl-project-032896316649-eu-north-1-an`

> AWS ключі не зберігаються у коді. Використовується стандартний механізм авторизації `boto3`.

## Встановлення

```bash
cd project
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Запуск

```bash
python src/generate_bank_data.py
```

Після запуску:

1. Дані зберігаються локально:
   - `data/parquet/<table_name>/part-000.parquet`
   - `data/csv/<table_name>.csv`
2. Parquet-файли завантажуються в S3:
   - `s3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/<table_name>/`
3. Генерується DDL:
   - `sql/create_athena_tables.sql`

## Налаштування розміру датасету

У файлі `src/config.py` змініть:

```python
DATASET_SIZE = "medium"  # small | medium | large
```

## Приклади перевірки в Athena

### 1) Перевірка кількості клієнтів

```sql
SELECT COUNT(*) FROM adaptive_etl_bank.clients;
```

### 2) Формування бази для розсилки

```sql
SELECT
    c.client_id,
    c.person_code,
    c.full_name,
    cc.phone,
    cc.email,
    cs.segment_name,
    o.offer_name,
    co.channel,
    co.score
FROM adaptive_etl_bank.clients c
JOIN adaptive_etl_bank.client_contacts cc
    ON c.client_id = cc.client_id
JOIN adaptive_etl_bank.client_segments cs
    ON c.client_id = cs.client_id
JOIN adaptive_etl_bank.client_offers co
    ON c.client_id = co.client_id
JOIN adaptive_etl_bank.offers o
    ON co.offer_id = o.offer_id
WHERE c.is_active = true
  AND co.offer_status IN ('prepared', 'sent', 'delivered', 'opened')
  AND o.is_active = true;
```

### 3) Підрахунок оферів по кампаніях

```sql
SELECT
    ca.campaign_name,
    COUNT(*) AS offers_count
FROM adaptive_etl_bank.client_offers co
JOIN adaptive_etl_bank.campaigns ca
    ON co.campaign_id = ca.campaign_id
GROUP BY ca.campaign_name
ORDER BY offers_count DESC;
```

### 4) Активність клієнтів у застосунку

```sql
SELECT
    c.client_id,
    c.full_name,
    COUNT(ac.app_event_id) AS app_events_count
FROM adaptive_etl_bank.clients c
JOIN adaptive_etl_bank.app_clickstream ac
    ON c.client_id = ac.client_id
GROUP BY c.client_id, c.full_name
ORDER BY app_events_count DESC
LIMIT 20;
```

### 5) Транзакційна активність клієнтів

```sql
SELECT
    c.client_id,
    c.full_name,
    SUM(t.amount) AS total_amount,
    COUNT(t.transaction_id) AS transactions_count
FROM adaptive_etl_bank.clients c
JOIN adaptive_etl_bank.client_products cp
    ON c.client_id = cp.client_id
JOIN adaptive_etl_bank.transactions t
    ON cp.client_product_id = t.client_product_id
WHERE t.status = 'successful'
GROUP BY c.client_id, c.full_name
ORDER BY total_amount DESC
LIMIT 20;
```

### 6) Кандидати на формування розсилки

```sql
SELECT
    ma.campaign_id,
    ma.recommended_channel,
    COUNT(*) AS candidates_count
FROM adaptive_etl_bank.mailing_audience ma
WHERE ma.is_eligible = true
GROUP BY ma.campaign_id, ma.recommended_channel
ORDER BY candidates_count DESC;
```

## ER-схема бази даних

Візуальна схема всіх таблиць доступна у файлі:

- `sql/bank_data_er_schema.md`

Файл містить Mermaid ER-діаграму, яку можна відкрити в Markdown preview у Cursor.

## Корисні команди

Окреме завантаження в S3:

```python
from upload_to_s3 import upload_all_tables_to_s3

upload_all_tables_to_s3(["clients", "transactions"])
```

Окрема генерація DDL:

```bash
python src/generate_athena_ddl.py
```

## Troubleshooting

### Помилка `NoSuchBucket` під час upload в S3

Якщо бачите `NoSuchBucket`, бакет із `src/config.py` ще не створений у вашому AWS-акаунті.

Створіть бакет (для регіону `eu-north-1`):

```bash
aws s3api create-bucket \
  --bucket adaptive-etl-project-032896316649-eu-north-1-an \
  --region eu-north-1 \
  --create-bucket-configuration LocationConstraint=eu-north-1
```

Потім перевірте доступ:

```bash
aws s3api head-bucket --bucket adaptive-etl-project-032896316649-eu-north-1-an
```

І повторіть запуск:

```bash
python src/generate_bank_data.py
```

### Попередження про Python 3.9

У проєкті заявлено Python 3.12+, а `boto3` вже завершує підтримку Python 3.9.
Рекомендовано створити venv на Python 3.12+ і перевстановити залежності.

