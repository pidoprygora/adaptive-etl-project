-- IMPORTANT:
-- Athena allows only one SQL statement per execution.
-- Run this file line-by-line (one statement at a time).

CREATE DATABASE IF NOT EXISTS adaptive_etl_bank;

-- 1) Bootstrap CREATE statements (tables must exist before ALTER)
CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.clients (
    bootstrap_col STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/clients/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.client_contacts (
    bootstrap_col STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/client_contacts/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.client_segments (
    bootstrap_col STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/client_segments/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.products (
    bootstrap_col STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/products/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.client_products (
    bootstrap_col STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/client_products/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.transactions (
    bootstrap_col STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/transactions/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.campaigns (
    bootstrap_col STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/campaigns/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.campaign_offers (
    bootstrap_col STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/campaign_offers/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.mailing_audience (
    bootstrap_col STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/mailing_audience/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.offers (
    bootstrap_col STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/offers/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.client_offers (
    bootstrap_col STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/client_offers/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.mailing_events (
    bootstrap_col STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/mailing_events/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.delivery_statuses (
    bootstrap_col STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/delivery_statuses/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.app_clickstream (
    bootstrap_col STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/app_clickstream/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.etl_execution_metrics (
    bootstrap_col STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/etl_execution_metrics/';

-- 2) Full schema via ALTER TABLE REPLACE COLUMNS
ALTER TABLE adaptive_etl_bank.clients REPLACE COLUMNS (
    client_id BIGINT,
    person_code STRING,
    full_name STRING,
    birth_date DATE,
    gender STRING,
    city STRING,
    region STRING,
    registration_date DATE,
    is_active BOOLEAN,
    created_at TIMESTAMP
);

ALTER TABLE adaptive_etl_bank.client_contacts REPLACE COLUMNS (
    contact_id BIGINT,
    client_id BIGINT,
    phone STRING,
    email STRING,
    viber_id STRING,
    push_token STRING,
    preferred_channel STRING,
    is_verified BOOLEAN,
    updated_at TIMESTAMP
);

ALTER TABLE adaptive_etl_bank.client_segments REPLACE COLUMNS (
    segment_id BIGINT,
    client_id BIGINT,
    segment_name STRING,
    segment_score DOUBLE,
    income_group STRING,
    risk_level STRING,
    updated_at TIMESTAMP
);

ALTER TABLE adaptive_etl_bank.products REPLACE COLUMNS (
    product_id BIGINT,
    product_name STRING,
    product_type STRING,
    product_category STRING,
    is_active BOOLEAN,
    created_at TIMESTAMP
);

ALTER TABLE adaptive_etl_bank.client_products REPLACE COLUMNS (
    client_product_id BIGINT,
    client_id BIGINT,
    product_id BIGINT,
    open_date DATE,
    close_date DATE,
    status STRING,
    balance DOUBLE,
    credit_limit DOUBLE,
    last_activity_date DATE
);

ALTER TABLE adaptive_etl_bank.transactions REPLACE COLUMNS (
    transaction_id BIGINT,
    client_product_id BIGINT,
    transaction_date TIMESTAMP,
    transaction_type STRING,
    merchant_category STRING,
    amount DOUBLE,
    currency STRING,
    city STRING,
    status STRING
);

ALTER TABLE adaptive_etl_bank.campaigns REPLACE COLUMNS (
    campaign_id BIGINT,
    campaign_name STRING,
    campaign_type STRING,
    start_date DATE,
    end_date DATE,
    status STRING,
    created_at TIMESTAMP
);

ALTER TABLE adaptive_etl_bank.campaign_offers REPLACE COLUMNS (
    campaign_offer_id BIGINT,
    campaign_id BIGINT,
    offer_id BIGINT,
    priority INT,
    created_at TIMESTAMP
);

ALTER TABLE adaptive_etl_bank.mailing_audience REPLACE COLUMNS (
    audience_id BIGINT,
    client_id BIGINT,
    campaign_id BIGINT,
    offer_id BIGINT,
    segment_name STRING,
    propensity_score DOUBLE,
    recommended_channel STRING,
    planned_send_date DATE,
    is_eligible BOOLEAN,
    exclusion_reason STRING,
    created_at TIMESTAMP
);

ALTER TABLE adaptive_etl_bank.offers REPLACE COLUMNS (
    offer_id BIGINT,
    product_id BIGINT,
    offer_name STRING,
    offer_description STRING,
    interest_rate DOUBLE,
    limit_amount DOUBLE,
    min_income DOUBLE,
    is_active BOOLEAN
);

ALTER TABLE adaptive_etl_bank.client_offers REPLACE COLUMNS (
    client_offer_id BIGINT,
    client_id BIGINT,
    offer_id BIGINT,
    campaign_id BIGINT,
    assigned_date DATE,
    score DOUBLE,
    offer_status STRING,
    channel STRING,
    sent_at TIMESTAMP,
    valid_until DATE
);

ALTER TABLE adaptive_etl_bank.mailing_events REPLACE COLUMNS (
    event_id BIGINT,
    client_offer_id BIGINT,
    event_type STRING,
    event_time TIMESTAMP,
    delivery_status_id BIGINT,
    error_code STRING,
    error_message STRING
);

ALTER TABLE adaptive_etl_bank.delivery_statuses REPLACE COLUMNS (
    delivery_status_id BIGINT,
    status_name STRING,
    status_description STRING,
    is_success BOOLEAN
);

ALTER TABLE adaptive_etl_bank.app_clickstream REPLACE COLUMNS (
    app_event_id BIGINT,
    client_id BIGINT,
    event_time TIMESTAMP,
    screen_name STRING,
    action_name STRING,
    product_id BIGINT,
    session_id STRING,
    device_type STRING
);

ALTER TABLE adaptive_etl_bank.etl_execution_metrics REPLACE COLUMNS (
    metric_id BIGINT,
    task_id STRING,
    dag_run_id STRING,
    execution_date TIMESTAMP,
    dataset_size STRING,
    rows_clients BIGINT,
    rows_transactions BIGINT,
    rows_clickstream BIGINT,
    rows_offers BIGINT,
    rows_client_offers BIGINT,
    extract_time_sec DOUBLE,
    transform_time_sec DOUBLE,
    load_time_sec DOUBLE,
    total_execution_time_sec DOUBLE,
    parallel_tasks_count INT,
    task_load DOUBLE,
    avg_task_load DOUBLE,
    cpu_utilization DOUBLE,
    ram_utilization DOUBLE,
    planned_cpu_utilization DOUBLE,
    planned_ram_utilization DOUBLE,
    measured_cpu_utilization DOUBLE,
    measured_ram_utilization DOUBLE,
    resource_signal_source STRING,
    speedup DOUBLE,
    efficiency DOUBLE,
    amdahl_speedup DOUBLE,
    critical_path_time_sec DOUBLE,
    etl_time_sec DOUBLE,
    load_balance_coeff DOUBLE,
    predicted_time_old_sec DOUBLE,
    predicted_time_new_sec DOUBLE,
    spark_app_name STRING,
    spark_shuffle_partitions INT,
    status STRING
);

ALTER TABLE adaptive_etl_bank.etl_metrics_aggregation REPLACE COLUMNS (
    dataset_size STRING,
    avg_extract_time_sec DOUBLE,
    min_extract_time_sec DOUBLE,
    max_extract_time_sec DOUBLE,
    avg_transform_time_sec DOUBLE,
    min_transform_time_sec DOUBLE,
    max_transform_time_sec DOUBLE,
    avg_load_time_sec DOUBLE,
    min_load_time_sec DOUBLE,
    max_load_time_sec DOUBLE,
    avg_total_time_sec DOUBLE,
    min_total_time_sec DOUBLE,
    max_total_time_sec DOUBLE,
    avg_parallel_tasks DOUBLE,
    avg_cpu_utilization DOUBLE,
    avg_ram_utilization DOUBLE,
    avg_speedup DOUBLE,
    avg_efficiency DOUBLE,
    avg_amdahl_speedup DOUBLE,
    avg_load_balance_coeff DOUBLE,
    avg_task_load DOUBLE,
    avg_critical_path_time_sec DOUBLE,
    avg_etl_time_sec DOUBLE,
    avg_predicted_time_new_sec DOUBLE
);

ALTER TABLE adaptive_etl_bank.adaptive_parallelism_recommendation REPLACE COLUMNS (
    dataset_size STRING,
    task_integral_load DOUBLE,
    dag_avg_load DOUBLE,
    avg_total_time_sec DOUBLE,
    avg_parallel_tasks DOUBLE,
    avg_cpu_utilization DOUBLE,
    avg_ram_utilization DOUBLE,
    load_balance_coeff DOUBLE,
    recommended_parallel_tasks INT
);
