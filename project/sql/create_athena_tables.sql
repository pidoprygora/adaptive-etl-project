CREATE DATABASE IF NOT EXISTS adaptive_etl_bank;

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.clients (
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
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/clients/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.client_contacts (
    contact_id BIGINT,
    client_id BIGINT,
    phone STRING,
    email STRING,
    viber_id STRING,
    push_token STRING,
    preferred_channel STRING,
    is_verified BOOLEAN,
    updated_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/client_contacts/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.client_segments (
    segment_id BIGINT,
    client_id BIGINT,
    segment_name STRING,
    segment_score DOUBLE,
    income_group STRING,
    risk_level STRING,
    updated_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/client_segments/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.products (
    product_id BIGINT,
    product_name STRING,
    product_type STRING,
    product_category STRING,
    is_active BOOLEAN,
    created_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/products/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.client_products (
    client_product_id BIGINT,
    client_id BIGINT,
    product_id BIGINT,
    open_date DATE,
    close_date DATE,
    status STRING,
    balance DOUBLE,
    credit_limit DOUBLE,
    last_activity_date DATE
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/client_products/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.transactions (
    transaction_id BIGINT,
    client_product_id BIGINT,
    transaction_date TIMESTAMP,
    transaction_type STRING,
    merchant_category STRING,
    amount DOUBLE,
    currency STRING,
    city STRING,
    status STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/transactions/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.campaigns (
    campaign_id BIGINT,
    campaign_name STRING,
    campaign_type STRING,
    start_date DATE,
    end_date DATE,
    status STRING,
    created_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/campaigns/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.campaign_offers (
    campaign_offer_id BIGINT,
    campaign_id BIGINT,
    offer_id BIGINT,
    priority INT,
    created_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/campaign_offers/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.mailing_audience (
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
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/mailing_audience/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.offers (
    offer_id BIGINT,
    product_id BIGINT,
    offer_name STRING,
    offer_description STRING,
    interest_rate DOUBLE,
    limit_amount DOUBLE,
    min_income DOUBLE,
    is_active BOOLEAN
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/offers/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.client_offers (
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
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/client_offers/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.mailing_events (
    event_id BIGINT,
    client_offer_id BIGINT,
    event_type STRING,
    event_time TIMESTAMP,
    delivery_status_id BIGINT,
    error_code STRING,
    error_message STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/mailing_events/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.delivery_statuses (
    delivery_status_id BIGINT,
    status_name STRING,
    status_description STRING,
    is_success BOOLEAN
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/delivery_statuses/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.app_clickstream (
    app_event_id BIGINT,
    client_id BIGINT,
    event_time TIMESTAMP,
    screen_name STRING,
    action_name STRING,
    product_id BIGINT,
    session_id STRING,
    device_type STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/app_clickstream/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.etl_execution_metrics (
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
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/raw/bank_data/etl_execution_metrics/';
