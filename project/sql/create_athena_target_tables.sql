-- ==================================================================================================
-- create_athena_target_tables.sql
-- Створення target (processed) таблиць для ETL-запитів q01-q20.
-- ==================================================================================================

CREATE DATABASE IF NOT EXISTS adaptive_etl_bank;

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.credit_campaign_target_audience (
    client_id BIGINT,
    person_code STRING,
    full_name STRING,
    phone STRING,
    email STRING,
    segment_name STRING,
    offer_id BIGINT,
    offer_name STRING,
    channel STRING,
    credit_score INTEGER,
    priority INTEGER
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/credit_campaign_target_audience/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.deposit_campaign_target_audience (
    client_id BIGINT,
    full_name STRING,
    segment_name STRING,
    avg_balance DOUBLE,
    salary_transactions_count BIGINT,
    deposit_score INTEGER,
    offer_id BIGINT,
    channel STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/deposit_campaign_target_audience/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.insurance_cross_sell_audience (
    client_id BIGINT,
    full_name STRING,
    offer_id BIGINT,
    offer_name STRING,
    insurance_screen_views BIGINT,
    lifestyle_spending_amount DOUBLE,
    insurance_score INTEGER
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/insurance_cross_sell_audience/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.premium_upgrade_audience (
    client_id BIGINT,
    segment_name STRING,
    total_turnover_180d DOUBLE,
    active_products_count BIGINT,
    avg_balance DOUBLE,
    app_events_90d BIGINT,
    premium_score INTEGER
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/premium_upgrade_audience/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.retention_inactive_clients (
    client_id BIGINT,
    full_name STRING,
    last_transaction_date DATE,
    last_app_event_date DATE,
    last_activity_date DATE,
    last_active_product_name STRING,
    last_active_product_type STRING,
    offer_id BIGINT,
    offer_name STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/retention_inactive_clients/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.client_profile_scoring (
    client_id BIGINT,
    person_code STRING,
    full_name STRING,
    segment_name STRING,
    preferred_channel STRING,
    active_products_count BIGINT,
    total_amount_90d DOUBLE,
    app_events_30d BIGINT,
    accepted_offers_count BIGINT,
    rejected_offers_count BIGINT,
    final_score INTEGER
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/client_profile_scoring/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.client_best_channel (
    client_id BIGINT,
    best_channel STRING,
    channel_score DOUBLE
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/client_best_channel/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.campaign_performance_dashboard (
    campaign_id BIGINT,
    campaign_name STRING,
    campaign_status STRING,
    total_client_offers BIGINT,
    sent_count BIGINT,
    delivered_count BIGINT,
    opened_count BIGINT,
    clicked_count BIGINT,
    accepted_count BIGINT,
    failed_count BIGINT,
    delivery_rate DOUBLE,
    open_rate DOUBLE,
    click_rate DOUBLE,
    conversion_rate DOUBLE
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/campaign_performance_dashboard/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.product_conversion_analysis (
    product_type STRING,
    total_offers BIGINT,
    opened_offers BIGINT,
    accepted_offers BIGINT,
    rejected_offers BIGINT,
    conversion_rate DOUBLE
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/product_conversion_analysis/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.delivery_failure_analysis (
    channel STRING,
    error_code STRING,
    total_events BIGINT,
    failed_events BIGINT,
    failure_rate DOUBLE
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/delivery_failure_analysis/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.app_behavior_offer_recommendation (
    client_id BIGINT,
    top_screen_name STRING,
    top_screen_views BIGINT,
    recommended_product_type STRING,
    offer_id BIGINT,
    offer_name STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/app_behavior_offer_recommendation/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.transaction_behavior_segmentation (
    client_id BIGINT,
    total_amount DOUBLE,
    avg_amount DOUBLE,
    transactions_count BIGINT,
    distinct_merchant_categories BIGINT,
    activity_group STRING,
    segment_name STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/transaction_behavior_segmentation/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.high_value_clients_top1000 (
    client_id BIGINT,
    full_name STRING,
    total_turnover_180d DOUBLE,
    avg_balance DOUBLE,
    active_products_count BIGINT,
    accepted_offers_count BIGINT,
    app_events_90d BIGINT,
    value_score DOUBLE
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/high_value_clients_top1000/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.duplicate_client_offers (
    client_id BIGINT,
    person_code STRING,
    full_name STRING,
    offer_id BIGINT,
    campaign_id BIGINT,
    duplicate_count BIGINT,
    first_assigned_date DATE,
    last_assigned_date DATE
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/duplicate_client_offers/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.expired_offer_cleanup_candidate (
    client_offer_id BIGINT,
    client_id BIGINT,
    offer_id BIGINT,
    campaign_id BIGINT,
    valid_until DATE,
    new_offer_status STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/expired_offer_cleanup_candidate/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.mailing_base (
    client_id BIGINT,
    person_code STRING,
    full_name STRING,
    campaign_id BIGINT,
    offer_id BIGINT,
    offer_name STRING,
    score INTEGER,
    channel STRING,
    priority INTEGER,
    planned_send_date DATE,
    mailing_status STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/mailing_base/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.mailing_schedule_optimization (
    client_id BIGINT,
    campaign_id BIGINT,
    offer_id BIGINT,
    channel STRING,
    priority INTEGER,
    planned_send_date DATE,
    schedule_hour STRING,
    schedule_batch STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/mailing_schedule_optimization/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.etl_metrics_aggregation (
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
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/etl_metrics_aggregation/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.adaptive_parallelism_recommendation (
    dataset_size STRING,
    task_integral_load DOUBLE,
    dag_avg_load DOUBLE,
    avg_total_time_sec DOUBLE,
    avg_parallel_tasks DOUBLE,
    avg_cpu_utilization DOUBLE,
    avg_ram_utilization DOUBLE,
    load_balance_coeff DOUBLE,
    recommended_parallel_tasks INTEGER
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/adaptive_parallelism_recommendation/';

CREATE EXTERNAL TABLE IF NOT EXISTS adaptive_etl_bank.campaign_readiness_check (
    campaign_id BIGINT,
    campaign_name STRING,
    offers_count BIGINT,
    mailing_clients_count BIGINT,
    valid_contacts_count BIGINT,
    duplicate_count BIGINT,
    expired_offer_count BIGINT,
    readiness_status STRING
)
STORED AS PARQUET
LOCATION 's3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/campaign_readiness_check/';
