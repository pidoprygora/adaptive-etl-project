from common import parse_runtime_options, run_sql


SQL_QUERY = """
INSERT INTO adaptive_etl_bank.duplicate_client_offers
WITH grouped_duplicates AS (
    SELECT
        co.client_id,
        co.offer_id,
        co.campaign_id,
        COUNT(1) AS duplicate_count,
        MIN(co.assigned_date) AS first_assigned_date,
        MAX(co.assigned_date) AS last_assigned_date
    FROM adaptive_etl_bank.client_offers co
    GROUP BY co.client_id, co.offer_id, co.campaign_id
    HAVING COUNT(1) > 1
),
client_info AS (
    SELECT c.client_id, c.person_code, c.full_name
    FROM adaptive_etl_bank.clients c
)
SELECT
    gd.client_id,
    ci.person_code,
    ci.full_name,
    gd.offer_id,
    gd.campaign_id,
    gd.duplicate_count,
    gd.first_assigned_date,
    gd.last_assigned_date
FROM grouped_duplicates gd
LEFT JOIN client_info ci
    ON gd.client_id = ci.client_id
"""


if __name__ == "__main__":
    run_sql(
        sql_text=SQL_QUERY,
        app_name="etl_q14_duplicate_client_offers",
        runtime_options=parse_runtime_options(),
    )
