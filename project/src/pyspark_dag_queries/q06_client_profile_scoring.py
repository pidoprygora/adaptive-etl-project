from common import parse_runtime_options, run_sql


SQL_QUERY = """
INSERT INTO adaptive_etl_bank.client_profile_scoring
WITH active_clients AS (
    SELECT c.client_id, c.person_code, c.full_name
    FROM adaptive_etl_bank.clients c
    WHERE c.is_active = true
),
latest_segment AS (
    SELECT client_id, segment_name
    FROM (
        SELECT
            cs.client_id,
            cs.segment_name,
            ROW_NUMBER() OVER (PARTITION BY cs.client_id ORDER BY cs.updated_at DESC) AS rn
        FROM adaptive_etl_bank.client_segments cs
    ) ranked_segments
    WHERE rn = 1
),
latest_contact AS (
    SELECT client_id, preferred_channel
    FROM (
        SELECT
            cc.client_id,
            cc.preferred_channel,
            ROW_NUMBER() OVER (PARTITION BY cc.client_id ORDER BY cc.updated_at DESC) AS rn
        FROM adaptive_etl_bank.client_contacts cc
    ) ranked_contacts
    WHERE rn = 1
),
active_product_counts AS (
    SELECT
        cp.client_id,
        COUNT_IF(cp.status = 'active') AS active_products_count
    FROM adaptive_etl_bank.client_products cp
    GROUP BY cp.client_id
),
amount_90d AS (
    SELECT
        cp.client_id,
        COALESCE(SUM(t.amount), 0.0) AS total_amount_90d
    FROM adaptive_etl_bank.client_products cp
    LEFT JOIN adaptive_etl_bank.transactions t
        ON cp.client_product_id = t.client_product_id
       AND t.status = 'successful'
       AND DATE(t.transaction_date) >= DATE_ADD('day', -90, CURRENT_DATE)
    GROUP BY cp.client_id
),
app_30d AS (
    SELECT
        ac.client_id,
        COUNT(ac.app_event_id) AS app_events_30d
    FROM adaptive_etl_bank.app_clickstream ac
    WHERE DATE(ac.event_time) >= DATE_ADD('day', -30, CURRENT_DATE)
    GROUP BY ac.client_id
),
offer_feedback AS (
    SELECT
        co.client_id,
        COUNT_IF(co.offer_status = 'accepted') AS accepted_offers_count,
        COUNT_IF(co.offer_status = 'rejected') AS rejected_offers_count
    FROM adaptive_etl_bank.client_offers co
    GROUP BY co.client_id
)
SELECT
    ac.client_id,
    ac.person_code,
    ac.full_name,
    COALESCE(ls.segment_name, 'unclassified') AS segment_name,
    COALESCE(lc.preferred_channel, 'email') AS preferred_channel,
    COALESCE(apc.active_products_count, 0) AS active_products_count,
    COALESCE(a90.total_amount_90d, 0.0) AS total_amount_90d,
    COALESCE(a30.app_events_30d, 0) AS app_events_30d,
    COALESCE(ofb.accepted_offers_count, 0) AS accepted_offers_count,
    COALESCE(ofb.rejected_offers_count, 0) AS rejected_offers_count,
    CASE
        WHEN COALESCE(a90.total_amount_90d, 0.0) >= 200000 THEN 3
        WHEN COALESCE(a90.total_amount_90d, 0.0) >= 80000 THEN 2
        ELSE 1
    END
    + CASE
        WHEN COALESCE(apc.active_products_count, 0) >= 4 THEN 2
        WHEN COALESCE(apc.active_products_count, 0) >= 2 THEN 1
        ELSE 0
    END
    + CASE
        WHEN COALESCE(a30.app_events_30d, 0) >= 20 THEN 2
        WHEN COALESCE(a30.app_events_30d, 0) >= 8 THEN 1
        ELSE 0
    END
    + CASE
        WHEN COALESCE(ofb.accepted_offers_count, 0) > COALESCE(ofb.rejected_offers_count, 0) THEN 1
        ELSE 0
    END AS final_score
FROM active_clients ac
LEFT JOIN latest_segment ls
    ON ac.client_id = ls.client_id
LEFT JOIN latest_contact lc
    ON ac.client_id = lc.client_id
LEFT JOIN active_product_counts apc
    ON ac.client_id = apc.client_id
LEFT JOIN amount_90d a90
    ON ac.client_id = a90.client_id
LEFT JOIN app_30d a30
    ON ac.client_id = a30.client_id
LEFT JOIN offer_feedback ofb
    ON ac.client_id = ofb.client_id
"""


if __name__ == "__main__":
    run_sql(
        sql_text=SQL_QUERY,
        app_name="etl_q06_client_profile_scoring",
        runtime_options=parse_runtime_options(),
    )
