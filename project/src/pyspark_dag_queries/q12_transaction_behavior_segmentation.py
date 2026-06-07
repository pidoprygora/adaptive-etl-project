from common import parse_runtime_options, run_sql


SQL_QUERY = """
INSERT INTO adaptive_etl_bank.transaction_behavior_segmentation
WITH successful_transactions_90d AS (
    SELECT
        cp.client_id,
        t.transaction_id,
        t.amount,
        t.merchant_category
    FROM adaptive_etl_bank.client_products cp
    JOIN adaptive_etl_bank.transactions t
        ON cp.client_product_id = t.client_product_id
    WHERE t.status = 'successful'
      AND DATE(t.transaction_date) >= DATE_ADD('day', -90, CURRENT_DATE)
),
tx_aggregates AS (
    SELECT
        st.client_id,
        COALESCE(SUM(st.amount), 0.0) AS total_amount,
        COALESCE(AVG(st.amount), 0.0) AS avg_amount,
        COUNT(st.transaction_id) AS transactions_count,
        COUNT(DISTINCT st.merchant_category) AS distinct_merchant_categories
    FROM successful_transactions_90d st
    GROUP BY st.client_id
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
)
SELECT
    ta.client_id,
    ta.total_amount,
    ta.avg_amount,
    ta.transactions_count,
    ta.distinct_merchant_categories,
    CASE
        WHEN ta.transactions_count >= 60 OR ta.total_amount >= 250000 THEN 'high_activity'
        WHEN ta.transactions_count >= 25 OR ta.total_amount >= 90000 THEN 'medium_activity'
        ELSE 'low_activity'
    END AS activity_group,
    COALESCE(ls.segment_name, 'unclassified') AS segment_name
FROM tx_aggregates ta
LEFT JOIN latest_segment ls
    ON ta.client_id = ls.client_id
"""


if __name__ == "__main__":
    run_sql(
        sql_text=SQL_QUERY,
        app_name="etl_q12_transaction_behavior_segmentation",
        runtime_options=parse_runtime_options(),
    )
