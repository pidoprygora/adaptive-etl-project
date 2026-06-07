from common import parse_runtime_options, run_sql


SQL_QUERY = """
INSERT INTO adaptive_etl_bank.mailing_base
WITH active_clients AS (
    SELECT c.client_id, c.person_code, c.full_name
    FROM adaptive_etl_bank.clients c
    WHERE c.is_active = true
),
active_campaign_offers AS (
    SELECT
        ca.campaign_id,
        ca.campaign_name,
        co.offer_id,
        co.priority
    FROM adaptive_etl_bank.campaigns ca
    JOIN adaptive_etl_bank.campaign_offers co
        ON ca.campaign_id = co.campaign_id
    WHERE ca.status = 'active'
      AND CURRENT_DATE BETWEEN ca.start_date AND ca.end_date
),
active_offer_details AS (
    SELECT
        o.offer_id,
        o.offer_name,
        o.is_active
    FROM adaptive_etl_bank.offers o
    WHERE o.is_active = true
),
client_score AS (
    SELECT
        cps.client_id,
        cps.final_score
    FROM adaptive_etl_bank.client_profile_scoring cps
),
latest_segment AS (
    SELECT client_id, segment_name
    FROM (
        SELECT
            cs.client_id,
            cs.segment_name,
            ROW_NUMBER() OVER (PARTITION BY cs.client_id ORDER BY cs.updated_at DESC) AS rn
        FROM adaptive_etl_bank.client_segments cs
    ) ranked_segment
    WHERE rn = 1
),
latest_contact AS (
    SELECT client_id, phone, email, preferred_channel, is_verified
    FROM (
        SELECT
            cc.client_id,
            cc.phone,
            cc.email,
            cc.preferred_channel,
            cc.is_verified,
            ROW_NUMBER() OVER (PARTITION BY cc.client_id ORDER BY cc.updated_at DESC) AS rn
        FROM adaptive_etl_bank.client_contacts cc
    ) ranked_contact
    WHERE rn = 1
),
best_offer_per_client AS (
    SELECT
        ac.client_id,
        aco.campaign_id,
        aco.offer_id,
        aco.priority AS campaign_priority,
        ROW_NUMBER() OVER (
            PARTITION BY ac.client_id
            ORDER BY COALESCE(cs.final_score, 0) DESC, aco.priority ASC, aco.offer_id ASC
        ) AS rn
    FROM active_clients ac
    JOIN active_campaign_offers aco
        ON 1 = 1
    LEFT JOIN client_score cs
        ON ac.client_id = cs.client_id
)
SELECT
    boc.client_id,
    ac.person_code,
    ac.full_name,
    boc.campaign_id,
    bod.offer_id,
    aod.offer_name,
    COALESCE(cs.final_score, 0) AS score,
    CASE
        WHEN lc.is_verified = true
             AND lc.preferred_channel IS NOT NULL
             AND (
                 (lc.preferred_channel = 'email' AND lc.email IS NOT NULL)
                 OR (lc.preferred_channel = 'sms' AND lc.phone IS NOT NULL)
                 OR lc.preferred_channel = 'push'
             ) THEN lc.preferred_channel
        WHEN lc.email IS NOT NULL THEN 'email'
        WHEN lc.phone IS NOT NULL THEN 'sms'
        ELSE 'push'
    END AS channel,
    CASE
        WHEN ls.segment_name = 'vip' THEN 1
        WHEN ls.segment_name = 'premium' THEN 2
        WHEN ls.segment_name = 'salary' THEN 3
        ELSE 4
    END AS priority,
    CURRENT_DATE AS planned_send_date,
    'planned' AS mailing_status
FROM best_offer_per_client boc
JOIN active_clients ac
    ON boc.client_id = ac.client_id
JOIN active_offer_details aod
    ON boc.offer_id = aod.offer_id
JOIN active_campaign_offers bod
    ON boc.campaign_id = bod.campaign_id
   AND boc.offer_id = bod.offer_id
LEFT JOIN client_score cs
    ON boc.client_id = cs.client_id
LEFT JOIN latest_segment ls
    ON boc.client_id = ls.client_id
LEFT JOIN latest_contact lc
    ON boc.client_id = lc.client_id
WHERE boc.rn = 1
"""


if __name__ == "__main__":
    run_sql(
        sql_text=SQL_QUERY,
        app_name="etl_q16_mailing_base",
        runtime_options=parse_runtime_options(),
    )
