from common import parse_runtime_options, run_sql


SQL_QUERY = """
INSERT INTO adaptive_etl_bank.client_best_channel
WITH contact_channels AS (
    SELECT
        cc.client_id,
        COUNT_IF(cc.phone IS NOT NULL) AS has_phone_count,
        COUNT_IF(cc.email IS NOT NULL) AS has_email_count,
        COUNT_IF(cc.push_token IS NOT NULL) AS has_push_count,
        MAX(CASE WHEN cc.preferred_channel = 'sms' THEN 1 ELSE 0 END) AS prefers_sms,
        MAX(CASE WHEN cc.preferred_channel = 'email' THEN 1 ELSE 0 END) AS prefers_email,
        MAX(CASE WHEN cc.preferred_channel = 'push' THEN 1 ELSE 0 END) AS prefers_push
    FROM adaptive_etl_bank.client_contacts cc
    GROUP BY cc.client_id
),
channel_events AS (
    SELECT
        co.client_id,
        co.channel,
        COUNT_IF(me.event_type = 'sent') AS sent_count,
        COUNT_IF(me.event_type = 'delivered') AS delivered_count,
        COUNT_IF(me.event_type = 'opened') AS opened_count,
        COUNT_IF(me.event_type = 'clicked') AS clicked_count,
        COUNT_IF(co.offer_status = 'accepted') AS accepted_count
    FROM adaptive_etl_bank.client_offers co
    LEFT JOIN adaptive_etl_bank.mailing_events me
        ON co.client_offer_id = me.client_offer_id
    GROUP BY co.client_id, co.channel
    HAVING COUNT_IF(me.event_type = 'sent') > 0
),
scored_channels AS (
    SELECT
        ce.client_id,
        ce.channel,
        ce.sent_count,
        ce.delivered_count,
        ce.opened_count,
        ce.clicked_count,
        ce.accepted_count,
        (COALESCE(ce.delivered_count, 0) * 0.2)
        + (COALESCE(ce.opened_count, 0) * 0.3)
        + (COALESCE(ce.clicked_count, 0) * 0.5)
        + (COALESCE(ce.accepted_count, 0) * 1.0)
        + CASE
            WHEN ce.channel = 'sms' AND cc.prefers_sms = 1 THEN 0.5
            WHEN ce.channel = 'email' AND cc.prefers_email = 1 THEN 0.5
            WHEN ce.channel = 'push' AND cc.prefers_push = 1 THEN 0.5
            ELSE 0.0
          END AS channel_score
    FROM channel_events ce
    LEFT JOIN contact_channels cc
        ON ce.client_id = cc.client_id
),
ranked_channels AS (
    SELECT
        sc.client_id,
        sc.channel,
        sc.channel_score,
        ROW_NUMBER() OVER (
            PARTITION BY sc.client_id
            ORDER BY sc.channel_score DESC, sc.accepted_count DESC, sc.channel ASC
        ) AS rn
    FROM scored_channels sc
)
SELECT
    rc.client_id,
    rc.channel AS best_channel,
    rc.channel_score
FROM ranked_channels rc
WHERE rc.rn = 1
"""


if __name__ == "__main__":
    run_sql(
        sql_text=SQL_QUERY,
        app_name="etl_q07_client_best_channel",
        runtime_options=parse_runtime_options(),
    )
