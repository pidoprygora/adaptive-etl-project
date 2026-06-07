-- ==================================================================================================
-- complex_etl_queries.sql
-- 20 складних Athena/Presto SQL-запитів для ETL-аналітики банківських маркетингових кампаній.
-- Кожен запит має CTE-блоки і виконується як INSERT INTO у вже підготовлені processed-таблиці.
-- ==================================================================================================

-- IMPORTANT:
-- Athena зазвичай виконує 1 statement за запуск. Виконуй цей файл поетапно.
-- Перед запуском цього файлу створіть цільові таблиці з файлу:
-- sql/complex_etl_queries_targets_ddl.sql
-- Цей файл не створює таблиці, а лише додає нові результати через INSERT INTO.

CREATE DATABASE IF NOT EXISTS adaptive_etl_bank;

-- ====================================
-- QUERY 1. Credit campaign target audience
-- Що робить: Формує базу клієнтів для кредитної кампанії без чинних кредитних продуктів.
-- Processed output: s3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/credit_campaign_target_audience/
-- ====================================
INSERT INTO adaptive_etl_bank.credit_campaign_target_audience
WITH active_clients AS (
    SELECT c.client_id, c.person_code, c.full_name
    FROM adaptive_etl_bank.clients c
    WHERE c.is_active = true
),
latest_contacts AS (
    SELECT client_id, phone, email, preferred_channel, is_verified
    FROM (
        SELECT
            cc.client_id,
            cc.phone,
            cc.email,
            cc.preferred_channel,
            cc.is_verified,
            ROW_NUMBER() OVER (
                PARTITION BY cc.client_id
                ORDER BY cc.updated_at DESC
            ) AS rn
        FROM adaptive_etl_bank.client_contacts cc
    ) ranked_contacts
    WHERE rn = 1
),
latest_segments AS (
    SELECT client_id, segment_name
    FROM (
        SELECT
            cs.client_id,
            cs.segment_name,
            ROW_NUMBER() OVER (
                PARTITION BY cs.client_id
                ORDER BY cs.updated_at DESC
            ) AS rn
        FROM adaptive_etl_bank.client_segments cs
    ) ranked_segments
    WHERE rn = 1
),
excluded_credit_clients AS (
    SELECT DISTINCT cp.client_id
    FROM adaptive_etl_bank.client_products cp
    JOIN adaptive_etl_bank.products p
        ON cp.product_id = p.product_id
    WHERE cp.status = 'active'
      AND p.product_type IN ('credit_card', 'cash_loan')
),
turnover_90d AS (
    SELECT
        cp.client_id,
        COALESCE(SUM(t.amount), 0.0) AS total_amount_90d
    FROM adaptive_etl_bank.client_products cp
    JOIN adaptive_etl_bank.transactions t
        ON cp.client_product_id = t.client_product_id
    WHERE t.status = 'successful'
      AND DATE(t.transaction_date) >= DATE_ADD('day', -90, CURRENT_DATE)
    GROUP BY cp.client_id
),
screen_activity AS (
    SELECT
        ac.client_id,
        COUNT_IF(ac.screen_name = 'loans_screen') AS loans_screen_views,
        COUNT_IF(ac.screen_name = 'cards_screen') AS cards_screen_views
    FROM adaptive_etl_bank.app_clickstream ac
    WHERE DATE(ac.event_time) >= DATE_ADD('day', -90, CURRENT_DATE)
    GROUP BY ac.client_id
),
credit_offers AS (
    SELECT
        o.offer_id,
        o.offer_name,
        p.product_type,
        ROW_NUMBER() OVER (
            PARTITION BY p.product_type
            ORDER BY o.limit_amount DESC, o.interest_rate ASC, o.offer_id ASC
        ) AS rn
    FROM adaptive_etl_bank.offers o
    JOIN adaptive_etl_bank.products p
        ON o.product_id = p.product_id
    WHERE o.is_active = true
      AND p.product_type IN ('credit_card', 'cash_loan')
),
best_credit_offer AS (
    SELECT offer_id, offer_name, product_type
    FROM credit_offers
    WHERE rn = 1
),
audience_scored AS (
    SELECT
        ac.client_id,
        ac.person_code,
        ac.full_name,
        lc.phone,
        lc.email,
        ls.segment_name,
        COALESCE(t.total_amount_90d, 0.0) AS total_amount_90d,
        COALESCE(sa.loans_screen_views, 0) AS loans_screen_views,
        COALESCE(sa.cards_screen_views, 0) AS cards_screen_views,
        CASE
            WHEN COALESCE(t.total_amount_90d, 0.0) >= 200000 THEN 3
            WHEN COALESCE(t.total_amount_90d, 0.0) >= 80000 THEN 2
            ELSE 1
        END
        + CASE
            WHEN COALESCE(sa.loans_screen_views, 0) + COALESCE(sa.cards_screen_views, 0) >= 10 THEN 2
            WHEN COALESCE(sa.loans_screen_views, 0) + COALESCE(sa.cards_screen_views, 0) >= 3 THEN 1
            ELSE 0
        END
        + CASE
            WHEN ls.segment_name IN ('vip', 'premium') THEN 2
            WHEN ls.segment_name = 'salary' THEN 1
            ELSE 0
        END AS credit_score
    FROM active_clients ac
    LEFT JOIN latest_contacts lc
        ON ac.client_id = lc.client_id
    LEFT JOIN latest_segments ls
        ON ac.client_id = ls.client_id
    LEFT JOIN turnover_90d t
        ON ac.client_id = t.client_id
    LEFT JOIN screen_activity sa
        ON ac.client_id = sa.client_id
    LEFT JOIN excluded_credit_clients ec
        ON ac.client_id = ec.client_id
    WHERE ec.client_id IS NULL
)
SELECT
    s.client_id,
    s.person_code,
    s.full_name,
    s.phone,
    s.email,
    COALESCE(s.segment_name, 'unclassified') AS segment_name,
    bco.offer_id,
    bco.offer_name,
    COALESCE(
        CASE
            WHEN s.phone IS NOT NULL THEN 'sms'
            WHEN s.email IS NOT NULL THEN 'email'
            ELSE 'push'
        END,
        'email'
    ) AS channel,
    s.credit_score,
    CASE
        WHEN s.credit_score >= 6 THEN 1
        WHEN s.credit_score >= 4 THEN 2
        ELSE 3
    END AS priority
FROM audience_scored s
JOIN best_credit_offer bco
    ON 1 = 1;

-- ====================================
-- QUERY 2. Deposit campaign target audience
-- Що робить: Формує клієнтів для депозитної кампанії із високим балансом і зарплатними надходженнями.
-- Processed output: s3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/deposit_campaign_target_audience/
-- ====================================
INSERT INTO adaptive_etl_bank.deposit_campaign_target_audience
WITH active_clients AS (
    SELECT c.client_id, c.full_name
    FROM adaptive_etl_bank.clients c
    WHERE c.is_active = true
),
latest_segments AS (
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
average_balance AS (
    SELECT
        cp.client_id,
        AVG(COALESCE(cp.balance, 0.0)) AS avg_balance
    FROM adaptive_etl_bank.client_products cp
    WHERE cp.status = 'active'
    GROUP BY cp.client_id
    HAVING AVG(COALESCE(cp.balance, 0.0)) >= 50000
),
existing_deposit_clients AS (
    SELECT DISTINCT cp.client_id
    FROM adaptive_etl_bank.client_products cp
    JOIN adaptive_etl_bank.products p
        ON cp.product_id = p.product_id
    WHERE cp.status = 'active'
      AND p.product_type = 'deposit'
),
salary_transactions_180d AS (
    SELECT
        cp.client_id,
        COUNT_IF(t.transaction_type = 'salary' AND t.status = 'successful') AS salary_transactions_count
    FROM adaptive_etl_bank.client_products cp
    LEFT JOIN adaptive_etl_bank.transactions t
        ON cp.client_product_id = t.client_product_id
       AND DATE(t.transaction_date) >= DATE_ADD('day', -180, CURRENT_DATE)
    GROUP BY cp.client_id
),
deposit_screen_activity AS (
    SELECT
        ac.client_id,
        COUNT_IF(ac.screen_name = 'deposits_screen') AS deposits_screen_views
    FROM adaptive_etl_bank.app_clickstream ac
    WHERE DATE(ac.event_time) >= DATE_ADD('day', -90, CURRENT_DATE)
    GROUP BY ac.client_id
),
deposit_offer AS (
    SELECT offer_id, channel_hint
    FROM (
        SELECT
            o.offer_id,
            CASE
                WHEN o.min_income >= 50000 THEN 'manager_call'
                ELSE 'email'
            END AS channel_hint,
            ROW_NUMBER() OVER (ORDER BY o.interest_rate DESC, o.offer_id ASC) AS rn
        FROM adaptive_etl_bank.offers o
        JOIN adaptive_etl_bank.products p
            ON o.product_id = p.product_id
        WHERE o.is_active = true
          AND p.product_type = 'deposit'
    ) ranked_offer
    WHERE rn = 1
),
scored_clients AS (
    SELECT
        ac.client_id,
        ac.full_name,
        ls.segment_name,
        COALESCE(ab.avg_balance, 0.0) AS avg_balance,
        COALESCE(st.salary_transactions_count, 0) AS salary_transactions_count,
        COALESCE(dsa.deposits_screen_views, 0) AS deposits_screen_views,
        CASE
            WHEN COALESCE(ab.avg_balance, 0.0) >= 200000 THEN 3
            WHEN COALESCE(ab.avg_balance, 0.0) >= 100000 THEN 2
            ELSE 1
        END
        + CASE
            WHEN COALESCE(st.salary_transactions_count, 0) >= 4 THEN 2
            WHEN COALESCE(st.salary_transactions_count, 0) >= 2 THEN 1
            ELSE 0
        END
        + CASE
            WHEN COALESCE(dsa.deposits_screen_views, 0) >= 5 THEN 2
            WHEN COALESCE(dsa.deposits_screen_views, 0) >= 2 THEN 1
            ELSE 0
        END AS deposit_score
    FROM active_clients ac
    JOIN average_balance ab
        ON ac.client_id = ab.client_id
    LEFT JOIN latest_segments ls
        ON ac.client_id = ls.client_id
    LEFT JOIN salary_transactions_180d st
        ON ac.client_id = st.client_id
    LEFT JOIN deposit_screen_activity dsa
        ON ac.client_id = dsa.client_id
    LEFT JOIN existing_deposit_clients ed
        ON ac.client_id = ed.client_id
    WHERE ed.client_id IS NULL
)
SELECT
    sc.client_id,
    sc.full_name,
    COALESCE(sc.segment_name, 'unclassified') AS segment_name,
    sc.avg_balance,
    sc.salary_transactions_count,
    sc.deposit_score,
    d.offer_id,
    d.channel_hint AS channel
FROM scored_clients sc
JOIN deposit_offer d
    ON 1 = 1;

-- ====================================
-- QUERY 3. Insurance cross-sell audience
-- Що робить: Визначає клієнтів із кредитними продуктами для крос-продажу страхування.
-- Processed output: s3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/insurance_cross_sell_audience/
-- ====================================
INSERT INTO adaptive_etl_bank.insurance_cross_sell_audience
WITH active_clients AS (
    SELECT c.client_id, c.full_name
    FROM adaptive_etl_bank.clients c
    WHERE c.is_active = true
),
clients_with_credit_products AS (
    SELECT DISTINCT cp.client_id
    FROM adaptive_etl_bank.client_products cp
    JOIN adaptive_etl_bank.products p
        ON cp.product_id = p.product_id
    WHERE cp.status = 'active'
      AND p.product_type IN ('credit_card', 'mortgage', 'cash_loan')
),
clients_with_insurance AS (
    SELECT DISTINCT cp.client_id
    FROM adaptive_etl_bank.client_products cp
    JOIN adaptive_etl_bank.products p
        ON cp.product_id = p.product_id
    WHERE cp.status = 'active'
      AND p.product_type = 'insurance'
),
insurance_screen_activity AS (
    SELECT
        ac.client_id,
        COUNT_IF(ac.screen_name = 'insurance_screen') AS insurance_screen_views
    FROM adaptive_etl_bank.app_clickstream ac
    WHERE DATE(ac.event_time) >= DATE_ADD('day', -90, CURRENT_DATE)
    GROUP BY ac.client_id
),
category_spending AS (
    SELECT
        cp.client_id,
        COALESCE(SUM(CASE WHEN t.merchant_category = 'travel' THEN t.amount ELSE 0.0 END), 0.0) AS travel_amount,
        COALESCE(SUM(CASE WHEN t.merchant_category = 'pharmacy' THEN t.amount ELSE 0.0 END), 0.0) AS pharmacy_amount,
        COALESCE(SUM(CASE WHEN t.merchant_category = 'fuel' THEN t.amount ELSE 0.0 END), 0.0) AS fuel_amount
    FROM adaptive_etl_bank.client_products cp
    LEFT JOIN adaptive_etl_bank.transactions t
        ON cp.client_product_id = t.client_product_id
       AND t.status = 'successful'
       AND DATE(t.transaction_date) >= DATE_ADD('day', -90, CURRENT_DATE)
    GROUP BY cp.client_id
),
insurance_offer AS (
    SELECT offer_id, offer_name
    FROM (
        SELECT
            o.offer_id,
            o.offer_name,
            ROW_NUMBER() OVER (ORDER BY o.limit_amount DESC, o.offer_id ASC) AS rn
        FROM adaptive_etl_bank.offers o
        JOIN adaptive_etl_bank.products p
            ON o.product_id = p.product_id
        WHERE o.is_active = true
          AND p.product_type = 'insurance'
    ) ranked_offer
    WHERE rn = 1
)
SELECT
    ac.client_id,
    ac.full_name,
    io.offer_id,
    io.offer_name,
    COALESCE(isa.insurance_screen_views, 0) AS insurance_screen_views,
    COALESCE(cs.travel_amount, 0.0) + COALESCE(cs.pharmacy_amount, 0.0) + COALESCE(cs.fuel_amount, 0.0) AS lifestyle_spending_amount,
    CASE
        WHEN COALESCE(isa.insurance_screen_views, 0) >= 5 THEN 3
        WHEN COALESCE(isa.insurance_screen_views, 0) >= 2 THEN 2
        ELSE 1
    END
    + CASE
        WHEN COALESCE(cs.travel_amount, 0.0) + COALESCE(cs.pharmacy_amount, 0.0) + COALESCE(cs.fuel_amount, 0.0) >= 40000 THEN 3
        WHEN COALESCE(cs.travel_amount, 0.0) + COALESCE(cs.pharmacy_amount, 0.0) + COALESCE(cs.fuel_amount, 0.0) >= 15000 THEN 2
        ELSE 1
    END AS insurance_score
FROM active_clients ac
JOIN clients_with_credit_products ccp
    ON ac.client_id = ccp.client_id
LEFT JOIN clients_with_insurance cwi
    ON ac.client_id = cwi.client_id
LEFT JOIN insurance_screen_activity isa
    ON ac.client_id = isa.client_id
LEFT JOIN category_spending cs
    ON ac.client_id = cs.client_id
JOIN insurance_offer io
    ON 1 = 1
WHERE cwi.client_id IS NULL;

-- ====================================
-- QUERY 4. Premium upgrade audience
-- Що робить: Рахує premium_score для mass/salary клієнтів і відбирає кращих кандидатів.
-- Processed output: s3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/premium_upgrade_audience/
-- ====================================
INSERT INTO adaptive_etl_bank.premium_upgrade_audience
WITH latest_segments AS (
    SELECT client_id, segment_name
    FROM (
        SELECT
            cs.client_id,
            cs.segment_name,
            ROW_NUMBER() OVER (PARTITION BY cs.client_id ORDER BY cs.updated_at DESC) AS rn
        FROM adaptive_etl_bank.client_segments cs
    ) ranked_segments
    WHERE rn = 1
      AND segment_name IN ('mass', 'salary')
),
turnover_180d AS (
    SELECT
        cp.client_id,
        COALESCE(SUM(t.amount), 0.0) AS total_turnover_180d
    FROM adaptive_etl_bank.client_products cp
    LEFT JOIN adaptive_etl_bank.transactions t
        ON cp.client_product_id = t.client_product_id
       AND t.status = 'successful'
       AND DATE(t.transaction_date) >= DATE_ADD('day', -180, CURRENT_DATE)
    GROUP BY cp.client_id
),
active_products AS (
    SELECT
        cp.client_id,
        COUNT_IF(cp.status = 'active') AS active_products_count,
        AVG(CASE WHEN cp.status = 'active' THEN COALESCE(cp.balance, 0.0) END) AS avg_balance
    FROM adaptive_etl_bank.client_products cp
    GROUP BY cp.client_id
),
app_activity AS (
    SELECT
        ac.client_id,
        COUNT(ac.app_event_id) AS app_events_90d
    FROM adaptive_etl_bank.app_clickstream ac
    WHERE DATE(ac.event_time) >= DATE_ADD('day', -90, CURRENT_DATE)
    GROUP BY ac.client_id
),
scored AS (
    SELECT
        ls.client_id,
        ls.segment_name,
        COALESCE(t.total_turnover_180d, 0.0) AS total_turnover_180d,
        COALESCE(ap.active_products_count, 0) AS active_products_count,
        COALESCE(ap.avg_balance, 0.0) AS avg_balance,
        COALESCE(aa.app_events_90d, 0) AS app_events_90d,
        CASE
            WHEN COALESCE(t.total_turnover_180d, 0.0) >= 400000 THEN 3
            WHEN COALESCE(t.total_turnover_180d, 0.0) >= 150000 THEN 2
            ELSE 1
        END
        + CASE
            WHEN COALESCE(ap.active_products_count, 0) >= 4 THEN 2
            WHEN COALESCE(ap.active_products_count, 0) >= 2 THEN 1
            ELSE 0
        END
        + CASE
            WHEN COALESCE(ap.avg_balance, 0.0) >= 120000 THEN 2
            WHEN COALESCE(ap.avg_balance, 0.0) >= 50000 THEN 1
            ELSE 0
        END
        + CASE
            WHEN COALESCE(aa.app_events_90d, 0) >= 40 THEN 2
            WHEN COALESCE(aa.app_events_90d, 0) >= 15 THEN 1
            ELSE 0
        END AS premium_score
    FROM latest_segments ls
    LEFT JOIN turnover_180d t
        ON ls.client_id = t.client_id
    LEFT JOIN active_products ap
        ON ls.client_id = ap.client_id
    LEFT JOIN app_activity aa
        ON ls.client_id = aa.client_id
)
SELECT
    s.client_id,
    s.segment_name,
    s.total_turnover_180d,
    s.active_products_count,
    s.avg_balance,
    s.app_events_90d,
    s.premium_score
FROM scored s
WHERE s.premium_score > 2;

-- ====================================
-- QUERY 5. Retention campaign for inactive clients
-- Що робить: Виявляє клієнтів без активності 60+ днів і підбирає retention-пропозицію.
-- Processed output: s3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/retention_inactive_clients/
-- ====================================
INSERT INTO adaptive_etl_bank.retention_inactive_clients
WITH active_clients AS (
    SELECT c.client_id, c.full_name
    FROM adaptive_etl_bank.clients c
    WHERE c.is_active = true
),
last_transaction AS (
    SELECT
        cp.client_id,
        MAX(DATE(t.transaction_date)) AS last_transaction_date
    FROM adaptive_etl_bank.client_products cp
    LEFT JOIN adaptive_etl_bank.transactions t
        ON cp.client_product_id = t.client_product_id
    GROUP BY cp.client_id
),
last_app_event AS (
    SELECT
        ac.client_id,
        MAX(DATE(ac.event_time)) AS last_app_event_date
    FROM adaptive_etl_bank.app_clickstream ac
    GROUP BY ac.client_id
),
inactive_clients AS (
    SELECT
        c.client_id,
        c.full_name,
        lt.last_transaction_date,
        la.last_app_event_date,
        GREATEST(
            COALESCE(lt.last_transaction_date, DATE_ADD('day', -9999, CURRENT_DATE)),
            COALESCE(la.last_app_event_date, DATE_ADD('day', -9999, CURRENT_DATE))
        ) AS last_activity_date
    FROM active_clients c
    LEFT JOIN last_transaction lt
        ON c.client_id = lt.client_id
    LEFT JOIN last_app_event la
        ON c.client_id = la.client_id
    WHERE GREATEST(
              COALESCE(lt.last_transaction_date, DATE_ADD('day', -9999, CURRENT_DATE)),
              COALESCE(la.last_app_event_date, DATE_ADD('day', -9999, CURRENT_DATE))
          ) < DATE_ADD('day', -60, CURRENT_DATE)
),
last_active_product AS (
    SELECT client_id, product_name, product_type
    FROM (
        SELECT
            cp.client_id,
            p.product_name,
            p.product_type,
            ROW_NUMBER() OVER (
                PARTITION BY cp.client_id
                ORDER BY COALESCE(cp.last_activity_date, cp.open_date) DESC
            ) AS rn
        FROM adaptive_etl_bank.client_products cp
        JOIN adaptive_etl_bank.products p
            ON cp.product_id = p.product_id
        WHERE cp.status = 'active'
    ) ranked_products
    WHERE rn = 1
),
retention_offer AS (
    SELECT offer_id, offer_name
    FROM (
        SELECT
            o.offer_id,
            o.offer_name,
            ROW_NUMBER() OVER (ORDER BY o.offer_id ASC) AS rn
        FROM adaptive_etl_bank.offers o
        WHERE o.is_active = true
          AND LOWER(o.offer_name) LIKE '%retention%'
    ) ranked_retention
    WHERE rn = 1
)
SELECT
    ic.client_id,
    ic.full_name,
    ic.last_transaction_date,
    ic.last_app_event_date,
    ic.last_activity_date,
    lap.product_name AS last_active_product_name,
    lap.product_type AS last_active_product_type,
    ro.offer_id,
    COALESCE(ro.offer_name, 'retention_campaign_default') AS offer_name
FROM inactive_clients ic
LEFT JOIN last_active_product lap
    ON ic.client_id = lap.client_id
LEFT JOIN retention_offer ro
    ON 1 = 1;

-- ====================================
-- QUERY 6. Client profile for scoring
-- Що робить: Будує єдиний скоринговий профіль клієнта для наступних ETL-кроків.
-- Processed output: s3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/client_profile_scoring/
-- ====================================
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
    ON ac.client_id = ofb.client_id;

-- ====================================
-- QUERY 7. Best channel selection for client
-- Що робить: Розраховує результативність каналів і обирає найкращий канал через ROW_NUMBER.
-- Processed output: s3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/client_best_channel/
-- ====================================
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
WHERE rc.rn = 1;

-- ====================================
-- QUERY 8. Campaign performance dashboard
-- Що робить: Формує агреговані KPI кампаній (sent/delivered/opened/clicked/accepted/failed + rates).
-- Processed output: s3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/campaign_performance_dashboard/
-- ====================================
INSERT INTO adaptive_etl_bank.campaign_performance_dashboard
WITH campaign_offers_base AS (
    SELECT
        ca.campaign_id,
        ca.campaign_name,
        ca.status AS campaign_status,
        co.client_offer_id,
        co.client_id,
        co.offer_status
    FROM adaptive_etl_bank.campaigns ca
    LEFT JOIN adaptive_etl_bank.client_offers co
        ON ca.campaign_id = co.campaign_id
),
event_flags AS (
    SELECT
        me.client_offer_id,
        MAX(CASE WHEN me.event_type = 'sent' THEN 1 ELSE 0 END) AS has_sent,
        MAX(CASE WHEN me.event_type = 'delivered' THEN 1 ELSE 0 END) AS has_delivered,
        MAX(CASE WHEN me.event_type = 'opened' THEN 1 ELSE 0 END) AS has_opened,
        MAX(CASE WHEN me.event_type = 'clicked' THEN 1 ELSE 0 END) AS has_clicked,
        MAX(CASE WHEN ds.is_success = false OR me.event_type = 'failed' THEN 1 ELSE 0 END) AS has_failed
    FROM adaptive_etl_bank.mailing_events me
    LEFT JOIN adaptive_etl_bank.delivery_statuses ds
        ON me.delivery_status_id = ds.delivery_status_id
    GROUP BY me.client_offer_id
),
campaign_kpi AS (
    SELECT
        cob.campaign_id,
        cob.campaign_name,
        cob.campaign_status,
        COUNT(cob.client_offer_id) AS total_client_offers,
        COALESCE(SUM(ef.has_sent), 0) AS sent_count,
        COALESCE(SUM(ef.has_delivered), 0) AS delivered_count,
        COALESCE(SUM(ef.has_opened), 0) AS opened_count,
        COALESCE(SUM(ef.has_clicked), 0) AS clicked_count,
        COUNT_IF(cob.offer_status = 'accepted') AS accepted_count,
        COALESCE(SUM(ef.has_failed), 0) AS failed_count
    FROM campaign_offers_base cob
    LEFT JOIN event_flags ef
        ON cob.client_offer_id = ef.client_offer_id
    GROUP BY cob.campaign_id, cob.campaign_name, cob.campaign_status
)
SELECT
    ck.campaign_id,
    ck.campaign_name,
    ck.campaign_status,
    ck.total_client_offers,
    ck.sent_count,
    ck.delivered_count,
    ck.opened_count,
    ck.clicked_count,
    ck.accepted_count,
    ck.failed_count,
    CASE WHEN ck.sent_count > 0 THEN CAST(ck.delivered_count AS DOUBLE) / ck.sent_count ELSE 0.0 END AS delivery_rate,
    CASE WHEN ck.delivered_count > 0 THEN CAST(ck.opened_count AS DOUBLE) / ck.delivered_count ELSE 0.0 END AS open_rate,
    CASE WHEN ck.opened_count > 0 THEN CAST(ck.clicked_count AS DOUBLE) / ck.opened_count ELSE 0.0 END AS click_rate,
    CASE WHEN ck.sent_count > 0 THEN CAST(ck.accepted_count AS DOUBLE) / ck.sent_count ELSE 0.0 END AS conversion_rate
FROM campaign_kpi ck;

-- ====================================
-- QUERY 9. Product conversion analysis
-- Що робить: Порівнює конверсію оферів по типах продуктів.
-- Processed output: s3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/product_conversion_analysis/
-- ====================================
INSERT INTO adaptive_etl_bank.product_conversion_analysis
WITH offers_with_type AS (
    SELECT
        co.client_offer_id,
        co.client_id,
        co.offer_status,
        co.offer_id,
        o.offer_name,
        p.product_type
    FROM adaptive_etl_bank.client_offers co
    JOIN adaptive_etl_bank.offers o
        ON co.offer_id = o.offer_id
    JOIN adaptive_etl_bank.products p
        ON o.product_id = p.product_id
),
event_summary AS (
    SELECT
        me.client_offer_id,
        MAX(CASE WHEN me.event_type = 'opened' THEN 1 ELSE 0 END) AS has_opened
    FROM adaptive_etl_bank.mailing_events me
    GROUP BY me.client_offer_id
),
product_aggregates AS (
    SELECT
        owt.product_type,
        COUNT(owt.client_offer_id) AS total_offers,
        COALESCE(SUM(es.has_opened), 0) AS opened_offers,
        COUNT_IF(owt.offer_status = 'accepted') AS accepted_offers,
        COUNT_IF(owt.offer_status = 'rejected') AS rejected_offers
    FROM offers_with_type owt
    LEFT JOIN event_summary es
        ON owt.client_offer_id = es.client_offer_id
    GROUP BY owt.product_type
    HAVING COUNT(owt.client_offer_id) > 0
)
SELECT
    pa.product_type,
    pa.total_offers,
    pa.opened_offers,
    pa.accepted_offers,
    pa.rejected_offers,
    CASE WHEN pa.total_offers > 0 THEN CAST(pa.accepted_offers AS DOUBLE) / pa.total_offers ELSE 0.0 END AS conversion_rate
FROM product_aggregates pa
ORDER BY conversion_rate DESC, total_offers DESC;

-- ====================================
-- QUERY 10. Delivery failure analysis
-- Що робить: Виявляє проблемні комбінації каналу та error_code з failure_rate > 10%.
-- Processed output: s3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/delivery_failure_analysis/
-- ====================================
INSERT INTO adaptive_etl_bank.delivery_failure_analysis
WITH event_base AS (
    SELECT
        co.channel,
        COALESCE(me.error_code, 'NO_ERROR_CODE') AS error_code,
        me.event_type,
        COALESCE(ds.is_success, false) AS is_success
    FROM adaptive_etl_bank.mailing_events me
    JOIN adaptive_etl_bank.client_offers co
        ON me.client_offer_id = co.client_offer_id
    LEFT JOIN adaptive_etl_bank.delivery_statuses ds
        ON me.delivery_status_id = ds.delivery_status_id
),
grouped_failures AS (
    SELECT
        eb.channel,
        eb.error_code,
        COUNT(1) AS total_events,
        COUNT_IF(eb.event_type = 'failed' OR eb.is_success = false) AS failed_events
    FROM event_base eb
    GROUP BY eb.channel, eb.error_code
    HAVING COUNT(1) >= 10
)
SELECT
    gf.channel,
    gf.error_code,
    gf.total_events,
    gf.failed_events,
    CASE WHEN gf.total_events > 0 THEN (CAST(gf.failed_events AS DOUBLE) / gf.total_events) * 100.0 ELSE 0.0 END AS failure_rate
FROM grouped_failures gf
WHERE CASE WHEN gf.total_events > 0 THEN (CAST(gf.failed_events AS DOUBLE) / gf.total_events) * 100.0 ELSE 0.0 END > 10.0
ORDER BY failure_rate DESC, total_events DESC;

-- ====================================
-- QUERY 11. App behavior based offer recommendation
-- Що робить: Рекомендує тип офера на основі найчастіше переглянутого екрану застосунку.
-- Processed output: s3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/app_behavior_offer_recommendation/
-- ====================================
INSERT INTO adaptive_etl_bank.app_behavior_offer_recommendation
WITH screen_views_30d AS (
    SELECT
        ac.client_id,
        ac.screen_name,
        COUNT(ac.app_event_id) AS views_count
    FROM adaptive_etl_bank.app_clickstream ac
    WHERE DATE(ac.event_time) >= DATE_ADD('day', -30, CURRENT_DATE)
    GROUP BY ac.client_id, ac.screen_name
),
top_screen AS (
    SELECT client_id, screen_name, views_count
    FROM (
        SELECT
            sv.client_id,
            sv.screen_name,
            sv.views_count,
            ROW_NUMBER() OVER (
                PARTITION BY sv.client_id
                ORDER BY sv.views_count DESC, sv.screen_name ASC
            ) AS rn
        FROM screen_views_30d sv
    ) ranked_screens
    WHERE rn = 1
),
recommended_type AS (
    SELECT
        ts.client_id,
        ts.screen_name AS top_screen_name,
        ts.views_count AS top_screen_views,
        CASE
            WHEN ts.screen_name = 'loans_screen' THEN 'cash_loan'
            WHEN ts.screen_name = 'cards_screen' THEN 'credit_card'
            WHEN ts.screen_name = 'deposits_screen' THEN 'deposit'
            WHEN ts.screen_name = 'insurance_screen' THEN 'insurance'
            ELSE 'credit_card'
        END AS recommended_product_type
    FROM top_screen ts
),
ranked_offers AS (
    SELECT
        p.product_type,
        o.offer_id,
        o.offer_name,
        ROW_NUMBER() OVER (
            PARTITION BY p.product_type
            ORDER BY o.limit_amount DESC, o.interest_rate ASC, o.offer_id ASC
        ) AS rn
    FROM adaptive_etl_bank.offers o
    JOIN adaptive_etl_bank.products p
        ON o.product_id = p.product_id
    WHERE o.is_active = true
)
SELECT
    rt.client_id,
    rt.top_screen_name,
    rt.top_screen_views,
    rt.recommended_product_type,
    ro.offer_id,
    ro.offer_name
FROM recommended_type rt
LEFT JOIN ranked_offers ro
    ON rt.recommended_product_type = ro.product_type
   AND ro.rn = 1;

-- ====================================
-- QUERY 12. Transaction behavior segmentation
-- Що робить: Сегментує клієнтів за обсягом, частотою і різноманіттям транзакцій.
-- Processed output: s3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/transaction_behavior_segmentation/
-- ====================================
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
    ON ta.client_id = ls.client_id;

-- ====================================
-- QUERY 13. High value client detection
-- Що робить: Рахує value_score і вибирає TOP 1000 найцінніших клієнтів.
-- Processed output: s3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/high_value_clients_top1000/
-- ====================================
INSERT INTO adaptive_etl_bank.high_value_clients_top1000
WITH turnover_180d AS (
    SELECT
        cp.client_id,
        COALESCE(SUM(t.amount), 0.0) AS total_turnover_180d
    FROM adaptive_etl_bank.client_products cp
    LEFT JOIN adaptive_etl_bank.transactions t
        ON cp.client_product_id = t.client_product_id
       AND t.status = 'successful'
       AND DATE(t.transaction_date) >= DATE_ADD('day', -180, CURRENT_DATE)
    GROUP BY cp.client_id
),
balance_and_products AS (
    SELECT
        cp.client_id,
        AVG(CASE WHEN cp.status = 'active' THEN COALESCE(cp.balance, 0.0) END) AS avg_balance,
        COUNT_IF(cp.status = 'active') AS active_products_count
    FROM adaptive_etl_bank.client_products cp
    GROUP BY cp.client_id
),
accepted_offers AS (
    SELECT
        co.client_id,
        COUNT_IF(co.offer_status = 'accepted') AS accepted_offers_count
    FROM adaptive_etl_bank.client_offers co
    GROUP BY co.client_id
),
app_activity AS (
    SELECT
        ac.client_id,
        COUNT(ac.app_event_id) AS app_events_90d
    FROM adaptive_etl_bank.app_clickstream ac
    WHERE DATE(ac.event_time) >= DATE_ADD('day', -90, CURRENT_DATE)
    GROUP BY ac.client_id
),
scored_clients AS (
    SELECT
        c.client_id,
        c.full_name,
        COALESCE(t.total_turnover_180d, 0.0) AS total_turnover_180d,
        COALESCE(bp.avg_balance, 0.0) AS avg_balance,
        COALESCE(bp.active_products_count, 0) AS active_products_count,
        COALESCE(ao.accepted_offers_count, 0) AS accepted_offers_count,
        COALESCE(aa.app_events_90d, 0) AS app_events_90d,
        (
            COALESCE(t.total_turnover_180d, 0.0) / 100000.0
            + COALESCE(bp.avg_balance, 0.0) / 50000.0
            + COALESCE(bp.active_products_count, 0) * 0.5
            + COALESCE(ao.accepted_offers_count, 0) * 0.7
            + COALESCE(aa.app_events_90d, 0) / 20.0
        ) AS value_score
    FROM adaptive_etl_bank.clients c
    LEFT JOIN turnover_180d t
        ON c.client_id = t.client_id
    LEFT JOIN balance_and_products bp
        ON c.client_id = bp.client_id
    LEFT JOIN accepted_offers ao
        ON c.client_id = ao.client_id
    LEFT JOIN app_activity aa
        ON c.client_id = aa.client_id
    WHERE c.is_active = true
),
ranked_clients AS (
    SELECT
        sc.client_id,
        sc.full_name,
        sc.total_turnover_180d,
        sc.avg_balance,
        sc.active_products_count,
        sc.accepted_offers_count,
        sc.app_events_90d,
        sc.value_score,
        ROW_NUMBER() OVER (
            ORDER BY sc.value_score DESC, sc.total_turnover_180d DESC, sc.client_id ASC
        ) AS rn
    FROM scored_clients sc
)
SELECT
    rc.client_id,
    rc.full_name,
    rc.total_turnover_180d,
    rc.avg_balance,
    rc.active_products_count,
    rc.accepted_offers_count,
    rc.app_events_90d,
    rc.value_score
FROM ranked_clients rc
WHERE rc.rn <= 1000;

-- ====================================
-- QUERY 14. Duplicate client offer detection
-- Що робить: Виявляє дублікати призначених оферів клієнтам для контролю якості даних.
-- Processed output: s3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/duplicate_client_offers/
-- ====================================
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
    ON gd.client_id = ci.client_id;

-- ====================================
-- QUERY 15. Expired offer cleanup candidate
-- Що робить: Готує список client_offers, що мають бути переведені у статус expired.
-- Processed output: s3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/expired_offer_cleanup_candidate/
-- ====================================
INSERT INTO adaptive_etl_bank.expired_offer_cleanup_candidate
WITH expired_candidates AS (
    SELECT
        co.client_offer_id,
        co.client_id,
        co.offer_id,
        co.campaign_id,
        co.offer_status,
        co.valid_until
    FROM adaptive_etl_bank.client_offers co
    WHERE co.valid_until < CURRENT_DATE
),
filtered_candidates AS (
    SELECT
        ec.client_offer_id,
        ec.client_id,
        ec.offer_id,
        ec.campaign_id,
        ec.valid_until
    FROM expired_candidates ec
    WHERE ec.offer_status NOT IN ('accepted', 'rejected', 'expired')
)
SELECT
    fc.client_offer_id,
    fc.client_id,
    fc.offer_id,
    fc.campaign_id,
    fc.valid_until,
    'expired' AS new_offer_status
FROM filtered_candidates fc;

-- ====================================
-- QUERY 16. Mailing base generation
-- Що робить: Формує фінальну таблицю mailing_base із каналом, score і priority для розсилки.
-- Processed output: s3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/mailing_base/
-- ====================================
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
WHERE boc.rn = 1;

-- ====================================
-- QUERY 17. Mailing schedule optimization
-- Що робить: Формує оптимальний schedule_batch за пріоритетами часу відправки.
-- Processed output: s3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/mailing_schedule_optimization/
-- ====================================
INSERT INTO adaptive_etl_bank.mailing_schedule_optimization
WITH planned_base AS (
    SELECT
        mb.client_id,
        mb.campaign_id,
        mb.offer_id,
        mb.channel,
        mb.priority,
        mb.planned_send_date
    FROM adaptive_etl_bank.mailing_base mb
    WHERE mb.mailing_status = 'planned'
),
scheduled AS (
    SELECT
        pb.client_id,
        pb.campaign_id,
        pb.offer_id,
        pb.channel,
        pb.priority,
        pb.planned_send_date,
        CASE
            WHEN pb.priority = 1 THEN '09:00'
            WHEN pb.priority = 2 THEN '11:00'
            WHEN pb.priority = 3 THEN '14:00'
            ELSE '16:00'
        END AS schedule_hour,
        CASE
            WHEN pb.priority = 1 THEN CONCAT(CAST(pb.planned_send_date AS VARCHAR), ' 09:00')
            WHEN pb.priority = 2 THEN CONCAT(CAST(pb.planned_send_date AS VARCHAR), ' 11:00')
            WHEN pb.priority = 3 THEN CONCAT(CAST(pb.planned_send_date AS VARCHAR), ' 14:00')
            ELSE CONCAT(CAST(pb.planned_send_date AS VARCHAR), ' 16:00')
        END AS schedule_batch
    FROM planned_base pb
)
SELECT
    s.client_id,
    s.campaign_id,
    s.offer_id,
    s.channel,
    s.priority,
    s.planned_send_date,
    s.schedule_hour,
    s.schedule_batch
FROM scheduled s;

-- ====================================
-- QUERY 18. ETL execution metrics aggregation
-- Що робить: Агрегує ETL-метрики по dataset_size для аналізу продуктивності.
-- Processed output: s3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/etl_metrics_aggregation/
-- ====================================
INSERT INTO adaptive_etl_bank.etl_metrics_aggregation
WITH successful_runs AS (
    SELECT
        em.dataset_size,
        em.extract_time_sec,
        em.transform_time_sec,
        em.load_time_sec,
        em.total_execution_time_sec,
        em.parallel_tasks_count,
        COALESCE(em.measured_cpu_utilization, em.cpu_utilization) AS effective_cpu_utilization,
        COALESCE(em.measured_ram_utilization, em.ram_utilization) AS effective_ram_utilization,
        em.speedup,
        em.efficiency,
        em.amdahl_speedup,
        em.load_balance_coeff,
        em.task_load,
        em.critical_path_time_sec,
        em.etl_time_sec,
        em.predicted_time_new_sec
    FROM adaptive_etl_bank.etl_execution_metrics em
    WHERE em.status = 'success'
),
aggregated AS (
    SELECT
        sr.dataset_size,
        AVG(sr.extract_time_sec) AS avg_extract_time_sec,
        MIN(sr.extract_time_sec) AS min_extract_time_sec,
        MAX(sr.extract_time_sec) AS max_extract_time_sec,
        AVG(sr.transform_time_sec) AS avg_transform_time_sec,
        MIN(sr.transform_time_sec) AS min_transform_time_sec,
        MAX(sr.transform_time_sec) AS max_transform_time_sec,
        AVG(sr.load_time_sec) AS avg_load_time_sec,
        MIN(sr.load_time_sec) AS min_load_time_sec,
        MAX(sr.load_time_sec) AS max_load_time_sec,
        AVG(sr.total_execution_time_sec) AS avg_total_time_sec,
        MIN(sr.total_execution_time_sec) AS min_total_time_sec,
        MAX(sr.total_execution_time_sec) AS max_total_time_sec,
        AVG(CAST(sr.parallel_tasks_count AS DOUBLE)) AS avg_parallel_tasks,
        AVG(sr.effective_cpu_utilization) AS avg_cpu_utilization,
        AVG(sr.effective_ram_utilization) AS avg_ram_utilization,
        AVG(sr.speedup) AS avg_speedup,
        AVG(sr.efficiency) AS avg_efficiency,
        AVG(sr.amdahl_speedup) AS avg_amdahl_speedup,
        AVG(sr.load_balance_coeff) AS avg_load_balance_coeff,
        AVG(sr.task_load) AS avg_task_load,
        AVG(sr.critical_path_time_sec) AS avg_critical_path_time_sec,
        AVG(sr.etl_time_sec) AS avg_etl_time_sec,
        AVG(sr.predicted_time_new_sec) AS avg_predicted_time_new_sec
    FROM successful_runs sr
    GROUP BY sr.dataset_size
)
SELECT
    a.dataset_size,
    a.avg_extract_time_sec,
    a.min_extract_time_sec,
    a.max_extract_time_sec,
    a.avg_transform_time_sec,
    a.min_transform_time_sec,
    a.max_transform_time_sec,
    a.avg_load_time_sec,
    a.min_load_time_sec,
    a.max_load_time_sec,
    a.avg_total_time_sec,
    a.min_total_time_sec,
    a.max_total_time_sec,
    a.avg_parallel_tasks,
    a.avg_cpu_utilization,
    a.avg_ram_utilization,
    a.avg_speedup,
    a.avg_efficiency,
    a.avg_amdahl_speedup,
    a.avg_load_balance_coeff,
    a.avg_task_load,
    a.avg_critical_path_time_sec,
    a.avg_etl_time_sec,
    a.avg_predicted_time_new_sec
FROM aggregated a;

-- ====================================
-- QUERY 19. Adaptive parallelism recommendation
-- Що робить: Розраховує recommended_parallel_tasks на основі агрегованих ETL-метрик.
-- Processed output: s3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/adaptive_parallelism_recommendation/
-- ====================================
INSERT INTO adaptive_etl_bank.adaptive_parallelism_recommendation
WITH metrics_base AS (
    SELECT
        ema.dataset_size,
        ema.avg_task_load AS task_integral_load,
        ema.avg_total_time_sec,
        ema.avg_parallel_tasks,
        ema.avg_cpu_utilization,
        ema.avg_ram_utilization,
        ema.avg_load_balance_coeff
    FROM adaptive_etl_bank.etl_metrics_aggregation ema
),
dag_load AS (
    SELECT AVG(mb.task_integral_load) AS dag_avg_load
    FROM metrics_base mb
),
recommended AS (
    SELECT
        mb.dataset_size,
        mb.task_integral_load,
        dl.dag_avg_load,
        mb.avg_total_time_sec,
        mb.avg_parallel_tasks,
        mb.avg_cpu_utilization,
        mb.avg_ram_utilization,
        mb.avg_load_balance_coeff,
        CASE
            WHEN mb.avg_cpu_utilization > 0.8 OR mb.avg_ram_utilization > 0.8
                THEN GREATEST(
                    1,
                    LEAST(8, CAST(CEIL(mb.task_integral_load / NULLIF(dl.dag_avg_load, 0.0)) AS INTEGER)) - 1
                )
            WHEN mb.avg_cpu_utilization < 0.5 AND mb.avg_ram_utilization < 0.5
                THEN LEAST(
                    8,
                    GREATEST(1, CAST(CEIL(mb.task_integral_load / NULLIF(dl.dag_avg_load, 0.0)) AS INTEGER)) + 1
                )
            ELSE LEAST(
                8,
                GREATEST(
                    1,
                    CAST(CEIL(mb.task_integral_load / NULLIF(dl.dag_avg_load, 0.0)) AS INTEGER)
                )
            )
        END AS recommended_parallel_tasks
    FROM metrics_base mb
    CROSS JOIN dag_load dl
)
SELECT
    r.dataset_size,
    r.task_integral_load,
    r.dag_avg_load,
    r.avg_total_time_sec,
    r.avg_parallel_tasks,
    r.avg_cpu_utilization,
    r.avg_ram_utilization,
    r.avg_load_balance_coeff AS load_balance_coeff,
    r.recommended_parallel_tasks
FROM recommended r;

-- ====================================
-- QUERY 20. End-to-end campaign readiness check
-- Що робить: Перевіряє готовність активних кампаній до запуску та присвоює readiness_status.
-- Processed output: s3://adaptive-etl-project-032896316649-eu-north-1-an/processed/bank_data/campaign_readiness_check/
-- ====================================
INSERT INTO adaptive_etl_bank.campaign_readiness_check
WITH active_campaigns AS (
    SELECT
        ca.campaign_id,
        ca.campaign_name
    FROM adaptive_etl_bank.campaigns ca
    WHERE ca.status = 'active'
      AND CURRENT_DATE BETWEEN ca.start_date AND ca.end_date
),
offers_count AS (
    SELECT
        co.campaign_id,
        COUNT(co.offer_id) AS offers_count
    FROM adaptive_etl_bank.campaign_offers co
    GROUP BY co.campaign_id
),
mailing_clients AS (
    SELECT
        mb.campaign_id,
        COUNT(DISTINCT mb.client_id) AS mailing_clients_count
    FROM adaptive_etl_bank.mailing_base mb
    GROUP BY mb.campaign_id
),
valid_contacts AS (
    SELECT
        mb.campaign_id,
        COUNT(DISTINCT mb.client_id) AS valid_contacts_count
    FROM adaptive_etl_bank.mailing_base mb
    JOIN adaptive_etl_bank.client_contacts cc
        ON mb.client_id = cc.client_id
    WHERE cc.is_verified = true
      AND (
          cc.email IS NOT NULL
          OR cc.phone IS NOT NULL
          OR cc.push_token IS NOT NULL
      )
    GROUP BY mb.campaign_id
),
duplicates AS (
    SELECT
        co.campaign_id,
        COALESCE(SUM(CASE WHEN duplicate_count > 1 THEN duplicate_count - 1 ELSE 0 END), 0) AS duplicate_count
    FROM (
        SELECT
            co_inner.campaign_id,
            co_inner.client_id,
            co_inner.offer_id,
            COUNT(1) AS duplicate_count
        FROM adaptive_etl_bank.client_offers co_inner
        GROUP BY co_inner.campaign_id, co_inner.client_id, co_inner.offer_id
    ) co
    GROUP BY co.campaign_id
),
expired_offers AS (
    SELECT
        co.campaign_id,
        COUNT_IF(co.valid_until < CURRENT_DATE AND co.offer_status NOT IN ('accepted', 'rejected')) AS expired_offer_count
    FROM adaptive_etl_bank.client_offers co
    GROUP BY co.campaign_id
)
SELECT
    ac.campaign_id,
    ac.campaign_name,
    COALESCE(oc.offers_count, 0) AS offers_count,
    COALESCE(mc.mailing_clients_count, 0) AS mailing_clients_count,
    COALESCE(vc.valid_contacts_count, 0) AS valid_contacts_count,
    COALESCE(d.duplicate_count, 0) AS duplicate_count,
    COALESCE(eo.expired_offer_count, 0) AS expired_offer_count,
    CASE
        WHEN COALESCE(oc.offers_count, 0) = 0
             OR COALESCE(mc.mailing_clients_count, 0) = 0
             OR COALESCE(vc.valid_contacts_count, 0) = 0 THEN 'blocked'
        WHEN COALESCE(d.duplicate_count, 0) > 0
             OR COALESCE(eo.expired_offer_count, 0) > 0 THEN 'warning'
        ELSE 'ready'
    END AS readiness_status
FROM active_campaigns ac
LEFT JOIN offers_count oc
    ON ac.campaign_id = oc.campaign_id
LEFT JOIN mailing_clients mc
    ON ac.campaign_id = mc.campaign_id
LEFT JOIN valid_contacts vc
    ON ac.campaign_id = vc.campaign_id
LEFT JOIN duplicates d
    ON ac.campaign_id = d.campaign_id
LEFT JOIN expired_offers eo
    ON ac.campaign_id = eo.campaign_id;
