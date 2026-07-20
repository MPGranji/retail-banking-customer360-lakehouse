-- =============================================================
-- PostgreSQL card_crm — SQL Data Generator (manual-run version)
-- Chạy bằng psql:
--   psql -h localhost -p 5432 -U lakehouse -d lakehouse -f postgres_gen.sql
--
-- Tất cả tham số được định nghĩa tập trung trong block \set bên dưới.
-- Giá trị mặc định khớp với data_generator/config.yaml
-- Customer IDs (Oracle): 10001 .. 10000 + n_customers
-- CASA account IDs (Oracle): 100001 .. 100000 + n_customers
-- =============================================================

-- ── Parameters (chỉnh tại đây) ────────────────────────────────
-- seed_float = seed / 1000.0  (seed=42 → 0.042); nhận giá trị -1.0..1.0
\set seed_float      0.042
\set n_customers     10000
\set n_vip           500
\set n_vip_plus_pri  3000
\set start_dt        2025-01-01
\set months_history  12
\set cc_vip_total    120
\set cc_pri_total    96
\set cc_ret_total    48
\set max_card_txns   120
\set crm_total       10000
-- Lưu ý: cc_vip_total = VIP_per_month * months_history (10*12=120)
-- cc_pri_total = PRI_per_month * months_history (8*12=96)
-- cc_ret_total = RET_per_month * months_history (4*12=48)

SELECT setseed(:seed_float);

-- ── Truncate (child-first) ────────────────────────────────────
TRUNCATE TABLE card_crm.card_txn CASCADE;
TRUNCATE TABLE card_crm.crm_interaction CASCADE;
TRUNCATE TABLE card_crm.card CASCADE;

-- ── Credit cards ──────────────────────────────────────────────
-- Assigned to customers whose offset MOD matches the credit_card_ratio:
--   VIP (idx 1..n_vip):       MOD(idx, 100) < 70
--   PRIORITY (next n_pri):    MOD(idx, 100) < 50
--   RETAIL (rest):            MOD(idx, 100) < 15
INSERT INTO card_crm.card (
  card_id, card_no_masked, customer_id, account_id,
  product_code, card_type, card_brand,
  credit_limit, issue_date, expiry_date, status, last_updated
)
WITH custs AS (
  SELECT
    gs                                    AS customer_id,
    gs - 10000                            AS idx,
    CASE
      WHEN gs - 10000 <= :n_vip          THEN 'VIP'
      WHEN gs - 10000 <= :n_vip_plus_pri THEN 'PRIORITY'
      ELSE                                    'RETAIL'
    END                                   AS segment
  FROM generate_series(10001, 10000 + :n_customers) AS gs
),
cc AS (
  SELECT customer_id, idx, segment,
         ROW_NUMBER() OVER (ORDER BY customer_id) AS rn
  FROM custs
  WHERE (segment = 'VIP'      AND MOD(idx, 100) < 70)
     OR (segment = 'PRIORITY' AND MOD(idx, 100) < 50)
     OR (segment = 'RETAIL'   AND MOD(idx, 100) < 15)
)
SELECT
  cc.rn                                                                       AS card_id,
  CASE MOD(cc.rn, 3) WHEN 0 THEN '4000' WHEN 1 THEN '5000' ELSE '4000' END
      || ' ' || LPAD(((cc.rn / 10000) % 10000)::text, 4, '0')
      || ' ****'
      || ' ' || LPAD((cc.rn % 10000)::text, 4, '0')                         AS card_no_masked,
  cc.customer_id,
  NULL::bigint                                                                AS account_id,
  CASE MOD(cc.rn, 3) WHEN 0 THEN 'CC_VISA' WHEN 1 THEN 'CC_MAST' ELSE 'CC_GOLD' END
                                                                              AS product_code,
  'CREDIT'                                                                    AS card_type,
  CASE MOD(cc.rn, 3) WHEN 0 THEN 'VISA' WHEN 1 THEN 'MASTER' ELSE 'VISA' END
                                                                              AS card_brand,
  ROUND((
    CASE cc.segment
      WHEN 'VIP'      THEN random() * 90000000 + 10000000
      WHEN 'PRIORITY' THEN random() * 40000000 +  5000000
      ELSE                 random() * 15000000 +  2000000
    END
  )::numeric, -6)                                                             AS credit_limit,
  CURRENT_DATE - (MOD(cc.idx * 13, 365 * 3) || ' days')::interval           AS issue_date,
  CURRENT_DATE + ((MOD(cc.idx * 7, 3) + 2) * 365 || ' days')::interval      AS expiry_date,
  CASE
    WHEN MOD(cc.rn, 20)  = 0 THEN 'BLOCKED'
    WHEN MOD(cc.rn, 100) = 0 THEN 'CLOSED'
    ELSE                           'ACTIVE'
  END                                                                         AS status,
  NOW() - (MOD(cc.idx, 90) || ' days')::interval                             AS last_updated
FROM cc;

-- ── Debit cards (remaining customers without a credit card) ───
INSERT INTO card_crm.card (
  card_id, card_no_masked, customer_id, account_id,
  product_code, card_type, card_brand,
  credit_limit, issue_date, expiry_date, status, last_updated
)
WITH custs AS (
  SELECT
    gs                                    AS customer_id,
    gs - 10000                            AS idx,
    CASE
      WHEN gs - 10000 <= :n_vip          THEN 'VIP'
      WHEN gs - 10000 <= :n_vip_plus_pri THEN 'PRIORITY'
      ELSE                                    'RETAIL'
    END                                   AS segment
  FROM generate_series(10001, 10000 + :n_customers) AS gs
),
dc AS (
  SELECT customer_id, idx, segment,
         ROW_NUMBER() OVER (ORDER BY customer_id) AS rn
  FROM custs
  WHERE NOT (
      (segment = 'VIP'      AND MOD(idx, 100) < 70)
   OR (segment = 'PRIORITY' AND MOD(idx, 100) < 50)
   OR (segment = 'RETAIL'   AND MOD(idx, 100) < 15)
  )
),
cc_cnt AS (SELECT COUNT(*) AS n FROM card_crm.card WHERE card_type = 'CREDIT')
SELECT
  (SELECT n FROM cc_cnt) + dc.rn                                             AS card_id,
  CASE MOD(dc.rn, 2) WHEN 0 THEN '9704' ELSE '4000' END
      || ' ' || LPAD((((SELECT n FROM cc_cnt) + dc.rn) / 10000 % 10000)::text, 4, '0')
      || ' ****'
      || ' ' || LPAD((((SELECT n FROM cc_cnt) + dc.rn) % 10000)::text, 4, '0') AS card_no_masked,
  dc.customer_id,
  (100000 + dc.idx)::bigint                                                   AS account_id,
  CASE MOD(dc.rn, 2) WHEN 0 THEN 'DC_NAPS' ELSE 'DC_VISA' END               AS product_code,
  'DEBIT'                                                                     AS card_type,
  CASE MOD(dc.rn, 2) WHEN 0 THEN 'NAPAS' ELSE 'VISA' END                    AS card_brand,
  NULL::numeric                                                               AS credit_limit,
  CURRENT_DATE - (MOD(dc.idx * 11, 365 * 3) || ' days')::interval            AS issue_date,
  CURRENT_DATE + ((MOD(dc.idx * 5, 3) + 2) * 365 || ' days')::interval       AS expiry_date,
  CASE WHEN MOD(dc.rn, 30) = 0 THEN 'BLOCKED' ELSE 'ACTIVE' END             AS status,
  NOW() - (MOD(dc.idx, 90) || ' days')::interval                             AS last_updated
FROM dc;

-- ── Card transactions (credit cards only) ─────────────────────
INSERT INTO card_crm.card_txn (
  txn_id, card_id, customer_id, txn_date, txn_amount,
  txn_type, currency, merchant_name, merchant_category,
  channel, status, created_ts, last_updated
)
WITH
  cc_cards AS (
    SELECT
      card_id,
      customer_id,
      (customer_id - 10000)                                                   AS idx,
      CASE
        WHEN customer_id - 10000 <= :n_vip          THEN :cc_vip_total
        WHEN customer_id - 10000 <= :n_vip_plus_pri THEN :cc_pri_total
        ELSE                                              :cc_ret_total
      END                                                                     AS n_txns,
      ROW_NUMBER() OVER (ORDER BY card_id)                                    AS rn
    FROM card_crm.card
    WHERE card_type = 'CREDIT'
  ),
  gen AS (SELECT generate_series(1, :max_card_txns) AS g),
  raw_txns AS (
    SELECT
      c.card_id,
      c.customer_id,
      c.rn,
      g.g,
      :'start_dt'::timestamp
          + random() * (:months_history * 30 + 1) * interval '1 day'        AS txn_date,
      ROUND((random() * 4990000 + 10000)::numeric, -3)                      AS txn_amount
    FROM cc_cards c
    CROSS JOIN gen g
    WHERE g.g <= c.n_txns
  )
SELECT
  ROW_NUMBER() OVER (ORDER BY r.card_id, r.g)                               AS txn_id,
  r.card_id,
  r.customer_id,
  r.txn_date,
  r.txn_amount,
  CASE MOD(r.rn + r.g, 4)
    WHEN 0 THEN 'PURCHASE' WHEN 1 THEN 'CASH_ADVANCE'
    WHEN 2 THEN 'REFUND'   ELSE       'PURCHASE'
  END                                                                        AS txn_type,
  'VND'                                                                      AS currency,
  'Merchant-' || r.card_id || '-' || r.g                                    AS merchant_name,
  CASE MOD(r.rn * 7 + r.g, 6)
    WHEN 0 THEN 'GROCERY'    WHEN 1 THEN 'RESTAURANT'
    WHEN 2 THEN 'TRAVEL'     WHEN 3 THEN 'ECOM'
    WHEN 4 THEN 'FUEL'       ELSE       'EDUCATION'
  END                                                                        AS merchant_category,
  CASE MOD(r.rn + r.g, 3)
    WHEN 0 THEN 'POS' WHEN 1 THEN 'ECOM' ELSE 'ATM'
  END                                                                        AS channel,
  CASE
    WHEN MOD(r.rn + r.g, 20) = 0 THEN 'FAILED'
    WHEN MOD(r.rn + r.g, 50) = 0 THEN 'PENDING'
    ELSE                               'SUCCESS'
  END                                                                        AS status,
  r.txn_date                                                                 AS created_ts,
  r.txn_date                                                                 AS last_updated
FROM raw_txns r;

-- ── CRM interactions ──────────────────────────────────────────
INSERT INTO card_crm.crm_interaction (
  interaction_id, customer_id, interaction_date,
  channel, direction, subject, category,
  status, assigned_to, created_ts, last_updated
)
SELECT
  g                                                                           AS interaction_id,
  customer_id,
  interaction_date,
  CASE MOD(g, 5)
    WHEN 0 THEN 'CALL'   WHEN 1 THEN 'EMAIL'
    WHEN 2 THEN 'CHAT'   WHEN 3 THEN 'BRANCH'
    ELSE       'SMS'
  END                                                                         AS channel,
  CASE WHEN MOD(g, 3) = 0 THEN 'OUTBOUND' ELSE 'INBOUND' END                AS direction,
  'Tuong tac so ' || g                                                        AS subject,
  CASE MOD(g, 5)
    WHEN 0 THEN 'COMPLAINT' WHEN 1 THEN 'INQUIRY'
    WHEN 2 THEN 'CAMPAIGN'  WHEN 3 THEN 'CROSS_SELL'
    ELSE       'RETENTION'
  END                                                                         AS category,
  CASE
    WHEN MOD(g, 10) < 7 THEN 'RESOLVED'
    WHEN MOD(g, 10) < 9 THEN 'OPEN'
    ELSE                      'PENDING'
  END                                                                         AS status,
  'Agent-' || (MOD(g, 50) + 1)                                               AS assigned_to,
  interaction_date                                                             AS created_ts,
  interaction_date                                                             AS last_updated
FROM (
  SELECT
    g,
    10001 + MOD(g - 1, :n_customers)                                         AS customer_id,
    :'start_dt'::timestamp
        + random() * (:months_history * 30 + 1) * interval '1 day'          AS interaction_date
  FROM generate_series(1, :crm_total) AS g
) sub;
