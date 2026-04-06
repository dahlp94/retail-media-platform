-- Staging: purchase / order outcomes
-- Source: raw.transactions (TEXT columns from CSV load)
-- Assumes columns: transaction_id, member_id, retailer_id, audience_segment_id,
--   order_timestamp, order_value_usd, outcome_currency, purchase_driver, source_campaign_id
--   (see data/synthetic/transactions.csv). source_campaign_id may be blank when unknown.

CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.stg_transactions;

CREATE TABLE staging.stg_transactions AS
SELECT
    transaction_id::bigint AS transaction_id,
    member_id::bigint AS member_id,
    retailer_id::integer AS retailer_id,
    audience_segment_id::integer AS audience_segment_id,
    order_timestamp::timestamp AS order_timestamp,
    order_value_usd::numeric(18, 2) AS order_value_usd,
    NULLIF(btrim(outcome_currency), '')::text AS outcome_currency,
    NULLIF(btrim(purchase_driver), '')::text AS purchase_driver,
    CASE
        WHEN NULLIF(btrim(source_campaign_id), '') IS NULL THEN NULL
        ELSE NULLIF(btrim(source_campaign_id), '')::bigint
    END AS source_campaign_id
FROM raw.transactions;

COMMENT ON TABLE staging.stg_transactions IS
    'Typed transactions for revenue KPIs, attribution to source_campaign_id, and lift outcomes.';
