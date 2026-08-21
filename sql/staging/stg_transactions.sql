-- FROZEN LEGACY REFERENCE (Stage 5).
-- Canonical build: dbt/models/staging/stg_transactions.sql
-- Staging: purchase / order outcomes

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
