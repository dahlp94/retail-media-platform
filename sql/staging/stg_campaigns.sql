-- Staging: campaigns (retail media campaign metadata)
-- Source: raw.campaigns (all columns TEXT from CSV load)
-- Assumes CSV columns: campaign_id, campaign_name, advertiser_id, retailer_id, channel,
--   pricing_model, bid_price_usd, budget_usd, daily_budget_usd,
--   target_audience_segment_id, target_geo_id, start_date, end_date
--   (see data/synthetic/campaigns.csv).

CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.stg_campaigns;

CREATE TABLE staging.stg_campaigns AS
SELECT
    campaign_id::bigint AS campaign_id,
    NULLIF(btrim(campaign_name), '')::text AS campaign_name,
    advertiser_id::integer AS advertiser_id,
    retailer_id::integer AS retailer_id,
    NULLIF(btrim(channel), '')::text AS channel,
    NULLIF(btrim(pricing_model), '')::text AS pricing_model,
    bid_price_usd::numeric(14, 4) AS bid_price_usd,
    budget_usd::numeric(18, 2) AS budget_usd,
    daily_budget_usd::numeric(18, 2) AS daily_budget_usd,
    target_audience_segment_id::integer AS target_audience_segment_id,
    target_geo_id::integer AS target_geo_id,
    start_date::date AS start_date,
    end_date::date AS end_date
FROM raw.campaigns;

COMMENT ON TABLE staging.stg_campaigns IS
    'Typed campaign dimension for attribution, spend, and geo/audience targeting joins.';
