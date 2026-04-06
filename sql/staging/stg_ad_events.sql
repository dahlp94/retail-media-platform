-- Staging: ad delivery / exposure events
-- Source: raw.ad_events (TEXT columns from CSV load)
-- Assumes columns: event_id, member_id, campaign_id, timestamp, event_type, channel,
--   cost, advertiser_id, retailer_id (see data/synthetic/ad_events.csv).
-- The CSV column "timestamp" is quoted in raw DDL; expose as event_timestamp for analytics.

CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.stg_ad_events;

CREATE TABLE staging.stg_ad_events AS
SELECT
    event_id::bigint AS event_id,
    member_id::bigint AS member_id,
    campaign_id::bigint AS campaign_id,
    "timestamp"::timestamp AS event_timestamp,
    NULLIF(btrim(event_type), '')::text AS event_type,
    NULLIF(btrim(channel), '')::text AS channel,
    cost::numeric(18, 6) AS cost,
    advertiser_id::integer AS advertiser_id,
    retailer_id::integer AS retailer_id
FROM raw.ad_events;

COMMENT ON TABLE staging.stg_ad_events IS
    'Typed ad events for reach, spend, and attribution joins (time-ordered exposure).';
