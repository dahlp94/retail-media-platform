-- Campaign performance summary by campaign_id

CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.campaign_base_metrics;

CREATE TABLE marts.campaign_base_metrics AS
WITH ad_by_campaign AS (
    SELECT
        campaign_id,
        COUNT(*) FILTER (WHERE event_type = 'impression')::bigint AS impressions,
        COUNT(*) FILTER (WHERE event_type = 'click')::bigint AS clicks,
        COALESCE(SUM(cost), 0)::numeric(18, 6) AS spend_usd
    FROM staging.stg_ad_events
    GROUP BY campaign_id
),
attributed_orders AS (
    SELECT
        source_campaign_id AS campaign_id,
        COUNT(*)::bigint AS orders,
        COALESCE(SUM(order_value_usd), 0)::numeric(18, 2) AS revenue_usd
    FROM staging.stg_transactions
    WHERE source_campaign_id IS NOT NULL
    GROUP BY source_campaign_id
)
SELECT
    COALESCE(a.campaign_id, o.campaign_id) AS campaign_id,
    COALESCE(a.impressions, 0)::bigint AS impressions,
    COALESCE(a.clicks, 0)::bigint AS clicks,
    COALESCE(a.spend_usd, 0)::numeric(18, 6) AS spend_usd,
    COALESCE(o.orders, 0)::bigint AS orders,
    COALESCE(o.revenue_usd, 0)::numeric(18, 2) AS revenue_usd
FROM ad_by_campaign a
FULL OUTER JOIN attributed_orders o ON a.campaign_id = o.campaign_id;
