-- Marts: campaign-level base delivery and attributed outcome totals
-- Inputs: staging.stg_ad_events (impressions, clicks, per-event cost),
--         staging.stg_transactions (orders attributed via source_campaign_id)
-- Grain: one row per campaign_id that has at least one ad event or attributed order

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

COMMENT ON TABLE marts.campaign_base_metrics IS
    'Campaign-level counts and sums: delivery (impressions, clicks, spend) from ad events; '
    'attributed orders and revenue from transactions linked by source_campaign_id.';

COMMENT ON COLUMN marts.campaign_base_metrics.campaign_id IS 'Retail media campaign identifier.';
COMMENT ON COLUMN marts.campaign_base_metrics.impressions IS 'Count of impression events for the campaign.';
COMMENT ON COLUMN marts.campaign_base_metrics.clicks IS 'Count of click events for the campaign.';
COMMENT ON COLUMN marts.campaign_base_metrics.spend_usd IS 'Sum of per-event media cost (aligned to simulation pricing rules).';
COMMENT ON COLUMN marts.campaign_base_metrics.orders IS 'Count of transactions attributed to this campaign (source_campaign_id).';
COMMENT ON COLUMN marts.campaign_base_metrics.revenue_usd IS 'Sum of order_value_usd for attributed transactions.';
