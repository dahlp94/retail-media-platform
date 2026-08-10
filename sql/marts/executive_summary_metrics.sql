-- Platform-level performance summary

CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.executive_summary_metrics;

CREATE TABLE marts.executive_summary_metrics AS
SELECT
    COALESCE(SUM(b.spend_usd), 0)::numeric(18, 6) AS total_spend_usd,
    COALESCE(SUM(b.revenue_usd), 0)::numeric(18, 2) AS total_revenue_usd,
    COALESCE(SUM(b.orders), 0)::bigint AS total_orders,
    COALESCE(SUM(b.impressions), 0)::bigint AS total_impressions,
    COALESCE(SUM(b.clicks), 0)::bigint AS total_clicks,
    COUNT(*)::bigint AS active_campaign_count,
    (SUM(b.revenue_usd) / NULLIF(SUM(b.spend_usd), 0))::numeric(18, 6) AS overall_roas,
    (SUM(b.clicks)::numeric / NULLIF(SUM(b.impressions), 0))::numeric(18, 8) AS overall_ctr,
    (SUM(b.revenue_usd) / NULLIF(SUM(b.orders), 0))::numeric(18, 4) AS avg_revenue_per_order_usd
FROM marts.campaign_base_metrics AS b;
