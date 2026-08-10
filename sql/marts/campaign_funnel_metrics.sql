-- Campaign-level funnel metrics for engagement and conversion performance

CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.campaign_funnel_metrics;

CREATE TABLE marts.campaign_funnel_metrics AS
SELECT
    b.campaign_id,
    b.impressions,
    b.clicks,
    b.orders,
    (b.clicks::numeric / NULLIF(b.impressions, 0))::numeric(18, 8) AS ctr,
    (b.orders::numeric / NULLIF(b.clicks, 0))::numeric(18, 8) AS cvr,
    (b.spend_usd / NULLIF(b.clicks, 0))::numeric(18, 6) AS cpc_usd,
    (b.spend_usd / NULLIF(b.orders, 0))::numeric(18, 6) AS cpo_usd
FROM marts.campaign_base_metrics AS b;
