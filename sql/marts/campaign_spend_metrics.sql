-- FROZEN LEGACY REFERENCE (Stage 5).
-- Canonical build: dbt/models/marts/attribution/campaign_spend_metrics.sql
-- Campaign-level spend and revenue efficiency metrics

CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.campaign_spend_metrics;

CREATE TABLE marts.campaign_spend_metrics AS
SELECT
    b.campaign_id,
    b.spend_usd,
    b.revenue_usd,
    b.orders,
    (b.revenue_usd / NULLIF(b.spend_usd, 0))::numeric(18, 6) AS roas,
    (b.revenue_usd / NULLIF(b.orders, 0))::numeric(18, 4) AS avg_revenue_per_order_usd,
    (b.spend_usd / NULLIF(b.impressions, 0))::numeric(18, 8) AS avg_spend_per_impression_usd,
    (b.spend_usd / NULLIF(b.clicks, 0))::numeric(18, 6) AS avg_spend_per_click_usd
FROM marts.campaign_base_metrics AS b;
