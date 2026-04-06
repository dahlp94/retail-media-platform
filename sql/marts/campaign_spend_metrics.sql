-- Marts: campaign-level spend and attributed revenue efficiency
-- Primary input: marts.campaign_base_metrics (spend_usd, revenue_usd, orders, impressions, clicks)
-- ROAS matches docs/kpi_definitions.md: attributed revenue / spend.

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

COMMENT ON TABLE marts.campaign_spend_metrics IS
    'Spend and revenue efficiency: ROAS, AOV, and average cost per impression and per click.';

COMMENT ON COLUMN marts.campaign_spend_metrics.roas IS 'Return on ad spend: revenue_usd / spend_usd (NULL if no spend).';
COMMENT ON COLUMN marts.campaign_spend_metrics.avg_revenue_per_order_usd IS 'Attributed average order value: revenue_usd / orders.';
COMMENT ON COLUMN marts.campaign_spend_metrics.avg_spend_per_impression_usd IS 'Spend divided by impressions (actual $ per impression; NULL if none).';
COMMENT ON COLUMN marts.campaign_spend_metrics.avg_spend_per_click_usd IS 'Spend divided by clicks; equals CPC from funnel metrics (NULL if no clicks).';
