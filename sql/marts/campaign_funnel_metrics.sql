-- Marts: campaign-level funnel KPIs (engagement and conversion efficiency)
-- Primary input: marts.campaign_base_metrics (impressions, clicks, orders, spend_usd)
-- CTR and CVR use ratio-of-sums at campaign grain; CVR is click CVR (orders per click).

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

COMMENT ON TABLE marts.campaign_funnel_metrics IS
    'Campaign funnel: delivery volumes plus CTR, click CVR, CPC, and CPO from base metrics.';

COMMENT ON COLUMN marts.campaign_funnel_metrics.ctr IS 'Click-through rate: clicks / impressions (NULL if no impressions).';
COMMENT ON COLUMN marts.campaign_funnel_metrics.cvr IS 'Click conversion rate: attributed orders / clicks (NULL if no clicks).';
COMMENT ON COLUMN marts.campaign_funnel_metrics.cpc_usd IS 'Cost per click: spend_usd / clicks (NULL if no clicks).';
COMMENT ON COLUMN marts.campaign_funnel_metrics.cpo_usd IS 'Cost per attributed order: spend_usd / orders (NULL if no orders).';
