-- Marts: daily campaign metrics for trend charts and exploration
-- Inputs: staging.stg_ad_events (delivery by event date), staging.stg_transactions (attributed orders by order date)
-- Grain: one row per (report_date, campaign_id) with activity on that day in either feed
-- Note: Delivery uses DATE(event_timestamp); outcomes use DATE(order_timestamp)—same campaign may show
--   spend on one day and revenue on another when the attribution lag crosses midnight.

CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.daily_campaign_trends;

CREATE TABLE marts.daily_campaign_trends AS
WITH daily_ad AS (
    SELECT
        campaign_id,
        event_timestamp::date AS report_date,
        COUNT(*) FILTER (WHERE event_type = 'impression')::bigint AS impressions,
        COUNT(*) FILTER (WHERE event_type = 'click')::bigint AS clicks,
        COALESCE(SUM(cost), 0)::numeric(18, 6) AS spend_usd
    FROM staging.stg_ad_events
    GROUP BY campaign_id, event_timestamp::date
),
daily_attributed_orders AS (
    SELECT
        source_campaign_id AS campaign_id,
        order_timestamp::date AS report_date,
        COUNT(*)::bigint AS orders,
        COALESCE(SUM(order_value_usd), 0)::numeric(18, 2) AS revenue_usd
    FROM staging.stg_transactions
    WHERE source_campaign_id IS NOT NULL
    GROUP BY source_campaign_id, order_timestamp::date
)
SELECT
    COALESCE(a.report_date, o.report_date) AS report_date,
    COALESCE(a.campaign_id, o.campaign_id) AS campaign_id,
    COALESCE(a.impressions, 0)::bigint AS impressions,
    COALESCE(a.clicks, 0)::bigint AS clicks,
    COALESCE(a.spend_usd, 0)::numeric(18, 6) AS spend_usd,
    COALESCE(o.orders, 0)::bigint AS orders,
    COALESCE(o.revenue_usd, 0)::numeric(18, 2) AS revenue_usd,
    (COALESCE(a.clicks, 0)::numeric / NULLIF(COALESCE(a.impressions, 0), 0))::numeric(18, 8) AS ctr,
    (COALESCE(o.orders, 0)::numeric / NULLIF(COALESCE(a.clicks, 0), 0))::numeric(18, 8) AS cvr,
    (COALESCE(o.revenue_usd, 0) / NULLIF(COALESCE(a.spend_usd, 0), 0))::numeric(18, 6) AS roas
FROM daily_ad AS a
FULL OUTER JOIN daily_attributed_orders AS o
    ON a.campaign_id = o.campaign_id
    AND a.report_date = o.report_date;

COMMENT ON TABLE marts.daily_campaign_trends IS
    'Daily delivery and attributed outcomes per campaign; CTR/CVR/ROAS use same-day ad and order rows.';

COMMENT ON COLUMN marts.daily_campaign_trends.report_date IS 'Calendar date: event date for delivery, order date for revenue.';
COMMENT ON COLUMN marts.daily_campaign_trends.ctr IS 'Daily CTR: clicks / impressions on report_date (NULL if no impressions).';
COMMENT ON COLUMN marts.daily_campaign_trends.cvr IS 'Daily click CVR: orders / clicks on report_date (NULL if no clicks that day).';
COMMENT ON COLUMN marts.daily_campaign_trends.roas IS 'Daily ROAS: revenue_usd / spend_usd on report_date (NULL if no spend).';
