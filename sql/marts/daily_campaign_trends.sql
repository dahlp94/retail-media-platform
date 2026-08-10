-- Daily campaign performance metrics for trend analysis

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
