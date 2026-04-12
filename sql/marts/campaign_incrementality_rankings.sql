-- Marts: campaign-level ranking for incrementality decision support
-- Input:
--   - marts.experiment_lift_metrics
--
-- Grain:
--   - one row per campaign_id
--
-- Purpose:
--   - provide simple, interpretable ranking signals for campaign prioritization
--   - combine normalized absolute lift and incremental revenue into a single score

CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.campaign_incrementality_rankings;

CREATE TABLE marts.campaign_incrementality_rankings AS
WITH base AS (
    SELECT
        campaign_id,
        COALESCE(treatment_conversion_rate, 0)::numeric(18, 8) AS treatment_conversion_rate,
        COALESCE(control_conversion_rate, 0)::numeric(18, 8) AS control_conversion_rate,
        COALESCE(absolute_lift, 0)::numeric(18, 8) AS absolute_lift,
        COALESCE(incremental_orders, 0)::numeric(18, 4) AS incremental_orders,
        COALESCE(incremental_revenue, 0)::numeric(18, 2) AS incremental_revenue
    FROM marts.experiment_lift_metrics
),
bounds AS (
    SELECT
        MIN(absolute_lift) AS min_lift,
        MAX(absolute_lift) AS max_lift,
        MIN(incremental_revenue) AS min_revenue,
        MAX(incremental_revenue) AS max_revenue
    FROM base
),
scored AS (
    SELECT
        b.campaign_id,
        b.treatment_conversion_rate,
        b.control_conversion_rate,
        b.absolute_lift,
        b.incremental_orders,
        b.incremental_revenue,
        CASE
            WHEN bo.max_lift = bo.min_lift THEN 0.5::numeric(18, 8)
            ELSE (
                (b.absolute_lift - bo.min_lift)
                / NULLIF(bo.max_lift - bo.min_lift, 0)
            )::numeric(18, 8)
        END AS normalized_lift_score,
        CASE
            WHEN bo.max_revenue = bo.min_revenue THEN 0.5::numeric(18, 8)
            ELSE (
                (b.incremental_revenue - bo.min_revenue)
                / NULLIF(bo.max_revenue - bo.min_revenue, 0)
            )::numeric(18, 8)
        END AS normalized_revenue_score
    FROM base AS b
    CROSS JOIN bounds AS bo
)
SELECT
    campaign_id,
    treatment_conversion_rate,
    control_conversion_rate,
    absolute_lift,
    incremental_orders,
    incremental_revenue,
    RANK() OVER (ORDER BY incremental_revenue DESC, campaign_id ASC)
        AS incremental_revenue_rank,
    RANK() OVER (ORDER BY absolute_lift DESC, campaign_id ASC)
        AS absolute_lift_rank,
    (
        0.5 * normalized_lift_score
        + 0.5 * normalized_revenue_score
    )::numeric(18, 8) AS combined_score
FROM scored;

COMMENT ON TABLE marts.campaign_incrementality_rankings IS
    'Campaign-level incrementality ranking table with lift/revenue ranks and a simple normalized combined score.';
