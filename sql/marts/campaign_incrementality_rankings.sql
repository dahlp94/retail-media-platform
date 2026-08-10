-- Campaign-level ranking for incrementality decision support.

CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.campaign_incrementality_rankings;

CREATE TABLE marts.campaign_incrementality_rankings AS

WITH campaign_metrics AS (
    SELECT
        campaign_id,
        COALESCE(treatment_conversion_rate, 0)::numeric(18, 8)
            AS treatment_conversion_rate,
        COALESCE(control_conversion_rate, 0)::numeric(18, 8)
            AS control_conversion_rate,
        COALESCE(absolute_lift, 0)::numeric(18, 8)
            AS absolute_lift,
        COALESCE(incremental_orders, 0)::numeric(18, 4)
            AS incremental_orders,
        COALESCE(incremental_revenue, 0)::numeric(18, 2)
            AS incremental_revenue
    FROM marts.experiment_lift_metrics
),

metric_bounds AS (
    SELECT
        MIN(absolute_lift) AS min_lift,
        MAX(absolute_lift) AS max_lift,
        MIN(incremental_revenue) AS min_revenue,
        MAX(incremental_revenue) AS max_revenue
    FROM campaign_metrics
),

scored_campaigns AS (
    SELECT
        c.campaign_id,
        c.treatment_conversion_rate,
        c.control_conversion_rate,
        c.absolute_lift,
        c.incremental_orders,
        c.incremental_revenue,

        CASE
            WHEN b.max_lift = b.min_lift THEN 0.5
            ELSE
                (c.absolute_lift - b.min_lift)
                / (b.max_lift - b.min_lift)
        END::numeric(18, 8) AS normalized_lift_score,

        CASE
            WHEN b.max_revenue = b.min_revenue THEN 0.5
            ELSE
                (c.incremental_revenue - b.min_revenue)
                / (b.max_revenue - b.min_revenue)
        END::numeric(18, 8) AS normalized_revenue_score

    FROM campaign_metrics AS c
    CROSS JOIN metric_bounds AS b
)

SELECT
    campaign_id,
    treatment_conversion_rate,
    control_conversion_rate,
    absolute_lift,
    incremental_orders,
    incremental_revenue,

    RANK() OVER (
        ORDER BY incremental_revenue DESC, campaign_id
    ) AS incremental_revenue_rank,

    RANK() OVER (
        ORDER BY absolute_lift DESC, campaign_id
    ) AS absolute_lift_rank,

    (
        0.5 * normalized_lift_score
        + 0.5 * normalized_revenue_score
    )::numeric(18, 8) AS combined_score

FROM scored_campaigns;
