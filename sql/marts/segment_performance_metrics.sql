-- Campaign-level subgroup metrics by audience segment and geography.
-- Outcomes include campaign-attributed purchases during the campaign window.

CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.segment_performance_metrics;

CREATE TABLE marts.segment_performance_metrics AS

WITH attributed_outcomes AS (
    SELECT
        t.member_id,
        t.source_campaign_id AS campaign_id,
        COUNT(*)::bigint AS order_count,
        SUM(t.order_value_usd)::numeric(18, 2) AS revenue_usd
    FROM staging.stg_transactions AS t
    INNER JOIN staging.stg_campaigns AS c
        ON c.campaign_id = t.source_campaign_id
    WHERE t.source_campaign_id IS NOT NULL
      AND t.order_timestamp::date BETWEEN c.start_date AND c.end_date
    GROUP BY
        t.member_id,
        t.source_campaign_id
),

member_outcomes AS (
    SELECT
        a.campaign_id,
        m.audience_segment_id,
        m.primary_geo_id,
        a.member_id,
        a.experiment_arm,
        COALESCE(o.order_count, 0)::bigint AS order_count,
        COALESCE(o.revenue_usd, 0)::numeric(18, 2) AS revenue_usd,
        CASE
            WHEN COALESCE(o.order_count, 0) > 0 THEN 1
            ELSE 0
        END::smallint AS is_converter
    FROM staging.stg_experiment_assignment AS a
    LEFT JOIN staging.stg_members AS m
        ON m.member_id = a.member_id
    LEFT JOIN attributed_outcomes AS o
        ON o.member_id = a.member_id
        AND o.campaign_id = a.campaign_id
    WHERE a.experiment_arm IN ('treatment', 'control')
),

arm_metrics AS (
    SELECT
        campaign_id,
        audience_segment_id,
        primary_geo_id,
        experiment_arm,
        COUNT(*)::bigint AS member_count,
        SUM(is_converter)::bigint AS converters,
        SUM(order_count)::bigint AS orders,
        SUM(revenue_usd)::numeric(18, 2) AS revenue
    FROM member_outcomes
    GROUP BY
        campaign_id,
        audience_segment_id,
        primary_geo_id,
        experiment_arm
),

segment_metrics AS (
    SELECT
        campaign_id,
        audience_segment_id,
        primary_geo_id,

        COALESCE(
            MAX(member_count) FILTER (WHERE experiment_arm = 'treatment'),
            0
        )::bigint AS treatment_member_count,

        COALESCE(
            MAX(member_count) FILTER (WHERE experiment_arm = 'control'),
            0
        )::bigint AS control_member_count,

        COALESCE(
            MAX(converters) FILTER (WHERE experiment_arm = 'treatment'),
            0
        )::bigint AS treatment_converters,

        COALESCE(
            MAX(converters) FILTER (WHERE experiment_arm = 'control'),
            0
        )::bigint AS control_converters,

        COALESCE(
            MAX(orders) FILTER (WHERE experiment_arm = 'treatment'),
            0
        )::bigint AS treatment_orders,

        COALESCE(
            MAX(orders) FILTER (WHERE experiment_arm = 'control'),
            0
        )::bigint AS control_orders,

        COALESCE(
            MAX(revenue) FILTER (WHERE experiment_arm = 'treatment'),
            0
        )::numeric(18, 2) AS treatment_revenue,

        COALESCE(
            MAX(revenue) FILTER (WHERE experiment_arm = 'control'),
            0
        )::numeric(18, 2) AS control_revenue

    FROM arm_metrics
    GROUP BY
        campaign_id,
        audience_segment_id,
        primary_geo_id
),

computed_metrics AS (
    SELECT
        *,
        (
            treatment_converters::numeric
            / NULLIF(treatment_member_count, 0)
        )::numeric(18, 8) AS treatment_conversion_rate,

        (
            control_converters::numeric
            / NULLIF(control_member_count, 0)
        )::numeric(18, 8) AS control_conversion_rate

    FROM segment_metrics
)

SELECT
    campaign_id,
    audience_segment_id,
    primary_geo_id,
    treatment_member_count,
    control_member_count,
    treatment_conversion_rate,
    control_conversion_rate,

    (treatment_conversion_rate - control_conversion_rate)::numeric(18, 8)
        AS absolute_lift,

    (
        (treatment_conversion_rate - control_conversion_rate)
        / NULLIF(control_conversion_rate, 0)
    )::numeric(18, 8) AS relative_lift,

    CASE
        WHEN treatment_member_count > 0
             AND control_member_count > 0
        THEN (
            treatment_orders
            - treatment_member_count::numeric
              * (control_orders::numeric / control_member_count)
        )::numeric(18, 4)
        ELSE NULL
    END AS incremental_orders,

    CASE
        WHEN treatment_member_count > 0
             AND control_member_count > 0
        THEN (
            treatment_revenue
            - treatment_member_count::numeric
              * (control_revenue / control_member_count)
        )::numeric(18, 2)
        ELSE NULL
    END AS incremental_revenue

FROM computed_metrics;
