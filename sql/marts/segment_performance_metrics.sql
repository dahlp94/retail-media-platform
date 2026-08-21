-- FROZEN LEGACY REFERENCE (Stage 5).
-- Canonical build: dbt/models/marts/incrementality/segment_performance_metrics.sql
-- Subgroup treatment-control metrics by audience segment and geography.
-- Outcomes match marts.experiment_lift_metrics: all member purchases during
-- the campaign window, independent of source_campaign_id.
-- Grain: one row per (campaign_id, audience_segment_id, primary_geo_id).

CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.segment_performance_metrics;

CREATE TABLE marts.segment_performance_metrics AS

WITH experiment_population AS (
    SELECT
        a.campaign_id,
        a.member_id,
        a.experiment_arm,
        m.audience_segment_id,
        m.primary_geo_id,
        c.start_date,
        c.end_date
    FROM staging.stg_experiment_assignment AS a
    INNER JOIN staging.stg_campaigns AS c
        ON c.campaign_id = a.campaign_id
    LEFT JOIN staging.stg_members AS m
        ON m.member_id = a.member_id
    WHERE a.experiment_arm IN ('treatment', 'control')
),

member_outcomes AS (
    SELECT
        ep.campaign_id,
        ep.audience_segment_id,
        ep.primary_geo_id,
        ep.member_id,
        ep.experiment_arm,
        COUNT(t.transaction_id)::bigint AS order_count,
        COALESCE(SUM(t.order_value_usd), 0)::numeric(18, 2) AS revenue_usd,
        CASE
            WHEN COUNT(t.transaction_id) > 0 THEN 1
            ELSE 0
        END::smallint AS is_converter
    FROM experiment_population AS ep
    LEFT JOIN staging.stg_transactions AS t
        ON t.member_id = ep.member_id
        AND t.order_timestamp::date BETWEEN ep.start_date AND ep.end_date
    GROUP BY
        ep.campaign_id,
        ep.audience_segment_id,
        ep.primary_geo_id,
        ep.member_id,
        ep.experiment_arm
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
    treatment_converters,
    control_converters,
    treatment_orders,
    control_orders,
    treatment_revenue,
    control_revenue,
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
