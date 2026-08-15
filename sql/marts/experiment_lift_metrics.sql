-- Campaign-level treatment/control incrementality metrics.
-- Outcomes include all member purchases during the campaign window.
-- Point estimates only. Uncertainty columns are added by
-- scripts/run_incrementality.py from marts.experiment_member_outcomes.

CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.experiment_lift_metrics;

CREATE TABLE marts.experiment_lift_metrics AS

WITH experiment_population AS (
    SELECT
        a.campaign_id,
        a.member_id,
        a.experiment_arm,
        c.start_date,
        c.end_date
    FROM staging.stg_experiment_assignment AS a
    INNER JOIN staging.stg_campaigns AS c
        ON c.campaign_id = a.campaign_id
    WHERE a.experiment_arm IN ('treatment', 'control')
),

member_outcomes AS (
    SELECT
        ep.campaign_id,
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
        ep.member_id,
        ep.experiment_arm
),

arm_metrics AS (
    SELECT
        campaign_id,
        experiment_arm,
        COUNT(*)::bigint AS member_count,
        SUM(is_converter)::bigint AS converters,
        SUM(order_count)::bigint AS orders,
        SUM(revenue_usd)::numeric(18, 2) AS revenue,
        AVG(is_converter::numeric)::numeric(18, 8) AS conversion_rate,
        AVG(order_count::numeric)::numeric(18, 8) AS orders_per_member,
        AVG(revenue_usd)::numeric(18, 8) AS revenue_per_member
    FROM member_outcomes
    GROUP BY
        campaign_id,
        experiment_arm
),

campaign_metrics AS (
    SELECT
        campaign_id,

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
        )::numeric(18, 2) AS control_revenue,

        MAX(conversion_rate) FILTER (
            WHERE experiment_arm = 'treatment'
        )::numeric(18, 8) AS treatment_conversion_rate,

        MAX(conversion_rate) FILTER (
            WHERE experiment_arm = 'control'
        )::numeric(18, 8) AS control_conversion_rate,

        MAX(orders_per_member) FILTER (
            WHERE experiment_arm = 'treatment'
        )::numeric(18, 8) AS treatment_orders_per_member,

        MAX(orders_per_member) FILTER (
            WHERE experiment_arm = 'control'
        )::numeric(18, 8) AS control_orders_per_member,

        MAX(revenue_per_member) FILTER (
            WHERE experiment_arm = 'treatment'
        )::numeric(18, 8) AS treatment_revenue_per_member,

        MAX(revenue_per_member) FILTER (
            WHERE experiment_arm = 'control'
        )::numeric(18, 8) AS control_revenue_per_member

    FROM arm_metrics
    GROUP BY campaign_id
)

SELECT
    campaign_id,
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

    (treatment_orders_per_member - control_orders_per_member)::numeric(18, 8)
        AS incremental_orders_per_member,

    (treatment_revenue_per_member - control_revenue_per_member)::numeric(18, 8)
        AS incremental_revenue_per_member,

    CASE
        WHEN treatment_member_count > 0
             AND control_member_count > 0
        THEN (
            treatment_member_count::numeric
            * (treatment_orders_per_member - control_orders_per_member)
        )::numeric(18, 4)
        ELSE NULL
    END AS incremental_orders,

    CASE
        WHEN treatment_member_count > 0
             AND control_member_count > 0
        THEN (
            treatment_member_count::numeric
            * (treatment_revenue_per_member - control_revenue_per_member)
        )::numeric(18, 2)
        ELSE NULL
    END AS incremental_revenue

FROM campaign_metrics;

