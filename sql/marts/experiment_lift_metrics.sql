-- Marts: campaign-level experiment lift (treatment vs control)
-- Inputs:
--   - staging.stg_experiment_assignment
--   - staging.stg_campaigns
--   - staging.stg_transactions
--
-- Grain:
--   - one row per campaign_id
--
-- Outcome definition:
--   Members are the experiment units assigned to a campaign.
--   Outcomes are measured using all observed transactions for assigned members
--   during the campaign window [start_date, end_date], regardless of source_campaign_id.
--
-- Design principle:
--   This table measures experimental incrementality, not attribution.
--   Attribution fields such as source_campaign_id may be used in descriptive
--   campaign reporting elsewhere, but they are intentionally excluded here
--   from the outcome definition.
--
-- Key metrics:
--   - treatment/control conversion rate
--   - absolute lift
--   - relative lift
--   - incremental orders
--   - incremental revenue
--
-- Incremental totals:
--   incremental_orders
--       = n_treatment * (orders_per_member_treatment - orders_per_member_control)
--
--   incremental_revenue
--       = n_treatment * (revenue_per_member_treatment - revenue_per_member_control)

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
        COUNT(t.transaction_id)::bigint AS order_cnt,
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

arm_agg AS (
    SELECT
        campaign_id,
        experiment_arm,
        COUNT(*)::bigint AS n_members,
        SUM(is_converter)::bigint AS converters,
        COALESCE(SUM(order_cnt), 0)::bigint AS total_orders,
        COALESCE(SUM(revenue_usd), 0)::numeric(18, 2) AS total_revenue,
        AVG(is_converter::numeric)::numeric(18, 8) AS conversion_rate,
        AVG(order_cnt::numeric)::numeric(18, 8) AS orders_per_member,
        AVG(revenue_usd)::numeric(18, 8) AS revenue_per_member
    FROM member_outcomes
    GROUP BY campaign_id, experiment_arm
),

by_campaign AS (
    SELECT
        campaign_id,

        COALESCE(MAX(CASE WHEN experiment_arm = 'treatment' THEN n_members END), 0)::bigint
            AS treatment_member_count,
        COALESCE(MAX(CASE WHEN experiment_arm = 'control' THEN n_members END), 0)::bigint
            AS control_member_count,

        COALESCE(MAX(CASE WHEN experiment_arm = 'treatment' THEN converters END), 0)::bigint
            AS treatment_converters,
        COALESCE(MAX(CASE WHEN experiment_arm = 'control' THEN converters END), 0)::bigint
            AS control_converters,

        COALESCE(MAX(CASE WHEN experiment_arm = 'treatment' THEN total_orders END), 0)::bigint
            AS treatment_orders,
        COALESCE(MAX(CASE WHEN experiment_arm = 'control' THEN total_orders END), 0)::bigint
            AS control_orders,

        COALESCE(MAX(CASE WHEN experiment_arm = 'treatment' THEN total_revenue END), 0)::numeric(18, 2)
            AS treatment_revenue,
        COALESCE(MAX(CASE WHEN experiment_arm = 'control' THEN total_revenue END), 0)::numeric(18, 2)
            AS control_revenue,

        MAX(CASE WHEN experiment_arm = 'treatment' THEN conversion_rate END)::numeric(18, 8)
            AS treatment_conversion_rate,
        MAX(CASE WHEN experiment_arm = 'control' THEN conversion_rate END)::numeric(18, 8)
            AS control_conversion_rate,

        MAX(CASE WHEN experiment_arm = 'treatment' THEN orders_per_member END)::numeric(18, 8)
            AS treatment_orders_per_member,
        MAX(CASE WHEN experiment_arm = 'control' THEN orders_per_member END)::numeric(18, 8)
            AS control_orders_per_member,

        MAX(CASE WHEN experiment_arm = 'treatment' THEN revenue_per_member END)::numeric(18, 8)
            AS treatment_revenue_per_member,
        MAX(CASE WHEN experiment_arm = 'control' THEN revenue_per_member END)::numeric(18, 8)
            AS control_revenue_per_member
    FROM arm_agg
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

    CASE
        WHEN control_conversion_rate IS NULL OR control_conversion_rate = 0 THEN NULL
        ELSE (
            (treatment_conversion_rate - control_conversion_rate)
            / control_conversion_rate
        )::numeric(18, 8)
    END AS relative_lift,

    (treatment_orders_per_member - control_orders_per_member)::numeric(18, 8)
        AS incremental_orders_per_member,

    (treatment_revenue_per_member - control_revenue_per_member)::numeric(18, 8)
        AS incremental_revenue_per_member,

    CASE
        WHEN treatment_member_count > 0 AND control_member_count > 0 THEN
            (
                treatment_member_count::numeric
                * (treatment_orders_per_member - control_orders_per_member)
            )::numeric(18, 4)
        ELSE NULL
    END AS incremental_orders,

    CASE
        WHEN treatment_member_count > 0 AND control_member_count > 0 THEN
            (
                treatment_member_count::numeric
                * (treatment_revenue_per_member - control_revenue_per_member)
            )::numeric(18, 2)
        ELSE NULL
    END AS incremental_revenue
FROM by_campaign;

COMMENT ON TABLE marts.experiment_lift_metrics IS
    'Campaign-level experimental lift table using treatment/control assignment and all member transactions during the campaign window, independent of attribution tagging.';

COMMENT ON COLUMN marts.experiment_lift_metrics.campaign_id IS
    'Campaign identifier from experiment assignment and campaign metadata.';

COMMENT ON COLUMN marts.experiment_lift_metrics.treatment_member_count IS
    'Assigned members in the treatment arm.';

COMMENT ON COLUMN marts.experiment_lift_metrics.control_member_count IS
    'Assigned members in the control arm.';

COMMENT ON COLUMN marts.experiment_lift_metrics.treatment_converters IS
    'Treatment-assigned members with at least one transaction during the campaign window.';

COMMENT ON COLUMN marts.experiment_lift_metrics.control_converters IS
    'Control-assigned members with at least one transaction during the campaign window.';

COMMENT ON COLUMN marts.experiment_lift_metrics.treatment_conversion_rate IS
    'Share of treatment-assigned members with at least one transaction during the campaign window.';

COMMENT ON COLUMN marts.experiment_lift_metrics.control_conversion_rate IS
    'Share of control-assigned members with at least one transaction during the campaign window.';

COMMENT ON COLUMN marts.experiment_lift_metrics.absolute_lift IS
    'Treatment conversion rate minus control conversion rate.';

COMMENT ON COLUMN marts.experiment_lift_metrics.relative_lift IS
    'Absolute lift divided by control conversion rate; NULL when control conversion rate is zero or undefined.';

COMMENT ON COLUMN marts.experiment_lift_metrics.incremental_orders_per_member IS
    'Difference in average orders per assigned member between treatment and control.';

COMMENT ON COLUMN marts.experiment_lift_metrics.incremental_revenue_per_member IS
    'Difference in average revenue per assigned member between treatment and control.';

COMMENT ON COLUMN marts.experiment_lift_metrics.incremental_orders IS
    'Estimated incremental orders in treatment relative to the control benchmark using per-member order differences.';

COMMENT ON COLUMN marts.experiment_lift_metrics.incremental_revenue IS
    'Estimated incremental revenue in treatment relative to the control benchmark using per-member revenue differences.';