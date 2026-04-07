-- Marts: campaign-level experiment lift (treatment vs control)
-- Inputs: staging.stg_experiment_assignment, staging.stg_transactions, staging.stg_campaigns
-- Grain: one row per campaign_id that has at least one treatment or control assignment
--
-- Outcome definition (aligned with experiment_design.md and KPI docs):
--   Members are the assigned experiment units per campaign. A "conversion" is having at least one
--   order with source_campaign_id = that campaign_id, with order_timestamp::date in the campaign's
--   [start_date, end_date] window. Baseline orders in the simulation carry no source_campaign_id,
--   so they do not count toward this campaign-attributed conversion—only orders linked to the
--   campaign do (consistent with attributed-order reporting elsewhere).
--
-- Lift:
--   absolute_lift  = treatment_conversion_rate - control_conversion_rate
--   relative_lift  = absolute_lift / control_conversion_rate  (NULL if control rate is 0 or undefined)
--
-- Incremental totals (RCT scaling from experiment_design.md; mean orders/revenue per assigned member):
--   incremental_orders  = total_orders_treatment  - n_treatment  * (total_orders_control  / n_control)
--   incremental_revenue = total_revenue_treatment - n_treatment * (total_revenue_control / n_control)
--   Both are NULL when n_control = 0 (no control benchmark) or n_treatment = 0.

CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.experiment_lift_metrics;

CREATE TABLE marts.experiment_lift_metrics AS
WITH
attributed_by_member AS (
    SELECT
        t.member_id,
        t.source_campaign_id AS campaign_id,
        COUNT(*)::bigint AS order_cnt,
        COALESCE(SUM(t.order_value_usd), 0)::numeric(18, 2) AS revenue_usd
    FROM staging.stg_transactions AS t
    INNER JOIN staging.stg_campaigns AS c ON c.campaign_id = t.source_campaign_id
    WHERE t.source_campaign_id IS NOT NULL
      AND t.order_timestamp::date BETWEEN c.start_date AND c.end_date
    GROUP BY t.member_id, t.source_campaign_id
),
member_level AS (
    SELECT
        a.campaign_id,
        a.member_id,
        a.experiment_arm,
        COALESCE(b.order_cnt, 0)::bigint AS order_cnt,
        COALESCE(b.revenue_usd, 0)::numeric(18, 2) AS revenue_usd,
        CASE
            WHEN COALESCE(b.order_cnt, 0) > 0 THEN 1
            ELSE 0
        END::smallint AS is_converter
    FROM staging.stg_experiment_assignment AS a
    LEFT JOIN attributed_by_member AS b
        ON b.member_id = a.member_id
        AND b.campaign_id = a.campaign_id
    WHERE a.experiment_arm IN ('treatment', 'control')
),
arm_agg AS (
    SELECT
        campaign_id,
        experiment_arm,
        COUNT(*)::bigint AS n_members,
        SUM(is_converter)::bigint AS converters,
        SUM(order_cnt)::bigint AS total_orders,
        SUM(revenue_usd)::numeric(18, 2) AS total_revenue
    FROM member_level
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
            AS control_revenue
    FROM arm_agg
    GROUP BY campaign_id
),
computed AS (
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
        (treatment_converters::numeric / NULLIF(treatment_member_count, 0))::numeric(18, 8)
            AS treatment_conversion_rate,
        (control_converters::numeric / NULLIF(control_member_count, 0))::numeric(18, 8)
            AS control_conversion_rate
    FROM by_campaign
)
SELECT
    campaign_id,
    treatment_member_count,
    control_member_count,
    treatment_conversion_rate,
    control_conversion_rate,
    (
        (treatment_conversion_rate - control_conversion_rate)::numeric(18, 8)
    ) AS absolute_lift,
    (
        (treatment_conversion_rate - control_conversion_rate)
        / NULLIF(control_conversion_rate, 0)
    )::numeric(18, 8) AS relative_lift,
    CASE
        WHEN treatment_member_count > 0
            AND control_member_count > 0
        THEN (
            treatment_orders::numeric
            - treatment_member_count::numeric * (control_orders::numeric / NULLIF(control_member_count::numeric, 0))
        )::numeric(18, 4)
        ELSE NULL
    END AS incremental_orders,
    CASE
        WHEN treatment_member_count > 0
            AND control_member_count > 0
        THEN (
            treatment_revenue
            - treatment_member_count::numeric * (control_revenue / NULLIF(control_member_count::numeric, 0))
        )::numeric(18, 2)
        ELSE NULL
    END AS incremental_revenue
FROM computed;

COMMENT ON TABLE marts.experiment_lift_metrics IS
    'Campaign-level randomized experiment readout: attributed conversion rates by arm, lift, and '
    'RCT-scaled incremental orders/revenue vs control (see file header for outcome and window rules).';

COMMENT ON COLUMN marts.experiment_lift_metrics.campaign_id IS 'Campaign identifier matching assignment and attributed transactions.';
COMMENT ON COLUMN marts.experiment_lift_metrics.treatment_member_count IS 'Assigned members in the treatment arm.';
COMMENT ON COLUMN marts.experiment_lift_metrics.control_member_count IS 'Assigned members in the control arm.';
COMMENT ON COLUMN marts.experiment_lift_metrics.treatment_conversion_rate IS
    'Share of treatment-assigned members with at least one attributed order in the campaign window.';
COMMENT ON COLUMN marts.experiment_lift_metrics.control_conversion_rate IS
    'Share of control-assigned members with at least one attributed order in the campaign window.';
COMMENT ON COLUMN marts.experiment_lift_metrics.absolute_lift IS
    'Treatment minus control conversion rate (NULL if either arm rate is undefined).';
COMMENT ON COLUMN marts.experiment_lift_metrics.relative_lift IS
    'Absolute lift divided by control conversion rate; NULL when control rate is 0 or undefined.';
COMMENT ON COLUMN marts.experiment_lift_metrics.incremental_orders IS
    'Scaled excess orders in treatment vs control mean orders per member: orders_T - n_T*(orders_C/n_C).';
COMMENT ON COLUMN marts.experiment_lift_metrics.incremental_revenue IS
    'Scaled excess revenue in treatment vs control mean revenue per member.';
