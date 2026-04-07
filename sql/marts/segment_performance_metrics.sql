-- Marts: segment-level experiment lift (treatment vs control) for heterogeneous effects
-- Inputs: staging.stg_experiment_assignment, staging.stg_transactions, staging.stg_campaigns,
--         staging.stg_members
-- Grain: one row per (campaign_id, audience_segment_id, primary_geo_id) with at least one
--   treatment or control assignment in that segment cell
--
-- Segment dimensions (see header note below):
--   audience_segment_id — platform audience tier (aligns with campaign targeting and simulation);
--   primary_geo_id      — shopper primary geography for geo-level response differences.
--
-- Outcome, lift, and incremental formulas match marts.experiment_lift_metrics and
-- docs/experiment_design.md / docs/kpi_definitions.md (campaign-attributed orders in window).

CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.segment_performance_metrics;

CREATE TABLE marts.segment_performance_metrics AS
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
        m.audience_segment_id,
        m.primary_geo_id,
        a.member_id,
        a.experiment_arm,
        COALESCE(b.order_cnt, 0)::bigint AS order_cnt,
        COALESCE(b.revenue_usd, 0)::numeric(18, 2) AS revenue_usd,
        CASE
            WHEN COALESCE(b.order_cnt, 0) > 0 THEN 1
            ELSE 0
        END::smallint AS is_converter
    FROM staging.stg_experiment_assignment AS a
    LEFT JOIN staging.stg_members AS m ON m.member_id = a.member_id
    LEFT JOIN attributed_by_member AS b
        ON b.member_id = a.member_id
        AND b.campaign_id = a.campaign_id
    WHERE a.experiment_arm IN ('treatment', 'control')
),
arm_agg AS (
    SELECT
        campaign_id,
        audience_segment_id,
        primary_geo_id,
        experiment_arm,
        COUNT(*)::bigint AS n_members,
        SUM(is_converter)::bigint AS converters,
        SUM(order_cnt)::bigint AS total_orders,
        SUM(revenue_usd)::numeric(18, 2) AS total_revenue
    FROM member_level
    GROUP BY
        campaign_id,
        audience_segment_id,
        primary_geo_id,
        experiment_arm
),
by_segment AS (
    SELECT
        campaign_id,
        audience_segment_id,
        primary_geo_id,
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
    GROUP BY
        campaign_id,
        audience_segment_id,
        primary_geo_id
),
computed AS (
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
        (treatment_converters::numeric / NULLIF(treatment_member_count, 0))::numeric(18, 8)
            AS treatment_conversion_rate,
        (control_converters::numeric / NULLIF(control_member_count, 0))::numeric(18, 8)
            AS control_conversion_rate
    FROM by_segment
)
SELECT
    campaign_id,
    audience_segment_id,
    primary_geo_id,
    treatment_member_count,
    control_member_count,
    treatment_conversion_rate,
    control_conversion_rate,
    (treatment_conversion_rate - control_conversion_rate)::numeric(18, 8) AS absolute_lift,
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

COMMENT ON TABLE marts.segment_performance_metrics IS
    'Campaign × segment experiment readout: attributed CVR by arm, lift, and RCT-scaled incremental '
    'orders/revenue within each audience_segment_id × primary_geo_id cell (same outcome window as '
    'experiment_lift_metrics).';

COMMENT ON COLUMN marts.segment_performance_metrics.campaign_id IS 'Retail media campaign identifier.';
COMMENT ON COLUMN marts.segment_performance_metrics.audience_segment_id IS
    'Member audience tier from staging.stg_members (NULL if member dimension row missing).';
COMMENT ON COLUMN marts.segment_performance_metrics.primary_geo_id IS
    'Member primary geography id from staging.stg_members (NULL if missing).';
COMMENT ON COLUMN marts.segment_performance_metrics.treatment_member_count IS
    'Treatment-assigned members in this segment cell.';
COMMENT ON COLUMN marts.segment_performance_metrics.control_member_count IS
    'Control-assigned members in this segment cell.';
COMMENT ON COLUMN marts.segment_performance_metrics.treatment_conversion_rate IS
    'Share of treatment members in the cell with ≥1 attributed order in the campaign window.';
COMMENT ON COLUMN marts.segment_performance_metrics.control_conversion_rate IS
    'Share of control members in the cell with ≥1 attributed order in the campaign window.';
COMMENT ON COLUMN marts.segment_performance_metrics.absolute_lift IS
    'Treatment minus control conversion rate within the segment cell.';
COMMENT ON COLUMN marts.segment_performance_metrics.relative_lift IS
    'Absolute lift divided by control conversion rate; NULL when control rate is 0 or undefined.';
COMMENT ON COLUMN marts.segment_performance_metrics.incremental_orders IS
    'orders_T - n_T*(orders_C/n_C) within the cell; NULL if either arm count is zero.';
COMMENT ON COLUMN marts.segment_performance_metrics.incremental_revenue IS
    'revenue_T - n_T*(revenue_C/n_C) within the cell; NULL if either arm count is zero.';
