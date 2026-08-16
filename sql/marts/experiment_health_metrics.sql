-- Campaign-level experiment-integrity diagnostics.
-- Counts and leakage only. SRM p-values, standardized differences, and
-- overall health status are attached by scripts/run_experiment_decisions.py.

CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.experiment_health_metrics;

CREATE TABLE marts.experiment_health_metrics AS

WITH assignments AS (
    SELECT
        campaign_id,
        member_id,
        experiment_arm,
        holdout_fraction
    FROM staging.stg_experiment_assignment
    WHERE experiment_arm IN ('treatment', 'control')
),

assignment_duplicates AS (
    SELECT
        campaign_id,
        member_id,
        COUNT(*)::bigint AS assignment_row_count
    FROM assignments
    GROUP BY
        campaign_id,
        member_id
),

duplicate_summary AS (
    SELECT
        campaign_id,
        COUNT(*) FILTER (WHERE assignment_row_count > 1)::bigint
            AS duplicate_assignment_pair_count,
        COALESCE(
            SUM(assignment_row_count - 1) FILTER (WHERE assignment_row_count > 1),
            0
        )::bigint AS extra_assignment_row_count
    FROM assignment_duplicates
    GROUP BY campaign_id
),

assignment_counts AS (
    SELECT
        campaign_id,
        COUNT(*)::bigint AS assigned_member_count,
        COUNT(*) FILTER (WHERE experiment_arm = 'treatment')::bigint
            AS treatment_member_count,
        COUNT(*) FILTER (WHERE experiment_arm = 'control')::bigint
            AS control_member_count,
        MAX(holdout_fraction)::numeric(10, 6) AS intended_control_share
    FROM assignments
    GROUP BY campaign_id
),

control_exposure AS (
    SELECT
        a.campaign_id,
        COUNT(DISTINCT a.member_id) FILTER (
            WHERE e.event_type = 'impression'
        )::bigint AS control_members_with_impressions,
        COUNT(*) FILTER (WHERE e.event_type = 'impression')::bigint
            AS control_impressions,
        COUNT(DISTINCT a.member_id) FILTER (
            WHERE e.event_type = 'click'
        )::bigint AS control_members_with_clicks,
        COUNT(*) FILTER (WHERE e.event_type = 'click')::bigint
            AS control_clicks
    FROM assignments AS a
    LEFT JOIN staging.stg_ad_events AS e
        ON e.campaign_id = a.campaign_id
        AND e.member_id = a.member_id
    WHERE a.experiment_arm = 'control'
    GROUP BY a.campaign_id
),

outcome_counts AS (
    SELECT
        campaign_id,
        COUNT(*)::bigint AS member_outcome_count
    FROM marts.experiment_member_outcomes
    GROUP BY campaign_id
),

preperiod_purchasers AS (
    SELECT DISTINCT
        a.campaign_id,
        a.member_id
    FROM assignments AS a
    INNER JOIN staging.stg_campaigns AS c
        ON c.campaign_id = a.campaign_id
    INNER JOIN staging.stg_transactions AS t
        ON t.member_id = a.member_id
        AND t.order_timestamp::date < c.start_date
),

preperiod AS (
    SELECT
        a.campaign_id,
        AVG(
            CASE
                WHEN a.experiment_arm = 'treatment'
                THEN CASE WHEN p.member_id IS NOT NULL THEN 1.0 ELSE 0.0 END
            END
        )::numeric(18, 8) AS treatment_preperiod_conversion_rate,
        AVG(
            CASE
                WHEN a.experiment_arm = 'control'
                THEN CASE WHEN p.member_id IS NOT NULL THEN 1.0 ELSE 0.0 END
            END
        )::numeric(18, 8) AS control_preperiod_conversion_rate
    FROM assignments AS a
    LEFT JOIN preperiod_purchasers AS p
        ON p.campaign_id = a.campaign_id
        AND p.member_id = a.member_id
    GROUP BY a.campaign_id
),

tenure AS (
    SELECT
        a.campaign_id,
        AVG((c.start_date - m.signup_date)) FILTER (
            WHERE a.experiment_arm = 'treatment'
        )::numeric(18, 4) AS treatment_mean_signup_tenure_days,
        AVG((c.start_date - m.signup_date)) FILTER (
            WHERE a.experiment_arm = 'control'
        )::numeric(18, 4) AS control_mean_signup_tenure_days,
        STDDEV_SAMP((c.start_date - m.signup_date)) FILTER (
            WHERE a.experiment_arm = 'treatment'
        )::numeric(18, 4) AS treatment_sd_signup_tenure_days,
        STDDEV_SAMP((c.start_date - m.signup_date)) FILTER (
            WHERE a.experiment_arm = 'control'
        )::numeric(18, 4) AS control_sd_signup_tenure_days
    FROM assignments AS a
    INNER JOIN staging.stg_campaigns AS c
        ON c.campaign_id = a.campaign_id
    LEFT JOIN staging.stg_members AS m
        ON m.member_id = a.member_id
    GROUP BY a.campaign_id
)

SELECT
    ac.campaign_id,
    ac.treatment_member_count,
    ac.control_member_count,
    ac.assigned_member_count,
    (
        ac.treatment_member_count::numeric
        / NULLIF(ac.assigned_member_count, 0)
    )::numeric(18, 8) AS treatment_share,
    (
        ac.control_member_count::numeric
        / NULLIF(ac.assigned_member_count, 0)
    )::numeric(18, 8) AS control_share,
    ac.intended_control_share,
    (1.0 - ac.intended_control_share)::numeric(10, 6) AS intended_treatment_share,

    (ac.assigned_member_count::numeric * (1.0 - ac.intended_control_share))
        ::numeric(18, 4) AS expected_treatment_count,
    (ac.assigned_member_count::numeric * ac.intended_control_share)
        ::numeric(18, 4) AS expected_control_count,

    COALESCE(ce.control_members_with_impressions, 0)::bigint
        AS control_members_with_impressions,
    COALESCE(ce.control_impressions, 0)::bigint AS control_impressions,
    COALESCE(ce.control_members_with_clicks, 0)::bigint
        AS control_members_with_clicks,
    COALESCE(ce.control_clicks, 0)::bigint AS control_clicks,

    COALESCE(ds.duplicate_assignment_pair_count, 0)::bigint
        AS duplicate_assignment_pair_count,
    COALESCE(ds.extra_assignment_row_count, 0)::bigint
        AS extra_assignment_row_count,

    COALESCE(oc.member_outcome_count, 0)::bigint AS member_outcome_count,
    (
        ac.assigned_member_count - COALESCE(oc.member_outcome_count, 0)
    )::bigint AS missing_member_outcome_count,
    (
        COALESCE(oc.member_outcome_count, 0)::numeric
        / NULLIF(ac.assigned_member_count, 0)
    )::numeric(18, 8) AS outcome_completeness_rate,

    pp.treatment_preperiod_conversion_rate,
    pp.control_preperiod_conversion_rate,
    tn.treatment_mean_signup_tenure_days,
    tn.control_mean_signup_tenure_days,
    tn.treatment_sd_signup_tenure_days,
    tn.control_sd_signup_tenure_days

FROM assignment_counts AS ac
LEFT JOIN control_exposure AS ce
    ON ce.campaign_id = ac.campaign_id
LEFT JOIN duplicate_summary AS ds
    ON ds.campaign_id = ac.campaign_id
LEFT JOIN outcome_counts AS oc
    ON oc.campaign_id = ac.campaign_id
LEFT JOIN preperiod AS pp
    ON pp.campaign_id = ac.campaign_id
LEFT JOIN tenure AS tn
    ON tn.campaign_id = ac.campaign_id;
