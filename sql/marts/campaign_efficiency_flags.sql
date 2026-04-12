-- Marts: campaign-level efficiency classification from experimental lift
-- Input:
--   - marts.experiment_lift_metrics
--
-- Grain:
--   - one row per campaign_id
--
-- efficiency_flag (absolute lift = treatment CVR minus control CVR):
--   high_impact  — lift >= 15 percentage points
--   moderate     — lift in [5%, 15%)
--   low_impact   — lift in [0%, 5%)
--   inefficient  — lift below zero (treatment underperforms control)

CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.campaign_efficiency_flags;

CREATE TABLE marts.campaign_efficiency_flags AS
SELECT
    campaign_id,
    treatment_conversion_rate,
    control_conversion_rate,
    absolute_lift,
    incremental_revenue,
    CASE
        WHEN absolute_lift IS NULL THEN NULL
        WHEN absolute_lift >= 0.15 THEN 'high_impact'
        WHEN absolute_lift >= 0.05 AND absolute_lift < 0.15 THEN 'moderate'
        WHEN absolute_lift >= 0 AND absolute_lift < 0.05 THEN 'low_impact'
        WHEN absolute_lift < 0 THEN 'inefficient'
        ELSE NULL
    END AS efficiency_flag
FROM marts.experiment_lift_metrics;

COMMENT ON TABLE marts.campaign_efficiency_flags IS
    'Campaign efficiency tier from absolute conversion lift vs fixed thresholds; includes CVRs and incremental revenue for context.';

COMMENT ON COLUMN marts.campaign_efficiency_flags.efficiency_flag IS
    'high_impact (>=15pp), moderate ([5pp,15pp)), low_impact ([0,5pp)), inefficient (<0); NULL when absolute_lift is NULL.';
