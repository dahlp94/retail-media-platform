-- Campaign efficiency tiers based on conversion lift and incremental revenue.

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
        WHEN absolute_lift IS NULL
             OR incremental_revenue IS NULL
        THEN NULL

        WHEN absolute_lift >= 0.045
             AND incremental_revenue >= 3500
        THEN 'high_impact'

        WHEN absolute_lift >= 0.025
             AND incremental_revenue >= 2000
        THEN 'moderate'

        WHEN absolute_lift >= 0.005
             AND incremental_revenue > 0
        THEN 'low_impact'

        ELSE 'inefficient'
    END AS efficiency_flag

FROM marts.experiment_lift_metrics;
