{{ config(alias='campaign_efficiency_flags') }}

-- Legacy point-estimate efficiency tiers used by the simple recommendation
-- CSV. Not the Stage 4 health-aware decision table.

select
    campaign_id,
    treatment_conversion_rate,
    control_conversion_rate,
    absolute_lift,
    incremental_revenue,
    case
        when absolute_lift is null
             or incremental_revenue is null
        then null
        when absolute_lift >= 0.045
             and incremental_revenue >= 3500
        then 'high_impact'
        when absolute_lift >= 0.025
             and incremental_revenue >= 2000
        then 'moderate'
        when absolute_lift >= 0.005
             and incremental_revenue > 0
        then 'low_impact'
        else 'inefficient'
    end as efficiency_flag
from {{ ref('experiment_lift_metrics') }}
