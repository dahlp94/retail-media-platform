-- Health completeness input must show no missing assigned outcomes
-- after the ITT member-outcome mart is built.

select
    campaign_id,
    assigned_member_count,
    member_outcome_count,
    missing_member_outcome_count
from {{ ref('experiment_health_metrics') }}
where missing_member_outcome_count <> 0
