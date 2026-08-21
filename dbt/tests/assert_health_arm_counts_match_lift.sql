-- Assignment member counts in health inputs must match ITT lift denominators.

select
    h.campaign_id,
    h.treatment_member_count as health_treatment_members,
    l.treatment_member_count as lift_treatment_members,
    h.control_member_count as health_control_members,
    l.control_member_count as lift_control_members
from {{ ref('experiment_health_metrics') }} as h
inner join {{ ref('experiment_lift_metrics') }} as l
    on l.campaign_id = h.campaign_id
where h.treatment_member_count <> l.treatment_member_count
   or h.control_member_count <> l.control_member_count
