-- Control-assigned campaign-members must have no advertising events.
-- Protects experimental isolation for the frozen v1 holdout.

select
    a.campaign_id,
    a.member_id,
    e.event_id,
    e.event_type
from {{ ref('stg_experiment_assignment') }} as a
inner join {{ ref('stg_ad_events') }} as e
    on e.campaign_id = a.campaign_id
    and e.member_id = a.member_id
where a.experiment_arm = 'control'
