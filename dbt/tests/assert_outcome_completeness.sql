-- Every assigned campaign-member appears exactly once in ITT outcomes,
-- including members with zero orders and zero revenue.

with assigned as (
    select distinct
        campaign_id,
        member_id
    from {{ ref('stg_experiment_assignment') }}
    where experiment_arm in ('treatment', 'control')
),

outcomes as (
    select
        campaign_id,
        member_id,
        count(*) as outcome_row_count
    from {{ ref('experiment_member_outcomes') }}
    group by
        campaign_id,
        member_id
)

select
    coalesce(a.campaign_id, o.campaign_id) as campaign_id,
    coalesce(a.member_id, o.member_id) as member_id,
    a.member_id as assigned_member_id,
    o.member_id as outcome_member_id,
    coalesce(o.outcome_row_count, 0) as outcome_row_count
from assigned as a
full outer join outcomes as o
    on a.campaign_id = o.campaign_id
    and a.member_id = o.member_id
where a.member_id is null
   or o.member_id is null
   or o.outcome_row_count <> 1
