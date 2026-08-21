{{ config(alias='experiment_health_metrics') }}

-- Campaign-level integrity *inputs* only.
-- SRM p-values, standardized differences, and PASS/WARN/FAIL are attached
-- by app/statistics/experiment_health.py after this table is built.

with assignments as (
    select
        campaign_id,
        member_id,
        experiment_arm,
        holdout_fraction
    from {{ ref('stg_experiment_assignment') }}
    where experiment_arm in ('treatment', 'control')
),

assignment_duplicates as (
    select
        campaign_id,
        member_id,
        count(*)::bigint as assignment_row_count
    from assignments
    group by
        campaign_id,
        member_id
),

duplicate_summary as (
    select
        campaign_id,
        count(*) filter (where assignment_row_count > 1)::bigint
            as duplicate_assignment_pair_count,
        coalesce(
            sum(assignment_row_count - 1) filter (where assignment_row_count > 1),
            0
        )::bigint as extra_assignment_row_count
    from assignment_duplicates
    group by campaign_id
),

assignment_counts as (
    select
        campaign_id,
        count(*)::bigint as assigned_member_count,
        count(*) filter (where experiment_arm = 'treatment')::bigint
            as treatment_member_count,
        count(*) filter (where experiment_arm = 'control')::bigint
            as control_member_count,
        max(holdout_fraction)::numeric(10, 6) as intended_control_share
    from assignments
    group by campaign_id
),

control_exposure as (
    select
        a.campaign_id,
        count(distinct a.member_id) filter (
            where e.event_type = 'impression'
        )::bigint as control_members_with_impressions,
        count(*) filter (where e.event_type = 'impression')::bigint
            as control_impressions,
        count(distinct a.member_id) filter (
            where e.event_type = 'click'
        )::bigint as control_members_with_clicks,
        count(*) filter (where e.event_type = 'click')::bigint
            as control_clicks
    from assignments as a
    left join {{ ref('stg_ad_events') }} as e
        on e.campaign_id = a.campaign_id
        and e.member_id = a.member_id
    where a.experiment_arm = 'control'
    group by a.campaign_id
),

outcome_counts as (
    select
        campaign_id,
        count(*)::bigint as member_outcome_count
    from {{ ref('experiment_member_outcomes') }}
    group by campaign_id
),

preperiod_purchasers as (
    select distinct
        a.campaign_id,
        a.member_id
    from assignments as a
    inner join {{ ref('stg_campaigns') }} as c
        on c.campaign_id = a.campaign_id
    inner join {{ ref('stg_transactions') }} as t
        on t.member_id = a.member_id
        and t.order_timestamp::date < c.start_date
),

preperiod as (
    select
        a.campaign_id,
        avg(
            case
                when a.experiment_arm = 'treatment'
                then case when p.member_id is not null then 1.0 else 0.0 end
            end
        )::numeric(18, 8) as treatment_preperiod_conversion_rate,
        avg(
            case
                when a.experiment_arm = 'control'
                then case when p.member_id is not null then 1.0 else 0.0 end
            end
        )::numeric(18, 8) as control_preperiod_conversion_rate
    from assignments as a
    left join preperiod_purchasers as p
        on p.campaign_id = a.campaign_id
        and p.member_id = a.member_id
    group by a.campaign_id
),

tenure as (
    select
        a.campaign_id,
        avg((c.start_date - m.signup_date)) filter (
            where a.experiment_arm = 'treatment'
        )::numeric(18, 4) as treatment_mean_signup_tenure_days,
        avg((c.start_date - m.signup_date)) filter (
            where a.experiment_arm = 'control'
        )::numeric(18, 4) as control_mean_signup_tenure_days,
        stddev_samp((c.start_date - m.signup_date)) filter (
            where a.experiment_arm = 'treatment'
        )::numeric(18, 4) as treatment_sd_signup_tenure_days,
        stddev_samp((c.start_date - m.signup_date)) filter (
            where a.experiment_arm = 'control'
        )::numeric(18, 4) as control_sd_signup_tenure_days
    from assignments as a
    inner join {{ ref('stg_campaigns') }} as c
        on c.campaign_id = a.campaign_id
    left join {{ ref('stg_members') }} as m
        on m.member_id = a.member_id
    group by a.campaign_id
)

select
    ac.campaign_id,
    ac.treatment_member_count,
    ac.control_member_count,
    ac.assigned_member_count,
    (
        ac.treatment_member_count::numeric
        / nullif(ac.assigned_member_count, 0)
    )::numeric(18, 8) as treatment_share,
    (
        ac.control_member_count::numeric
        / nullif(ac.assigned_member_count, 0)
    )::numeric(18, 8) as control_share,
    ac.intended_control_share,
    (1.0 - ac.intended_control_share)::numeric(10, 6) as intended_treatment_share,
    (ac.assigned_member_count::numeric * (1.0 - ac.intended_control_share))
        ::numeric(18, 4) as expected_treatment_count,
    (ac.assigned_member_count::numeric * ac.intended_control_share)
        ::numeric(18, 4) as expected_control_count,
    coalesce(ce.control_members_with_impressions, 0)::bigint
        as control_members_with_impressions,
    coalesce(ce.control_impressions, 0)::bigint as control_impressions,
    coalesce(ce.control_members_with_clicks, 0)::bigint
        as control_members_with_clicks,
    coalesce(ce.control_clicks, 0)::bigint as control_clicks,
    coalesce(ds.duplicate_assignment_pair_count, 0)::bigint
        as duplicate_assignment_pair_count,
    coalesce(ds.extra_assignment_row_count, 0)::bigint
        as extra_assignment_row_count,
    coalesce(oc.member_outcome_count, 0)::bigint as member_outcome_count,
    (
        ac.assigned_member_count - coalesce(oc.member_outcome_count, 0)
    )::bigint as missing_member_outcome_count,
    (
        coalesce(oc.member_outcome_count, 0)::numeric
        / nullif(ac.assigned_member_count, 0)
    )::numeric(18, 8) as outcome_completeness_rate,
    pp.treatment_preperiod_conversion_rate,
    pp.control_preperiod_conversion_rate,
    tn.treatment_mean_signup_tenure_days,
    tn.control_mean_signup_tenure_days,
    tn.treatment_sd_signup_tenure_days,
    tn.control_sd_signup_tenure_days
from assignment_counts as ac
left join control_exposure as ce
    on ce.campaign_id = ac.campaign_id
left join duplicate_summary as ds
    on ds.campaign_id = ac.campaign_id
left join outcome_counts as oc
    on oc.campaign_id = ac.campaign_id
left join preperiod as pp
    on pp.campaign_id = ac.campaign_id
left join tenure as tn
    on tn.campaign_id = ac.campaign_id
