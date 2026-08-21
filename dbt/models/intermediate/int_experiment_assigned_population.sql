{{
    config(
        alias='int_experiment_assigned_population',
        materialized='table'
    )
}}

-- Eligible randomized population with campaign windows.
-- Does not join transactions. Causal outcomes are built downstream
-- from this population plus all in-window purchases.

select
    a.campaign_id,
    a.member_id,
    a.experiment_arm,
    a.holdout_fraction,
    c.start_date,
    c.end_date
from {{ ref('stg_experiment_assignment') }} as a
inner join {{ ref('stg_campaigns') }} as c
    on c.campaign_id = a.campaign_id
where a.experiment_arm in ('treatment', 'control')
