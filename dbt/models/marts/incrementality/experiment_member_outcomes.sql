{{ config(alias='experiment_member_outcomes') }}

-- Frozen v1 ITT member outcomes.
-- Grain: one row per campaign × randomized member, including zeros.
-- Incrementality branch: all purchases in the campaign date window.
-- Ignores source_campaign_id, exposure, clicks, and purchase_driver.

select
    ep.campaign_id,
    ep.member_id,
    ep.experiment_arm,
    count(t.transaction_id)::bigint as order_count,
    coalesce(sum(t.order_value_usd), 0)::numeric(18, 2) as revenue_usd,
    case
        when count(t.transaction_id) > 0 then 1
        else 0
    end::smallint as is_converter
from {{ ref('int_experiment_assigned_population') }} as ep
left join {{ ref('stg_transactions') }} as t
    on t.member_id = ep.member_id
    and t.order_timestamp::date between ep.start_date and ep.end_date
group by
    ep.campaign_id,
    ep.member_id,
    ep.experiment_arm
