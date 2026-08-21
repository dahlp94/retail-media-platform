{{ config(alias='experiment_lift_metrics') }}

-- Frozen v1 campaign-level ITT point estimates.
-- Grain: one row per campaign.
-- Uncertainty columns are NOT produced here. Python
-- (app/statistics/experiment_inference.py) attaches Wald and bootstrap
-- intervals after this table is built.

with arm_metrics as (
    select
        campaign_id,
        experiment_arm,
        count(*)::bigint as member_count,
        sum(is_converter)::bigint as converters,
        sum(order_count)::bigint as orders,
        sum(revenue_usd)::numeric(18, 2) as revenue,
        avg(is_converter::numeric)::numeric(18, 8) as conversion_rate,
        avg(order_count::numeric)::numeric(18, 8) as orders_per_member,
        avg(revenue_usd)::numeric(18, 8) as revenue_per_member
    from {{ ref('experiment_member_outcomes') }}
    group by
        campaign_id,
        experiment_arm
),

campaign_metrics as (
    select
        campaign_id,
        coalesce(
            max(member_count) filter (where experiment_arm = 'treatment'),
            0
        )::bigint as treatment_member_count,
        coalesce(
            max(member_count) filter (where experiment_arm = 'control'),
            0
        )::bigint as control_member_count,
        coalesce(
            max(converters) filter (where experiment_arm = 'treatment'),
            0
        )::bigint as treatment_converters,
        coalesce(
            max(converters) filter (where experiment_arm = 'control'),
            0
        )::bigint as control_converters,
        coalesce(
            max(orders) filter (where experiment_arm = 'treatment'),
            0
        )::bigint as treatment_orders,
        coalesce(
            max(orders) filter (where experiment_arm = 'control'),
            0
        )::bigint as control_orders,
        coalesce(
            max(revenue) filter (where experiment_arm = 'treatment'),
            0
        )::numeric(18, 2) as treatment_revenue,
        coalesce(
            max(revenue) filter (where experiment_arm = 'control'),
            0
        )::numeric(18, 2) as control_revenue,
        max(conversion_rate) filter (
            where experiment_arm = 'treatment'
        )::numeric(18, 8) as treatment_conversion_rate,
        max(conversion_rate) filter (
            where experiment_arm = 'control'
        )::numeric(18, 8) as control_conversion_rate,
        max(orders_per_member) filter (
            where experiment_arm = 'treatment'
        )::numeric(18, 8) as treatment_orders_per_member,
        max(orders_per_member) filter (
            where experiment_arm = 'control'
        )::numeric(18, 8) as control_orders_per_member,
        max(revenue_per_member) filter (
            where experiment_arm = 'treatment'
        )::numeric(18, 8) as treatment_revenue_per_member,
        max(revenue_per_member) filter (
            where experiment_arm = 'control'
        )::numeric(18, 8) as control_revenue_per_member
    from arm_metrics
    group by campaign_id
)

select
    campaign_id,
    treatment_member_count,
    control_member_count,
    treatment_converters,
    control_converters,
    treatment_orders,
    control_orders,
    treatment_revenue,
    control_revenue,
    treatment_conversion_rate,
    control_conversion_rate,
    (treatment_conversion_rate - control_conversion_rate)::numeric(18, 8)
        as absolute_lift,
    (
        (treatment_conversion_rate - control_conversion_rate)
        / nullif(control_conversion_rate, 0)
    )::numeric(18, 8) as relative_lift,
    (treatment_orders_per_member - control_orders_per_member)::numeric(18, 8)
        as incremental_orders_per_member,
    (treatment_revenue_per_member - control_revenue_per_member)::numeric(18, 8)
        as incremental_revenue_per_member,
    case
        when treatment_member_count > 0
             and control_member_count > 0
        then (
            treatment_member_count::numeric
            * (treatment_orders_per_member - control_orders_per_member)
        )::numeric(18, 4)
        else null
    end as incremental_orders,
    case
        when treatment_member_count > 0
             and control_member_count > 0
        then (
            treatment_member_count::numeric
            * (treatment_revenue_per_member - control_revenue_per_member)
        )::numeric(18, 2)
        else null
    end as incremental_revenue
from campaign_metrics
