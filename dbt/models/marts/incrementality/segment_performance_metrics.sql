{{ config(alias='segment_performance_metrics') }}

-- Descriptive subgroup ITT metrics. Same campaign-window outcomes as
-- experiment_member_outcomes. Not CATE / uplift modeling.
-- Grain: one row per campaign × audience_segment_id × primary_geo_id.

with member_outcomes as (
    select
        o.campaign_id,
        m.audience_segment_id,
        m.primary_geo_id,
        o.member_id,
        o.experiment_arm,
        o.order_count,
        o.revenue_usd,
        o.is_converter
    from {{ ref('experiment_member_outcomes') }} as o
    left join {{ ref('stg_members') }} as m
        on m.member_id = o.member_id
),

arm_metrics as (
    select
        campaign_id,
        audience_segment_id,
        primary_geo_id,
        experiment_arm,
        count(*)::bigint as member_count,
        sum(is_converter)::bigint as converters,
        sum(order_count)::bigint as orders,
        sum(revenue_usd)::numeric(18, 2) as revenue
    from member_outcomes
    group by
        campaign_id,
        audience_segment_id,
        primary_geo_id,
        experiment_arm
),

segment_metrics as (
    select
        campaign_id,
        audience_segment_id,
        primary_geo_id,
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
        )::numeric(18, 2) as control_revenue
    from arm_metrics
    group by
        campaign_id,
        audience_segment_id,
        primary_geo_id
),

computed_metrics as (
    select
        *,
        (
            treatment_converters::numeric
            / nullif(treatment_member_count, 0)
        )::numeric(18, 8) as treatment_conversion_rate,
        (
            control_converters::numeric
            / nullif(control_member_count, 0)
        )::numeric(18, 8) as control_conversion_rate
    from segment_metrics
)

select
    campaign_id,
    audience_segment_id,
    primary_geo_id,
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
    case
        when treatment_member_count > 0
             and control_member_count > 0
        then (
            treatment_orders
            - treatment_member_count::numeric
              * (control_orders::numeric / control_member_count)
        )::numeric(18, 4)
        else null
    end as incremental_orders,
    case
        when treatment_member_count > 0
             and control_member_count > 0
        then (
            treatment_revenue
            - treatment_member_count::numeric
              * (control_revenue / control_member_count)
        )::numeric(18, 2)
        else null
    end as incremental_revenue
from computed_metrics
