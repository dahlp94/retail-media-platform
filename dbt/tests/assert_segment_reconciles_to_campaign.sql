-- Subgroup ITT totals must reconcile to campaign-level lift metrics.
-- Revenue allows a 1-cent tolerance for numeric(18,2) aggregation noise.

with segment_totals as (
    select
        campaign_id,
        sum(treatment_member_count)::bigint as treatment_member_count,
        sum(control_member_count)::bigint as control_member_count,
        sum(treatment_converters)::bigint as treatment_converters,
        sum(control_converters)::bigint as control_converters,
        sum(treatment_orders)::bigint as treatment_orders,
        sum(control_orders)::bigint as control_orders,
        sum(treatment_revenue)::numeric(18, 2) as treatment_revenue,
        sum(control_revenue)::numeric(18, 2) as control_revenue
    from {{ ref('segment_performance_metrics') }}
    group by campaign_id
)

select
    s.campaign_id,
    s.treatment_member_count as segment_treatment_members,
    l.treatment_member_count as lift_treatment_members,
    s.control_member_count as segment_control_members,
    l.control_member_count as lift_control_members,
    s.treatment_revenue as segment_treatment_revenue,
    l.treatment_revenue as lift_treatment_revenue
from segment_totals as s
inner join {{ ref('experiment_lift_metrics') }} as l
    on l.campaign_id = s.campaign_id
where s.treatment_member_count <> l.treatment_member_count
   or s.control_member_count <> l.control_member_count
   or s.treatment_converters <> l.treatment_converters
   or s.control_converters <> l.control_converters
   or s.treatment_orders <> l.treatment_orders
   or s.control_orders <> l.control_orders
   or abs(s.treatment_revenue - l.treatment_revenue) > 0.05
   or abs(s.control_revenue - l.control_revenue) > 0.05
