{{ config(alias='campaign_base_metrics') }}

-- Attribution-oriented campaign delivery and credited outcomes.
-- Orders and revenue use source_campaign_id, not randomized assignment.

with ad_by_campaign as (
    select
        campaign_id,
        count(*) filter (where event_type = 'impression')::bigint as impressions,
        count(*) filter (where event_type = 'click')::bigint as clicks,
        coalesce(sum(cost), 0)::numeric(18, 6) as spend_usd
    from {{ ref('stg_ad_events') }}
    group by campaign_id
),

attributed_orders as (
    select
        source_campaign_id as campaign_id,
        count(*)::bigint as orders,
        coalesce(sum(order_value_usd), 0)::numeric(18, 2) as revenue_usd
    from {{ ref('stg_transactions') }}
    where source_campaign_id is not null
    group by source_campaign_id
)

select
    coalesce(a.campaign_id, o.campaign_id) as campaign_id,
    coalesce(a.impressions, 0)::bigint as impressions,
    coalesce(a.clicks, 0)::bigint as clicks,
    coalesce(a.spend_usd, 0)::numeric(18, 6) as spend_usd,
    coalesce(o.orders, 0)::bigint as orders,
    coalesce(o.revenue_usd, 0)::numeric(18, 2) as revenue_usd
from ad_by_campaign as a
full outer join attributed_orders as o
    on a.campaign_id = o.campaign_id
