{{ config(alias='campaign_spend_metrics') }}

-- Attributed ROAS and spend efficiency. Distinct from iROAS, which is
-- incremental_revenue / spend and is computed in Python.

select
    b.campaign_id,
    b.spend_usd,
    b.revenue_usd,
    b.orders,
    (b.revenue_usd / nullif(b.spend_usd, 0))::numeric(18, 6) as roas,
    (b.revenue_usd / nullif(b.orders, 0))::numeric(18, 4) as avg_revenue_per_order_usd,
    (b.spend_usd / nullif(b.impressions, 0))::numeric(18, 8) as avg_spend_per_impression_usd,
    (b.spend_usd / nullif(b.clicks, 0))::numeric(18, 6) as avg_spend_per_click_usd
from {{ ref('campaign_base_metrics') }} as b
