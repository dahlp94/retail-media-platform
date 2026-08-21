{{ config(alias='stg_campaigns') }}

select
    campaign_id::bigint as campaign_id,
    nullif(btrim(campaign_name), '')::text as campaign_name,
    advertiser_id::integer as advertiser_id,
    retailer_id::integer as retailer_id,
    nullif(btrim(channel), '')::text as channel,
    nullif(btrim(pricing_model), '')::text as pricing_model,
    bid_price_usd::numeric(14, 4) as bid_price_usd,
    budget_usd::numeric(18, 2) as budget_usd,
    daily_budget_usd::numeric(18, 2) as daily_budget_usd,
    target_audience_segment_id::integer as target_audience_segment_id,
    target_geo_id::integer as target_geo_id,
    start_date::date as start_date,
    end_date::date as end_date
from {{ source('raw', 'campaigns') }}
