{{ config(alias='stg_ad_events') }}

select
    event_id::bigint as event_id,
    member_id::bigint as member_id,
    campaign_id::bigint as campaign_id,
    "timestamp"::timestamp as event_timestamp,
    nullif(btrim(event_type), '')::text as event_type,
    nullif(btrim(channel), '')::text as channel,
    cost::numeric(18, 6) as cost,
    advertiser_id::integer as advertiser_id,
    retailer_id::integer as retailer_id
from {{ source('raw', 'ad_events') }}
