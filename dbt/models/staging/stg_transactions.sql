{{ config(alias='stg_transactions') }}

select
    transaction_id::bigint as transaction_id,
    member_id::bigint as member_id,
    retailer_id::integer as retailer_id,
    audience_segment_id::integer as audience_segment_id,
    order_timestamp::timestamp as order_timestamp,
    order_value_usd::numeric(18, 2) as order_value_usd,
    nullif(btrim(outcome_currency), '')::text as outcome_currency,
    nullif(btrim(purchase_driver), '')::text as purchase_driver,
    case
        when nullif(btrim(source_campaign_id), '') is null then null
        else nullif(btrim(source_campaign_id), '')::bigint
    end as source_campaign_id
from {{ source('raw', 'transactions') }}
