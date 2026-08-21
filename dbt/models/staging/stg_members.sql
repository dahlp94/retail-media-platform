{{ config(alias='stg_members') }}

select
    member_id::bigint as member_id,
    retailer_id::integer as retailer_id,
    audience_segment_id::integer as audience_segment_id,
    primary_geo_id::integer as primary_geo_id,
    signup_date::date as signup_date,
    nullif(btrim(outcome_currency), '')::text as outcome_currency
from {{ source('raw', 'members') }}
