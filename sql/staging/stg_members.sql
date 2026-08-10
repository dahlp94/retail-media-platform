-- Staging: members (shopper / analysis unit dimension)

CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.stg_members;

CREATE TABLE staging.stg_members AS
SELECT
    member_id::bigint AS member_id,
    retailer_id::integer AS retailer_id,
    audience_segment_id::integer AS audience_segment_id,
    primary_geo_id::integer AS primary_geo_id,
    signup_date::date AS signup_date,
    NULLIF(btrim(outcome_currency), '')::text AS outcome_currency
FROM raw.members;
