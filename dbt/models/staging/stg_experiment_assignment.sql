{{ config(alias='stg_experiment_assignment') }}

select
    campaign_id::bigint as campaign_id,
    member_id::bigint as member_id,
    nullif(btrim(experiment_arm), '')::text as experiment_arm,
    nullif(btrim(assignment_unit), '')::text as assignment_unit,
    nullif(btrim(assignment_method), '')::text as assignment_method,
    holdout_fraction::numeric(10, 6) as holdout_fraction
from {{ source('raw', 'campaign_experiment_assignments') }}
