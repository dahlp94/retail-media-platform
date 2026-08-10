-- Staging: campaign-level experiment / treatment assignment (user-level arms)

CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.stg_experiment_assignment;

CREATE TABLE staging.stg_experiment_assignment AS
SELECT
    campaign_id::bigint AS campaign_id,
    member_id::bigint AS member_id,
    NULLIF(btrim(experiment_arm), '')::text AS experiment_arm,
    NULLIF(btrim(assignment_unit), '')::text AS assignment_unit,
    NULLIF(btrim(assignment_method), '')::text AS assignment_method,
    holdout_fraction::numeric(10, 6) AS holdout_fraction
FROM raw.campaign_experiment_assignments;
