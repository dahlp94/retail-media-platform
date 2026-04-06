-- Staging: campaign-level experiment / treatment assignment (user-level arms)
-- Source: raw.campaign_experiment_assignments (TEXT columns from CSV load)
-- Assumes columns: campaign_id, member_id, experiment_arm, assignment_unit,
--   assignment_method, holdout_fraction (see data/synthetic/campaign_experiment_assignments.csv).

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

COMMENT ON TABLE staging.stg_experiment_assignment IS
    'Treatment assignment for incrementality and experiment analysis (join on campaign_id + member_id).';
