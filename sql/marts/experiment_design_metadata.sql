-- One experiment per campaign. Dates come from campaigns.
-- planned_power and planned_mde are NULL: the simulator never stored a
-- pre-registered MDE or power calculation. Do not invent those values.

CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.experiment_design_metadata;

CREATE TABLE marts.experiment_design_metadata AS
SELECT
    c.campaign_id,
    c.campaign_id AS experiment_id,
    'Advertising assignment increases campaign-window purchasing relative to a randomized holdout.'::text
        AS hypothesis,
    'incremental_revenue'::text AS primary_metric,
    'spend_usd,roas'::text AS guardrail_metrics,
    0.05::numeric(6, 4) AS alpha,
    NULL::numeric AS planned_power,
    NULL::numeric AS planned_mde,
    c.start_date,
    c.end_date,
    'completed'::text AS status
FROM staging.stg_campaigns AS c;
