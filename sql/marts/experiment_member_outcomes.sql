-- Creates member-level outcomes for treatment and control groups.
-- Includes all purchases made during the campaign window.

CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.experiment_member_outcomes;

CREATE TABLE marts.experiment_member_outcomes AS
WITH experiment_population AS (
    SELECT
        a.campaign_id,
        a.member_id,
        a.experiment_arm,
        c.start_date,
        c.end_date
    FROM staging.stg_experiment_assignment AS a
    INNER JOIN staging.stg_campaigns AS c
        ON c.campaign_id = a.campaign_id
    WHERE a.experiment_arm IN ('treatment', 'control')
)
SELECT
    ep.campaign_id,
    ep.member_id,
    ep.experiment_arm,
    COUNT(t.transaction_id)::bigint AS order_count,
    COALESCE(SUM(t.order_value_usd), 0)::numeric(18, 2) AS revenue_usd,
    CASE
        WHEN COUNT(t.transaction_id) > 0 THEN 1
        ELSE 0
    END::smallint AS is_converter
FROM experiment_population AS ep
LEFT JOIN staging.stg_transactions AS t
    ON t.member_id = ep.member_id
    AND t.order_timestamp::date BETWEEN ep.start_date AND ep.end_date
GROUP BY
    ep.campaign_id,
    ep.member_id,
    ep.experiment_arm;
