{{ config(alias='experiment_design_metadata') }}

-- One experiment per campaign. planned_power and planned_mde are NULL
-- because the simulator never stored a pre-registered design calculation.
-- Holdout fraction lives on assignment rows / health metrics, not here.

select
    c.campaign_id,
    c.campaign_id as experiment_id,
    'Advertising assignment increases campaign-window purchasing relative to a randomized holdout.'::text
        as hypothesis,
    'incremental_revenue'::text as primary_metric,
    'spend_usd,roas'::text as guardrail_metrics,
    0.05::numeric(6, 4) as alpha,
    null::numeric as planned_power,
    null::numeric as planned_mde,
    c.start_date,
    c.end_date,
    'completed'::text as status
from {{ ref('stg_campaigns') }} as c
