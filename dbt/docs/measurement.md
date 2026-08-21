{% docs incrementality_vs_attribution %}

# Attribution versus incrementality

These are different questions and different warehouse lineages.

## Attribution

Which campaign receives **credit** for a purchase?

Implemented with `source_campaign_id` on transactions. Downstream models:

- `campaign_base_metrics`
- `campaign_spend_metrics` (attributed ROAS)

## Incrementality

Did advertising **cause** additional purchasing relative to a randomized holdout?

Implemented from randomized `campaign_id × member_id` assignment plus **all** purchases in the campaign date window. Downstream models:

- `int_experiment_assigned_population`
- `experiment_member_outcomes`
- `experiment_lift_metrics`
- `segment_performance_metrics`
- `experiment_health_metrics`

Incrementality models ignore `source_campaign_id`, impression/click status, and `purchase_driver` when defining outcomes. Zero-purchase members stay in the denominator.

iROAS is incremental revenue / spend and is computed in Python. It is not attributed ROAS.

{% enddocs %}
