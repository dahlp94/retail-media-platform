{% test valid_date_window(model, start_column, end_column) %}
    select *
    from {{ model }}
    where {{ start_column }} is not null
      and {{ end_column }} is not null
      and {{ start_column }} > {{ end_column }}
{% endtest %}
