{% test test_not_null_columns(model, columns, condition='AND', where_condition=None) %}
  {% set condition = condition | upper %}
  {% if condition not in ['AND', 'OR'] %}
    {% do exceptions.raise_compiler_error("Condition must be either 'AND' or 'OR'") %}
  {% endif %}
  with null_counts as (
    select
      {% for column in columns %}
        sum(case when {{ column }} is null then 1 else 0 end) as {{ column }}_null_count
        {% if not loop.last %}, {% endif %}
      {% endfor %}
    from {{ model }}
    {% if where_condition is not none and where_condition|length > 0 %}
      where {{ where_condition }}
    {% endif %}
  )
select
    'column_name' as column_name,
    {% for column in columns %}
    case when {{ column }}_null_count > 0 then '{{ column }}' else null end as {{ column }}_result
      {% if not loop.last %}, {% endif %}
    {% endfor %}
  from null_counts
  where
    {% for column in columns %}
      {{ column }}_null_count > 0
      {% if not loop.last %}
        {{ condition }}
      {% endif %}
    {% endfor %}
{% endtest %}