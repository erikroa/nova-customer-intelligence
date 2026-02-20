
{% macro classify_segment(employee_count_col, plan_tier_col) %}
  case
    when {{ plan_tier_col }} = 'enterprise' or {{ employee_count_col }} > 500
      then 'enterprise'
    when {{ plan_tier_col }} = 'growth' or {{ employee_count_col }} > 50
      then 'mid_market'
    else 'smb'
  end
{% endmacro %}
