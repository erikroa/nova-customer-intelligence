
{% macro calculate_health_score(
    usage_score_col,
    support_score_col,
    revenue_score_col,
    engagement_score_col
) %}
  round(
    ({{ usage_score_col }} * 0.30) +
    ({{ support_score_col }} * 0.20) +
    ({{ revenue_score_col }} * 0.25) +
    ({{ engagement_score_col }} * 0.25),
    1
  )
{% endmacro %}
