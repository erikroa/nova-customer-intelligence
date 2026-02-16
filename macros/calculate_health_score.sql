-- macros/calculate_health_score.sql
-- Calculates a weighted composite health score from four signals.
-- Usage: {{ calculate_health_score('usage_score', 'support_score', 'revenue_score', 'engagement_score') }}

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
