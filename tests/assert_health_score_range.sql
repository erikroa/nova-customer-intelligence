-- tests/assert_health_score_range.sql
-- Custom data quality test: health scores must be between 0 and 100.
-- If this query returns any rows, the test fails.

select
    account_id,
    company_name,
    health_score
from {{ ref('customer_health_scores') }}
where health_score < 0 or health_score > 100
