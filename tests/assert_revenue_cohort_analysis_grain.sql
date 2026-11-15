-- tests/assert_revenue_cohort_analysis_grain.sql
-- Grain test: revenue_cohort_analysis must have one row per cohort_month + months_since_signup.
-- If this query returns any rows, the test fails.

select
    cohort_month,
    months_since_signup,
    count(*) as row_count
from {{ ref('revenue_cohort_analysis') }}
group by cohort_month, months_since_signup
having count(*) > 1
