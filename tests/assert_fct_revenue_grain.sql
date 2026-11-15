-- tests/assert_fct_revenue_grain.sql
-- Grain test: fct_revenue must have one row per account_id + revenue_month.
-- If this query returns any rows, the test fails.

select
    account_id,
    revenue_month,
    count(*) as row_count
from {{ ref('fct_revenue') }}
group by account_id, revenue_month
having count(*) > 1
