-- tests/assert_mrr_non_negative.sql
-- Custom data quality test: MRR should never be negative.
-- If this query returns any rows, the test fails.

select
    account_id,
    revenue_month,
    mrr
from {{ ref('fct_revenue') }}
where mrr < 0
