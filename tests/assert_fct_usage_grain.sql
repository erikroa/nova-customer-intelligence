-- tests/assert_fct_usage_grain.sql
-- Grain test: fct_usage must have one row per account_id + event_date.
-- If this query returns any rows, the test fails.

select
    account_id,
    event_date,
    count(*) as row_count
from {{ ref('fct_usage') }}
group by account_id, event_date
having count(*) > 1
