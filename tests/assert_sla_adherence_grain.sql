-- tests/assert_sla_adherence_grain.sql
-- Grain test: sla_adherence must have one row per ticket_year_month + dimension_type + dimension_value.
-- If this query returns any rows, the test fails.

select
    ticket_year_month,
    dimension_type,
    dimension_value,
    count(*) as row_count
from {{ ref('sla_adherence') }}
group by ticket_year_month, dimension_type, dimension_value
having count(*) > 1
