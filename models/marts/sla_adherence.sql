-- models/marts/sla_adherence.sql
-- Support SLA performance tracking.
-- Shows whether the support team is meeting response time targets
-- by priority level, segment, and time period.
-- Operations and CS leadership use this to staff and improve support.

with tickets_enriched as (
    select
        t.*,
        a.account_segment,
        a.plan_tier as account_plan_tier_dim,
        format_date('%Y-%m', t.created_date) as ticket_year_month
    from {{ ref('fct_tickets') }} t
    inner join {{ ref('dim_account') }} a on t.account_id = a.account_id
),

-- Monthly SLA summary by priority
by_priority_month as (
    select
        ticket_year_month,
        priority,
        count(*) as total_tickets,
        countif(status = 'resolved') as resolved_tickets,
        countif(is_sla_breach = true) as sla_breaches,
        countif(is_sla_breach = false and status = 'resolved') as within_sla,
        countif(status in ('open', 'escalated')) as unresolved_tickets,

        -- SLA adherence rate
        round(
            safe_divide(
                countif(is_sla_breach = false and status = 'resolved'),
                countif(status = 'resolved')
            ) * 100,
            1
        ) as sla_adherence_pct,

        -- Average resolution time
        round(avg(case when status = 'resolved' then resolution_hours end), 1) as avg_resolution_hours,

        -- Median resolution time (P50)
        round(
            approx_quantiles(
                case when status = 'resolved' then resolution_hours end,
                100
            )[offset(50)],
            1
        ) as median_resolution_hours,

        -- P90 resolution time
        round(
            approx_quantiles(
                case when status = 'resolved' then resolution_hours end,
                100
            )[offset(90)],
            1
        ) as p90_resolution_hours,

        -- Average CSAT
        round(avg(satisfaction_score), 2) as avg_csat

    from tickets_enriched
    group by 1, 2
),

-- SLA by segment (for segment-level reporting)
by_segment as (
    select
        ticket_year_month,
        account_segment,
        count(*) as total_tickets,
        countif(is_sla_breach = true) as sla_breaches,
        round(
            safe_divide(
                countif(is_sla_breach = false and status = 'resolved'),
                countif(status = 'resolved')
            ) * 100,
            1
        ) as sla_adherence_pct,
        round(avg(case when status = 'resolved' then resolution_hours end), 1) as avg_resolution_hours,
        round(avg(satisfaction_score), 2) as avg_csat
    from tickets_enriched
    group by 1, 2
),

-- Combine both views with a dimension label
final as (
    -- Priority-level view
    select
        ticket_year_month,
        'priority' as dimension_type,
        priority as dimension_value,
        total_tickets,
        resolved_tickets,
        sla_breaches,
        within_sla,
        unresolved_tickets,
        sla_adherence_pct,
        avg_resolution_hours,
        median_resolution_hours,
        p90_resolution_hours,
        avg_csat
    from by_priority_month

    union all

    -- Segment-level view
    select
        ticket_year_month,
        'segment' as dimension_type,
        account_segment as dimension_value,
        total_tickets,
        null as resolved_tickets,
        sla_breaches,
        null as within_sla,
        null as unresolved_tickets,
        sla_adherence_pct,
        avg_resolution_hours,
        null as median_resolution_hours,
        null as p90_resolution_hours,
        avg_csat
    from by_segment
)

select * from final
order by ticket_year_month, dimension_type, dimension_value
