
with health as (
    select * from {{ ref('customer_health_scores') }}
),

-- Usage trend
usage_trend as (
    select
        account_id,
        sum(case
            when event_date >= date_sub(date('2025-01-31'), interval 30 day)
            then total_events else 0
        end) as events_last_30d,
        sum(case
            when event_date >= date_sub(date('2025-01-31'), interval 60 day)
                and event_date < date_sub(date('2025-01-31'), interval 30 day)
            then total_events else 0
        end) as events_prev_30d
    from {{ ref('fct_usage') }}
    where event_date >= date_sub(date('2025-01-31'), interval 60 day)
    group by 1
),

-- Ticket escalations
escalation_signal as (
    select
        account_id,
        count(*) as escalated_tickets_90d,
        countif(priority in ('p1', 'p2')) as high_priority_tickets_90d
    from {{ ref('fct_tickets') }}
    where created_date >= date_sub(date('2025-01-31'), interval 90 day)
        and status = 'escalated'
    group by 1
),

-- Revenue trajectory
revenue_trend as (
    select
        account_id,
        -- Last 3 months of MRR
        array_agg(mrr order by revenue_month desc limit 3) as recent_mrr_values,
        countif(revenue_movement = 'contraction') as contraction_months_6m
    from {{ ref('fct_revenue') }}
    where revenue_month >= date_sub(date('2025-01-31'), interval 6 month)
    group by 1
),

final as (
    select
        h.account_id,
        h.company_name,
        h.account_segment,
        h.plan_tier,
        h.account_owner,
        h.region,
        h.current_mrr,
        h.current_arr,
        h.tenure_months,
        h.health_score,
        h.health_tier,

        -- Usage trend
        coalesce(ut.events_last_30d, 0) as events_last_30d,
        coalesce(ut.events_prev_30d, 0) as events_prev_30d,
        case
            when coalesce(ut.events_prev_30d, 0) = 0 then 0
            else round(
                (coalesce(ut.events_last_30d, 0) - ut.events_prev_30d)
                / ut.events_prev_30d * 100,
                1
            )
        end as usage_change_pct,

        -- Escalation signals
        coalesce(es.escalated_tickets_90d, 0) as escalated_tickets_90d,
        coalesce(es.high_priority_tickets_90d, 0) as high_priority_tickets_90d,

        -- Revenue signals
        coalesce(rt.contraction_months_6m, 0) as contraction_months_6m,

        -- Composite risk flags
        case when h.health_score < 40 then true else false end as is_low_health,
        case
            when coalesce(ut.events_prev_30d, 0) > 0
                and coalesce(ut.events_last_30d, 0) < ut.events_prev_30d * 0.5
            then true else false
        end as is_usage_declining,
        case
            when coalesce(es.escalated_tickets_90d, 0) >= 2
            then true else false
        end as has_escalations,
        case
            when coalesce(rt.contraction_months_6m, 0) >= 2
            then true else false
        end as has_revenue_contraction,

        -- Overall risk level
        case
            when h.health_score < 20 then 'critical'
            when h.health_score < 40
                or (coalesce(ut.events_prev_30d, 0) > 0
                    and coalesce(ut.events_last_30d, 0) < ut.events_prev_30d * 0.5)
                or coalesce(es.escalated_tickets_90d, 0) >= 2
            then 'high'
            when h.health_score < 60
                or coalesce(rt.contraction_months_6m, 0) >= 2
            then 'medium'
            else 'low'
        end as risk_level,

        -- Number of active risk signals 
        (case when h.health_score < 40 then 1 else 0 end)
        + (case when coalesce(ut.events_prev_30d, 0) > 0
                and coalesce(ut.events_last_30d, 0) < ut.events_prev_30d * 0.5
            then 1 else 0 end)
        + (case when coalesce(es.escalated_tickets_90d, 0) >= 2 then 1 else 0 end)
        + (case when coalesce(rt.contraction_months_6m, 0) >= 2 then 1 else 0 end)
        as risk_signal_count,

        current_timestamp() as assessed_at

    from health h
    left join usage_trend ut on h.account_id = ut.account_id
    left join escalation_signal es on h.account_id = es.account_id
    left join revenue_trend rt on h.account_id = rt.account_id
)

select * from final
order by risk_signal_count desc, health_score asc
