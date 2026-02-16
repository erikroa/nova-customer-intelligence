
with account_base as (
    select * from {{ ref('dim_account') }}
    where status = 'active'
),

-- USAGE SIGNAL
usage_scores as (
    select
        account_id,
        avg(total_events) as avg_daily_events,
        ntile(4) over (order by avg(total_events)) * 25 as usage_score
    from {{ ref('fct_usage') }}
    where event_date >= date_sub(date('2025-01-31'), interval 30 day)
    group by 1
),

-- SUPPORT SIGNAL
support_scores as (
    select
        account_id,
        count(case when status in ('open', 'escalated') then 1 end) as recent_open_tickets,
        avg(satisfaction_score) as recent_avg_csat,
        case
            when count(case when status in ('open', 'escalated') then 1 end) = 0
                and coalesce(avg(satisfaction_score), 5.0) >= 4.0 then 100
            when count(case when status in ('open', 'escalated') then 1 end) = 0 then 75
            when count(case when status in ('open', 'escalated') then 1 end) <= 2
                and coalesce(avg(satisfaction_score), 3.0) >= 3.0 then 50
            when count(case when status in ('open', 'escalated') then 1 end) <= 5 then 25
            else 10
        end as support_score
    from {{ ref('fct_tickets') }}
    where created_date >= date_sub(date('2025-01-31'), interval 30 day)
    group by 1
),

-- REVENUE SIGNAL
revenue_scores as (
    select
        account_id,
        mrr,
        net_mrr_change,
        case
            when net_mrr_change > 0 then 100
            when net_mrr_change = 0 then 60
            when net_mrr_change > -50 then 30
            else 10
        end as revenue_score
    from {{ ref('fct_revenue') }}
    
    qualify row_number() over (partition by account_id order by revenue_month desc) = 1
),

-- ENGAGEMENT SIGNAL
engagement_scores as (
    select
        account_id,
        count(distinct event_name) as features_used_30d,
        ntile(4) over (order by count(distinct event_name)) * 25 as engagement_score
    from {{ ref('stg_usage_events') }}
    where event_date >= date_sub(date('2025-01-31'), interval 30 day)
    group by 1
),

-- Combine Signals
scored as (
    select
        a.account_id,
        a.company_name,
        a.account_segment,
        a.plan_tier,
        a.tenure_months,
        a.current_mrr,
        a.current_arr,
        a.account_owner,
        a.lifecycle_stage,
        a.region,

        -- Individual scores
        coalesce(u.usage_score, 0) as usage_score,
        coalesce(s.support_score, 50) as support_score,
        coalesce(r.revenue_score, 50) as revenue_score,
        coalesce(e.engagement_score, 0) as engagement_score,

        -- Context metrics
        coalesce(u.avg_daily_events, 0) as avg_daily_events_30d,
        coalesce(s.recent_open_tickets, 0) as open_tickets_90d,
        round(coalesce(s.recent_avg_csat, 0), 1) as avg_csat_90d,
        coalesce(r.mrr, 0) as latest_mrr,
        coalesce(r.net_mrr_change, 0) as latest_mrr_change,
        coalesce(e.features_used_30d, 0) as features_used_30d,

        -- Weighted composite health score
        {{ calculate_health_score(
            'coalesce(u.usage_score, 0)',
            'coalesce(s.support_score, 50)',
            'coalesce(r.revenue_score, 50)',
            'coalesce(e.engagement_score, 0)'
        ) }} as health_score

    from account_base a
    left join usage_scores u on a.account_id = u.account_id
    left join support_scores s on a.account_id = s.account_id
    left join revenue_scores r on a.account_id = r.account_id
    left join engagement_scores e on a.account_id = e.account_id
),

final as (
    select
        *,

        -- Health tier
        case
            when health_score >= 80 then 'champion'
            when health_score >= 60 then 'healthy'
            when health_score >= 40 then 'neutral'
            when health_score >= 20 then 'at_risk'
            else 'critical'
        end as health_tier,

        -- Priority rank
        row_number() over (order by health_score asc) as intervention_priority,

        current_timestamp() as scored_at

    from scored
)

select * from final
