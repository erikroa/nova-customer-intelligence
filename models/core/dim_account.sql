-- models/core/dim_account.sql
-- Master account dimension. 

with accounts as (
    select * from {{ ref('stg_accounts') }}
),

-- Current subscription summary per account
subscription_summary as (
    select
        account_id,
        sum(case when is_active then mrr_amount else 0 end)     as current_mrr,
        count(case when is_active then 1 end)                   as active_subscription_count,
        count(*)                                                 as total_subscription_count,
        min(start_date)                                         as first_subscription_date,
        max(case when is_active then start_date end)            as latest_subscription_date,
        -- Check if they have any add-ons
        count(case when is_active and product_name != 'novacrm_platform' then 1 end) as active_addon_count
    from {{ ref('stg_subscriptions') }}
    group by 1
),

-- Lifetime support summary per account
ticket_summary as (
    select
        account_id,
        count(*)                                                as lifetime_tickets,
        count(case when status in ('open', 'escalated') then 1 end) as open_tickets,
        avg(satisfaction_score)                                  as avg_csat,
        avg(resolution_hours)                                   as avg_resolution_hours,
        sum(case when is_sla_breach then 1 else 0 end)         as sla_breaches
    from {{ ref('stg_support_tickets') }}
    group by 1
),

final as (
    select
        -- Account identifiers
        a.account_id,
        a.company_name,
        a.industry,
        a.employee_count,
        a.plan_tier,
        a.account_owner,
        a.region,
        a.signup_date,
        a.status,

        --Account tenure
        date_diff(date('2025-01-31'), a.signup_date, day)           as tenure_days,
        date_diff(date('2025-01-31'), a.signup_date, month)         as tenure_months,

        --Business segment
        {{ classify_segment('a.employee_count', 'a.plan_tier') }} as account_segment,

        --From subscriptions
        coalesce(s.current_mrr, 0)                              as current_mrr,
        coalesce(s.current_mrr, 0) * 12                         as current_arr,
        coalesce(s.active_subscription_count, 0)                as active_subscription_count,
        coalesce(s.total_subscription_count, 0)                 as total_subscription_count,
        coalesce(s.active_addon_count, 0)                       as active_addon_count,
        s.first_subscription_date,
        s.latest_subscription_date,

        --From support
        coalesce(t.lifetime_tickets, 0)                         as lifetime_tickets,
        coalesce(t.open_tickets, 0)                             as open_tickets,
        round(coalesce(t.avg_csat, 0), 1)                       as avg_csat,
        round(coalesce(t.avg_resolution_hours, 0), 1)           as avg_resolution_hours,
        coalesce(t.sla_breaches, 0)                             as sla_breaches,

        --Account lifecycle stage
        case
            when a.status = 'trial' then 'trial'
            when a.status = 'churned' then 'churned'
            when date_diff(date('2025-01-31'), a.signup_date, day) <= 90 then 'onboarding'
            when date_diff(date('2025-01-31'), a.signup_date, day) <= 365 then 'growing'
            else 'mature'
        end                                                     as lifecycle_stage

    from accounts a
    left join subscription_summary s on a.account_id = s.account_id
    left join ticket_summary t on a.account_id = t.account_id
)

select * from final
