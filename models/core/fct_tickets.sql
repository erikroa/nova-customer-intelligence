-- models/core/fct_tickets.sql
-- Enriched support ticket fact table. One row per ticket.
-- Adds SLA performance metrics and time-based dimensions.

with tickets as (
    select * from {{ ref('stg_support_tickets') }}
),

-- Get account context for each ticket
account_context as (
    select
        account_id,
        plan_tier,
        account_owner,
        region
    from {{ ref('stg_accounts') }}
),

final as (
    select
        -- Ticket identifiers
        t.ticket_id,
        t.account_id,

        -- Ticket attributes
        t.priority,
        t.category,
        t.status,
        t.satisfaction_score,

        -- Time dimensions
        t.created_at,
        t.resolved_at,
        t.created_date,
        extract(month from t.created_at)                        as created_month,
        format_timestamp('%Y-%m', t.created_at)                 as created_year_month,

        -- Resolution metrics
        t.resolution_hours,
        t.sla_target_hours,
        t.is_sla_breach,

        -- SLA performance ratio (< 1.0 = within SLA, > 1.0 = breached)
        case
            when t.resolution_hours is not null and t.sla_target_hours > 0
            then round(t.resolution_hours / t.sla_target_hours, 2)
            else null
        end                                                     as sla_ratio,

        -- Resolution speed classification
        case
            when t.status in ('open', 'escalated') then 'unresolved'
            when t.resolution_hours <= t.sla_target_hours * 0.5 then 'fast'
            when t.resolution_hours <= t.sla_target_hours then 'on_time'
            when t.resolution_hours <= t.sla_target_hours * 1.5 then 'slow'
            else 'critical_breach'
        end                                                     as resolution_speed,

        -- CSAT classification
        case
            when t.satisfaction_score is null then 'no_rating'
            when t.satisfaction_score >= 4.0 then 'satisfied'
            when t.satisfaction_score >= 3.0 then 'neutral'
            else 'dissatisfied'
        end                                                     as csat_tier,

        -- Account context (denormalized for easier analysis)
        ac.plan_tier                                            as account_plan_tier,
        ac.account_owner,
        ac.region                                               as account_region

    from tickets t
    left join account_context ac on t.account_id = ac.account_id
)

select * from final
