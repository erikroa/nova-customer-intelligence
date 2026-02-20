with account_cohorts as (
    select
        account_id,
        date_trunc(signup_date, month) as cohort_month
    from {{ ref('dim_account') }}
),

-- Join revenue data 
revenue_with_cohort as (
    select
        r.account_id,
        c.cohort_month,
        r.revenue_month,
        r.mrr,
        r.month_number,

        -- Months since cohort start
        date_diff(r.revenue_month, c.cohort_month, month) as months_since_signup

    from {{ ref('fct_revenue') }} r
    inner join account_cohorts c on r.account_id = c.account_id
),

-- Aggregate by cohort and month offset
cohort_metrics as (
    select
        cohort_month,
        months_since_signup,

        -- Accounts and revenue
        count(distinct account_id) as active_accounts,
        sum(mrr) as cohort_mrr,
        avg(mrr) as avg_mrr_per_account

    from revenue_with_cohort
    group by 1, 2
),

-- 
cohort_sizes as (
    select
        cohort_month,
        active_accounts as initial_accounts,
        cohort_mrr as initial_mrr
    from cohort_metrics
    where months_since_signup = 0
),

final as (
    select
        cm.cohort_month,
        format_date('%Y-%m', cm.cohort_month) as cohort_label,
        cm.months_since_signup,
        cm.active_accounts,
        cm.cohort_mrr,
        cm.avg_mrr_per_account,
        cs.initial_accounts,
        cs.initial_mrr,

        -- Account retention rate
        round(cm.active_accounts / cs.initial_accounts * 100, 1) as account_retention_pct,

        -- Revenue retention rate (can exceed 100% with expansion)
        round(cm.cohort_mrr / cs.initial_mrr * 100, 1) as revenue_retention_pct,

        -- Per-account revenue change since month 0
        round(cm.avg_mrr_per_account - (cs.initial_mrr / cs.initial_accounts), 2) as avg_mrr_change_per_account

    from cohort_metrics cm
    inner join cohort_sizes cs on cm.cohort_month = cs.cohort_month
)

select * from final
order by cohort_month, months_since_signup
