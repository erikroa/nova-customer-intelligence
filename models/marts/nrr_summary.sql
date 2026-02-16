-- models/marts/nrr_summary.sql
-- Net Revenue Retention (NRR) summary by month.
-- Breaks NRR into its components: starting MRR, new, expansion, contraction, churn, ending MRR.
-- This is the #1 metric every SaaS board and investor tracks.

with monthly_movements as (
    select
        revenue_month,
        sum(mrr) as total_mrr,
        sum(case when revenue_movement = 'new' then mrr else 0 end) as new_mrr,
        sum(expansion_mrr) as expansion_mrr,
        sum(contraction_mrr) as contraction_mrr,
        count(distinct account_id) as paying_accounts
    from {{ ref('fct_revenue') }}
    group by 1
),

-- Identify churned MRR: accounts that paid last month but not this month
churn_detection as (
    select
        r.revenue_month as last_active_month,
        date_add(r.revenue_month, interval 1 month) as churn_month,
        sum(r.mrr) as churned_mrr,
        count(distinct r.account_id) as churned_accounts
    from {{ ref('fct_revenue') }} r
    left join {{ ref('fct_revenue') }} next_month
        on r.account_id = next_month.account_id
        and date_add(r.revenue_month, interval 1 month) = next_month.revenue_month
    where next_month.account_id is null
    group by 1, 2
),

-- Combine into a complete monthly waterfall
waterfall as (
    select
        m.revenue_month,
        m.total_mrr as ending_mrr,
        m.new_mrr,
        m.expansion_mrr,
        m.contraction_mrr,
        coalesce(c.churned_mrr, 0) as churned_mrr,
        m.paying_accounts,
        coalesce(c.churned_accounts, 0) as churned_accounts,

        -- Starting MRR = previous month's ending MRR
        lag(m.total_mrr) over (order by m.revenue_month) as starting_mrr

    from monthly_movements m
    left join churn_detection c on m.revenue_month = c.churn_month
),

final as (
    select
        revenue_month,
        coalesce(starting_mrr, 0) as starting_mrr,
        new_mrr,
        expansion_mrr,
        contraction_mrr,
        churned_mrr,
        ending_mrr,

        -- The key metric: Net Revenue Retention
        -- NRR = (Starting MRR + Expansion - Contraction - Churn) / Starting MRR
        case
            when coalesce(starting_mrr, 0) = 0 then null
            else round(
                (starting_mrr + expansion_mrr - contraction_mrr - churned_mrr)
                / starting_mrr * 100,
                1
            )
        end as nrr_pct,

        -- Gross Revenue Retention (excludes expansion)
        -- GRR = (Starting MRR - Contraction - Churn) / Starting MRR
        case
            when coalesce(starting_mrr, 0) = 0 then null
            else round(
                (starting_mrr - contraction_mrr - churned_mrr)
                / starting_mrr * 100,
                1
            )
        end as grr_pct,

        -- Account counts
        paying_accounts,
        churned_accounts,

        -- MRR growth rate month-over-month
        case
            when coalesce(starting_mrr, 0) = 0 then null
            else round(
                (ending_mrr - starting_mrr) / starting_mrr * 100,
                1
            )
        end as mrr_growth_pct,

        -- Ending ARR
        ending_mrr * 12 as ending_arr

    from waterfall
)

select * from final
order by revenue_month
