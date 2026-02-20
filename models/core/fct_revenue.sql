with monthly_revenue as (
    select
        account_id,
        invoice_month                           as revenue_month,
        sum(amount)                             as mrr
    from {{ ref('stg_invoices') }}
    where is_paid = true
    group by 1, 2
),

with_previous as (
    select
        account_id,
        revenue_month,
        mrr,

        lag(mrr) over (
            partition by account_id
            order by revenue_month
        )                                       as previous_mrr,

        lag(revenue_month) over (
            partition by account_id
            order by revenue_month
        )                                       as previous_revenue_month

    from monthly_revenue
),

final as (
    select
        account_id,
        revenue_month,
        mrr,
        mrr * 12                                as arr,
        coalesce(previous_mrr, 0)               as previous_mrr,

        case
            when previous_mrr is null then 'new'
            when mrr > previous_mrr then 'expansion'
            when mrr < previous_mrr then 'contraction'
            else 'flat'
        end                                     as revenue_movement,

        -- Expansion amount (how much MORE are they paying?)
        case
            when previous_mrr is null then mrr
            when mrr > previous_mrr then mrr - previous_mrr
            else 0
        end                                     as expansion_mrr,

        -- Contraction amount (how much LESS are they paying?)
        case
            when previous_mrr is not null and mrr < previous_mrr
            then previous_mrr - mrr
            else 0
        end                                     as contraction_mrr,

        -- Net change
        mrr - coalesce(previous_mrr, 0)         as net_mrr_change,

        -- Months since first invoice
        dense_rank() over (
            partition by account_id
            order by revenue_month
        )                                       as month_number

    from with_previous
)

select * from final
