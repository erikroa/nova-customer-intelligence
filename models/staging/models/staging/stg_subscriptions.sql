-- models/staging/stg_subscriptions.sql

with source as (
    select * from {{ ref('raw_subscriptions') }}
),

cleaned as (
    select
        cast(subscription_id as string)         as subscription_id,
        cast(account_id as string)              as account_id,
        lower(trim(product_name))               as product_name,
        lower(trim(plan_tier))                  as plan_tier,
        cast(mrr_amount as numeric)             as mrr_amount,

        date(cast(start_date as timestamp))     as start_date,
        case
            when end_date is null then null
            else cast(end_date as date)
        end                                     as end_date,

        lower(trim(status))                     as status,

        case
            when lower(trim(status)) = 'active' then true
            else false
        end                                     as is_active,

        case
            when end_date is not null
            then date_diff(cast(end_date as date), cast(start_date as date), day)
            else null
        end                                     as duration_days,

        current_timestamp()                     as _loaded_at

    from source
    where subscription_id is not null
)

select * from cleaned