
with source as (
    select * from {{ ref('raw_accounts') }}
),

cleaned as (
    select
        -- Standardize IDs
        cast(account_id as string)              as account_id,

        -- Clean text fields
        trim(company_name)                      as company_name,
        lower(trim(industry))                   as industry,
        cast(employee_count as int64)           as employee_count,
        lower(trim(plan_tier))                  as plan_tier,
        trim(account_owner)                     as account_owner,
        lower(trim(region))                     as region,

        -- Standardize dates
        cast(signup_date as date)               as signup_date,

        -- Standardize status
        lower(trim(status))                     as status,

        -- Metadata
        current_timestamp()                     as _loaded_at

    from source
    where account_id is not null
)

select * from cleaned
