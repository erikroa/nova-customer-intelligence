-- models/staging/stg_support_tickets.sql

with source as (
    select * from {{ ref('raw_support_tickets') }}
),

cleaned as (
    select
        cast(ticket_id as string)               as ticket_id,
        cast(account_id as string)              as account_id,
        cast(created_at as timestamp)           as created_at,

        case
            when resolved_at is null then null
            else cast(resolved_at as timestamp)
        end                                     as resolved_at,

        lower(trim(priority))                   as priority,
        lower(trim(category))                   as category,
        lower(trim(status))                     as status,

        case
            when satisfaction_score is null then null
            else cast(satisfaction_score as float64)
        end                                     as satisfaction_score,

        case
            when resolved_at is not null
            then round(
                timestamp_diff(
                    cast(resolved_at as timestamp),
                    cast(created_at as timestamp),
                    minute
                ) / 60.0,
                1
            )
            else null
        end                                     as resolution_hours,

        case
            when lower(trim(priority)) = 'p1' then 4
            when lower(trim(priority)) = 'p2' then 12
            when lower(trim(priority)) = 'p3' then 48
            when lower(trim(priority)) = 'p4' then 120
        end                                     as sla_target_hours,

        case
            when resolved_at is null then null
            when lower(trim(priority)) = 'p1'
                and timestamp_diff(cast(resolved_at as timestamp), cast(created_at as timestamp), minute) / 60.0 > 4
                then true
            when lower(trim(priority)) = 'p2'
                and timestamp_diff(cast(resolved_at as timestamp), cast(created_at as timestamp), minute) / 60.0 > 12
                then true
            when lower(trim(priority)) = 'p3'
                and timestamp_diff(cast(resolved_at as timestamp), cast(created_at as timestamp), minute) / 60.0 > 48
                then true
            when lower(trim(priority)) = 'p4'
                and timestamp_diff(cast(resolved_at as timestamp), cast(created_at as timestamp), minute) / 60.0 > 120
                then true
            else false
        end                                     as is_sla_breach,

        date(cast(created_at as timestamp))     as created_date,

        current_timestamp()                     as _loaded_at

    from source
    where ticket_id is not null
)

select * from cleaned