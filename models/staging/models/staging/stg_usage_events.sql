-- models/staging/stg_usage_events.sql
-- Cleans product usage event data. Extracts date and event categories.
-- Source: raw_usage_events seed (Product analytics export)

with source as (
    select * from {{ ref('raw_usage_events') }}
),

cleaned as (
    select
        cast(event_id as string)                as event_id,
        cast(account_id as string)              as account_id,
        cast(user_id as string)                 as user_id,
        lower(trim(event_name))                 as event_name,
        cast(event_timestamp as timestamp)      as event_timestamp,

        -- Derived: extract date for daily aggregation
        date(cast(event_timestamp as timestamp))  as event_date,

        -- Derived: extract hour for usage pattern analysis
        extract(hour from cast(event_timestamp as timestamp))  as event_hour,

        -- Derived: categorize events into feature groups
        case
            when lower(trim(event_name)) in ('dashboard_viewed', 'report_created', 'export_generated')
                then 'reporting'
            when lower(trim(event_name)) in ('contact_added', 'deal_updated', 'note_added')
                then 'crm_core'
            when lower(trim(event_name)) in ('email_sent', 'meeting_logged', 'task_completed')
                then 'communication'
            when lower(trim(event_name)) in ('api_call', 'integration_configured', 'workflow_created')
                then 'advanced'
            when lower(trim(event_name)) in ('search_performed', 'filter_applied', 'user_invited')
                then 'platform'
            else 'other'
        end                                     as event_category,

        current_timestamp()                     as _loaded_at

    from source
    where event_id is not null
)

select * from cleaned
