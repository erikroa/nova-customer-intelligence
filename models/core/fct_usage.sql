-- models/core/fct_usage.sql
-- Daily product usage aggregated per account. One row per account per day.
-- Measures engagement depth, feature breadth, and user activity.

with daily_usage as (
    select
        account_id,
        event_date,

        -- Volume metrics
        count(*)                                                as total_events,
        count(distinct user_id)                                 as active_users,
        count(distinct event_name)                              as distinct_features_used,
        count(distinct event_category)                          as distinct_categories_used,

        -- Events by category
        countif(event_category = 'reporting')                   as reporting_events,
        countif(event_category = 'crm_core')                    as crm_core_events,
        countif(event_category = 'communication')               as communication_events,
        countif(event_category = 'advanced')                    as advanced_events,
        countif(event_category = 'platform')                    as platform_events,

        -- Engagement signals
        countif(event_name = 'api_call')                        as api_calls,
        countif(event_name = 'report_created')                  as reports_created,
        countif(event_name = 'workflow_created')                as workflows_created,

        -- Activity window
        min(event_timestamp)                                    as first_event_at,
        max(event_timestamp)                                    as last_event_at,

        -- Session proxy: hours between first and last event
        round(
            timestamp_diff(
                max(event_timestamp),
                min(event_timestamp),
                minute
            ) / 60.0,
            1
        )                                                       as active_hours

    from {{ ref('stg_usage_events') }}
    group by 1, 2
),

final as (
    select
        *,

        -- Feature depth score: how many of the 5 categories did they touch?
        round(distinct_categories_used / 5.0 * 100, 0)         as feature_breadth_pct,

        -- Is this a "power user" day? (above-average activity)
        case
            when total_events >= 10 and distinct_features_used >= 5
            then true
            else false
        end                                                     as is_power_usage_day

    from daily_usage
)

select * from final
