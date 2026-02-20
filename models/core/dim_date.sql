with date_spine as (
    select
        date_day
    from
        unnest(
            generate_date_array('2023-01-01', '2025-12-31', interval 1 day)
        ) as date_day
),

final as (
    select
        date_day,

        -- Year / quarter / month / week
        extract(year from date_day)                             as year,
        extract(quarter from date_day)                          as quarter,
        extract(month from date_day)                            as month_number,
        format_date('%B', date_day)                             as month_name,
        format_date('%Y-%m', date_day)                          as year_month,
        extract(isoweek from date_day)                          as week_number,

        -- Day attributes
        extract(dayofweek from date_day)                        as day_of_week,
        format_date('%A', date_day)                             as day_name,
        extract(day from date_day)                              as day_of_month,

        -- Useful flags
        case
            when extract(dayofweek from date_day) in (1, 7) then true
            else false
        end                                                     as is_weekend,

        -- Period boundaries
        date_trunc(date_day, month)                             as first_day_of_month,
        last_day(date_day, month)                               as last_day_of_month,
        date_trunc(date_day, quarter)                           as first_day_of_quarter,
        date_trunc(date_day, year)                              as first_day_of_year

    from date_spine
)

select * from final
