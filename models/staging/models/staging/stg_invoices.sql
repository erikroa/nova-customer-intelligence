-- models/staging/stg_invoices.sql
-- Cleans invoice/billing data. Standardizes amounts and dates.
-- Source: raw_invoices seed (Billing system export)

with source as (
    select * from {{ ref('raw_invoices') }}
),

cleaned as (
    select
        cast(invoice_id as string)              as invoice_id,
        cast(account_id as string)              as account_id,
        cast(invoice_date as date)              as invoice_date,

        -- Revenue fields
        cast(amount as numeric)                 as amount,
        upper(trim(currency))                   as currency,
        lower(trim(status))                     as status,

        -- Parse pipe-separated line items into a clean field
        trim(line_items)                        as line_items,

        -- Derived: extract month for aggregation
        date_trunc(cast(invoice_date as date), month)  as invoice_month,

        -- Derived: is this a paid invoice? (only paid invoices count as revenue)
        case
            when lower(trim(status)) = 'paid' then true
            else false
        end                                     as is_paid,

        current_timestamp()                     as _loaded_at

    from source
    where invoice_id is not null
)

select * from cleaned
