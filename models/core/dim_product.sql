with products_from_subs as (
    select distinct
        product_name,
        plan_tier
    from {{ ref('stg_subscriptions') }}
),

product_attributes as (
    select
        product_name,

        -- Product type classification
        case
            when product_name = 'novacrm_platform' then 'platform'
            else 'addon'
        end as product_type,

        -- Display name
        case
            when product_name = 'novacrm_platform' then 'NovaCRM Platform'
            when product_name = 'api_access' then 'API Access'
            when product_name = 'advanced_analytics' then 'Advanced Analytics'
            when product_name = 'priority_support' then 'Priority Support'
            else initcap(replace(product_name, '_', ' '))
        end as product_display_name,

        -- List price by product 
        case
            when product_name = 'novacrm_platform' then null  -- varies by tier
            when product_name = 'api_access' then 29
            when product_name = 'advanced_analytics' then 79
            when product_name = 'priority_support' then 59
            else 0
        end as list_price_mrr,

        -- Feature category 
        case
            when product_name = 'novacrm_platform' then 'core'
            when product_name = 'api_access' then 'integration'
            when product_name = 'advanced_analytics' then 'analytics'
            when product_name = 'priority_support' then 'support'
            else 'other'
        end as feature_category

    from products_from_subs
    group by product_name
),

platform_tiers as (
    select
        'novacrm_platform' as product_name,
        plan_tier,
        case
            when plan_tier = 'starter' then 49
            when plan_tier = 'growth' then 149
            when plan_tier = 'enterprise' then 499
        end as tier_price_mrr
    from unnest(['starter', 'growth', 'enterprise']) as plan_tier
),

final as (
    select
        -- Generate a surrogate key
        concat(pa.product_name, '_', coalesce(pt.plan_tier, 'all')) as product_key,
        pa.product_name,
        pa.product_type,
        pa.product_display_name,
        coalesce(pt.plan_tier, 'all') as plan_tier,
        coalesce(pt.tier_price_mrr, pa.list_price_mrr) as list_price_mrr,
        pa.feature_category

    from product_attributes pa
    left join platform_tiers pt
        on pa.product_name = pt.product_name
)

select * from final
