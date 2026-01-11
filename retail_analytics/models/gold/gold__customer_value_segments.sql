{{ config(materialized='table') }}

with tx as (

    select
        customer_id,
        invoice_ts::date as invoice_date,
        invoice_no,
        item_qty::int as quantity,
        gross_revenue::float as unit_price,

        -- opinionated revenue calc:
        -- treat negative quantity as returns (reduces revenue)
        (item_qty::int * gross_revenue::float) as line_revenue

    from {{ ref('silver_transactions') }}
    where customer_id is not null
      and gross_revenue is not null

),

agg as (

    select
        customer_id,

        max(invoice_date) as last_purchase_date,
        count(distinct invoice_no) as order_count,
        count(*) as line_count,
        sum(line_revenue) as lifetime_revenue,

        -- recent windows
        sum(case when invoice_date >= dateadd('day', -30, current_date()) then line_revenue else 0 end) as revenue_last_30d,
        sum(case when invoice_date <  dateadd('day', -30, current_date())
                  and invoice_date >= dateadd('day', -60, current_date())
                 then line_revenue else 0 end) as revenue_prev_30d,

        count(distinct case when invoice_date >= dateadd('day', -30, current_date()) then invoice_no end) as orders_last_30d

    from tx
    group by 1

),

rfm as (

    select
        *,
        datediff('day', last_purchase_date, current_date()) as days_since_last_purchase,

        -- RFM scoring (opinionated thresholds; tune later)
        case
            when datediff('day', last_purchase_date, current_date()) <= 30 then 5
            when datediff('day', last_purchase_date, current_date()) <= 60 then 4
            when datediff('day', last_purchase_date, current_date()) <= 90 then 3
            when datediff('day', last_purchase_date, current_date()) <= 180 then 2
            else 1
        end as r_score,

        case
            when order_count >= 20 then 5
            when order_count >= 10 then 4
            when order_count >= 5  then 3
            when order_count >= 2  then 2
            else 1
        end as f_score,

        case
            when lifetime_revenue >= 5000 then 5
            when lifetime_revenue >= 2000 then 4
            when lifetime_revenue >= 1000 then 3
            when lifetime_revenue >= 300  then 2
            else 1
        end as m_score

    from agg

),

final as (

    select
        customer_id,
        last_purchase_date,
        days_since_last_purchase,
        order_count,
        lifetime_revenue,
        revenue_last_30d,
        revenue_prev_30d,
        orders_last_30d,

        r_score,
        f_score,
        m_score,
        (r_score + f_score + m_score) as rfm_score,

        case
            when (r_score + f_score + m_score) >= 13 then 'champion'
            when r_score >= 4 and m_score >= 3 then 'loyal_high_value'
            when r_score >= 4 and f_score <= 2 then 'new_or_reactivated'
            when r_score <= 2 and m_score >= 4 then 'at_risk_high_value'
            when r_score <= 2 and m_score <= 2 then 'lost_low_value'
            else 'core'
        end as customer_segment,

        case
            when revenue_prev_30d = 0 and revenue_last_30d > 0 then 'up'
            when revenue_last_30d > revenue_prev_30d then 'up'
            when revenue_last_30d < revenue_prev_30d then 'down'
            else 'flat'
        end as revenue_trend_30d

    from rfm

)

select * from final