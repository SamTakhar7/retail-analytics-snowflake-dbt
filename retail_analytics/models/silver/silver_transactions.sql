{{ config(materialized='table') }}

select
  invoice_no,
  invoice_ts,
  customer_id,
  country,
  sum(quantity * unit_price) as gross_revenue,
  sum(quantity) as item_qty
from {{ ref('bronze__online_retail') }}
where invoice_no is not null
group by 1,2,3,4