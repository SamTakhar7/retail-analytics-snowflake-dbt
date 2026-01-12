{{ config(materialized='table') }}

select
  customer_id,
  max(country) as country,
  min(invoice_ts) as first_seen_ts,
  max(invoice_ts) as last_seen_ts
from {{ ref('bronze__online_retail') }}
where customer_id is not null
group by 1