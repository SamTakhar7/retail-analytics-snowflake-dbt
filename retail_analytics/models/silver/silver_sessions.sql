{{ config(materialized='table') }}

select
  user_id,
  user_session,
  min(event_ts) as session_start_ts,
  max(event_ts) as session_end_ts,
  count(*) as event_count,
  sum(case when event_type = 'purchase' then 1 else 0 end) as purchase_events
from {{ ref('bronze__ecom_events') }}
group by 1,2