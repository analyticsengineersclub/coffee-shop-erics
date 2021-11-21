{{ config(
    materialized='table'
) }}


with minutes_calc as
(
    select
      pv.*
    , round(timestamp_diff(timestamp,lag(pv.timestamp) over (partition by visitor_id order by timestamp asc), second) / 60, 2) minutes_diff
    from {{ ref('page_views_visitor_stitch') }} pv
)

, new_session_flag as
(
    select
      minutes_calc.*
    , case
        when (minutes_diff > 30 or minutes_diff is null) then 1
        else 0
      end as new_session_flag
    from minutes_calc
)

, session_id_sum as
(
    select
      nsf.*
    , sum(new_session_flag) over (partition by visitor_id order by timestamp asc) session_id
    from new_session_flag nsf
)

select
  id
, visitor_id
, device_type
, timestamp
, page
, customer_id
, session_id session_id_num
, visitor_id || "_" ||session_id visitor_id_session_id
from session_id_sum

