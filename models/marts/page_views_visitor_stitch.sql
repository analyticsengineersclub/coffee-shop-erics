{{ config(
    materialized='table'
) }}


with min_vis_id as
(
    select 
    pv.customer_id
    , pv.visitor_id
    , dense_rank() over (partition by customer_id order by timestamp asc) pv_rank
    from {{ source('web_tracking', 'pageviews') }} pv
    where customer_id is not null
    qualify pv_rank = 1
)

select
pv.id
,ifnull(min_vis_id.visitor_id, pv.visitor_id) visitor_id
,pv.device_type
,pv.timestamp
,pv.page
,pv.customer_id
from {{ source('web_tracking', 'pageviews') }} pv
left join min_vis_id
on pv.customer_id = min_vis_id.customer_id