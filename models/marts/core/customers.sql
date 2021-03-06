{{ config(
    materialized='table'
) }}


with 
order_agg as
(
    select
      customer_id id
    , min(created_at) min_order_at
    , count(id) num_of_orders
    from {{ source('coffee_shop', 'orders') }}
    group by 1
)

, final as 
(
    select
      cust.id customer_id
    , cust.name
    , cust.email
    , order_agg.min_order_at
    , order_agg.num_of_orders
    from {{ source('coffee_shop', 'customers') }} cust
    left join order_agg
    on cust.id = order_agg.id
)

select
*
from final