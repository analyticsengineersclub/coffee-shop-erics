{{ config(
    materialized='table'
) }}

select
  order_items.id
, order_items.order_id
, order_items.product_id
, orders.created_at
, orders.customer_id
, product_prices.id product_price_id
, product_prices.price
from {{ source('coffee_shop', 'order_items') }} as order_items

left join {{ source('coffee_shop', 'orders') }} as orders
    on order_items.order_id = orders.id

left join {{ source('coffee_shop', 'products') }} as products
    on order_items.product_id = products.id

left join {{ source('coffee_shop', 'product_prices') }} as product_prices
  on order_items.product_id = product_prices.product_id
  and orders.created_at between product_prices.created_at and product_prices.ended_at
