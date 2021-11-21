{{ config(
    materialized='table'
) }}


with week_spine_base as
(
    select
      *
    from
        UNNEST(GENERATE_DATE_ARRAY('2021-01-01', current_date(), INTERVAL 1 week)) AS date_snapshot
)

, week_spine as
(
    select
      date_trunc(date_snapshot, week) weekly_snapshots
    from week_spine_base
)

-- , purchasing_customers as
-- (
--     select
--       distinct customer_id
--     from `analytics-engineers-club.coffee_shop.orders` ord
-- )

, customer_week_spine as
(
    select
      week_spine.weekly_snapshots
    , customer_id
    , count(*) over (partition by customer_id order by weekly_snapshots asc) week
    from week_spine
    cross join {{ ref('customers') }} customers
    where week_spine.weekly_snapshots >= cast(date_trunc(customers.min_order_at, week) as date)
)

, weekly_purchases as
(
    select
      cast(date_trunc(created_at, week) as date) weekly_snapshot
    , customer_id
    , sum(total) revenue
    from {{ source('coffee_shop', 'orders') }}
    group by 1,2
)

select
  customer_week_spine.*
, weekly_purchases.revenue
, sum(revenue) over (partition by customer_week_spine.customer_id order by customer_week_spine.weekly_snapshots asc) cumulative_revenue
from customer_week_spine
left join weekly_purchases 
on (customer_week_spine.weekly_snapshots = weekly_purchases.weekly_snapshot and 
    customer_week_spine.customer_id = weekly_purchases.customer_id)