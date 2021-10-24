with
date_spine as
(
SELECT
*
FROM 
    UNNEST(GENERATE_DATE_ARRAY('2021-01-01', current_date(), INTERVAL 1 MONTH)) AS date_snapshot
)

select
*
from date_spine
left join {{ ref('customers') }} cust
on date_spine.date_snapshot = cast(date_trunc(cust.min_order_at, month) as date)
