-- This test fails if any message has a date in the future
-- dbt expects this query to return ZERO rows

select
    f.message_id,
    d.full_date
from analytics_analytics.fct_messages f
join analytics_analytics.dim_dates d
  on f.date_key = d.date_key
where d.full_date > current_date


