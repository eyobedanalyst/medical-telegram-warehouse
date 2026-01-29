-- This test fails if any message has negative view counts
-- View counts must be zero or positive

select
    message_id,
    view_count
from analytics_analytics.fct_messages
where view_count < 0

