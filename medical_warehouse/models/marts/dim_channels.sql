with channel_stats as (

    select
        channel_name,
        min(message_timestamp) as first_post_date,
        max(message_timestamp) as last_post_date,
        count(*) as total_posts,
        avg(views)::int as avg_views

    from analytics_staging.stg_telegram_messages
    group by channel_name
),

final as (

    select
        row_number() over (order by channel_name) as channel_key,
        channel_name,

        case
            when channel_name ilike '%pharma%' then 'Pharmaceutical'
            when channel_name ilike '%cosmetic%' then 'Cosmetics'
            else 'Medical'
        end as channel_type,

        first_post_date,
        last_post_date,
        total_posts,
        avg_views

    from channel_stats
)

select * from final
