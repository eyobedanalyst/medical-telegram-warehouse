with messages as (

    select
        message_id,
        channel_name,
        message_timestamp,
        message_text,
        message_length,
        views,
        forwards,
        has_image
    from analytics_staging.stg_telegram_messages
),

final as (

    select
        m.message_id,

        c.channel_key,
        d.date_key,

        m.message_text,
        m.message_length,
        m.views as view_count,
        m.forwards as forward_count,
        m.has_image

    from messages m
    join {{ ref('dim_channels') }} c
        on m.channel_name = c.channel_name
    join {{ ref('dim_dates') }} d
        on m.message_timestamp::date = d.full_date
)

select * from final

