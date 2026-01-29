with source as (

    select
        message_id,
        channel_name,
        message_date,
        message_text,
        views,
        forwards,
        has_media,
        image_path
    from raw.telegram_messages

),

cleaned as (

    select
        message_id::bigint as message_id,
        channel_name,
        message_date::timestamp as message_timestamp,
        message_text,
        coalesce(views, 0)::int as views,
        coalesce(forwards, 0)::int as forwards,
        has_media::boolean as has_media,
        image_path,

        -- Derived fields
        length(message_text) as message_length,
        case
            when image_path is not null then true
            else false
        end as has_image

    from source
    where message_text is not null
)

select * from cleaned
