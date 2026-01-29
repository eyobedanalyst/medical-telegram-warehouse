{{ config(materialized='table') }}

select
    y.message_id,

    -- Join keys
    f.channel_key,
    f.date_key,

    -- Image metadata
    y.image_path,
    y.detected_objects,
    y.object_count,
    y.avg_confidence,
    y.has_person,
    y.has_product,
    y.image_category

from {{ source('raw', 'yolo_image_enrichment') }} y
join {{ ref('fct_messages') }} f
  on y.message_id = f.message_id
