WITH filtered_data AS (
  SELECT
    time_of_load,
    conversation_id,
    id,
    created_at,
    text
  FROM
    {{ref('stg_metro_deduplicated')}}
  WHERE 
    in_reply_to_user_id = 'nan'
)

SELECT
  *
FROM
  filtered_data
