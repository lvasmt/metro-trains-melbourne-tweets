WITH filtered_info AS (
  SELECT
    time_of_load,
    conversation_id,
    id,
    created_at,
    text
  FROM
    {{ref('stg_metro_filtered')}}
),

dates AS (
  SELECT
    time_of_load,
    conversation_id,
    id,
    created_at,
    text,
    TIMESTAMP(created_at,"UTC") as ts_created_at
  FROM
    filtered_info
),

converted_timezone AS (
  SELECT
    time_of_load,
    conversation_id,
    id,
    created_at,
    ts_created_at,
    EXTRACT(DAYOFWEEK FROM ts_created_at AT TIME ZONE "Australia/Melbourne") AS aest_created_day_of_week,
    EXTRACT(DATE FROM ts_created_at AT TIME ZONE "Australia/Melbourne") AS aest_created_date,
    EXTRACT(HOUR FROM ts_created_at AT TIME ZONE "Australia/Melbourne") AS aest_created_time,
    text
  FROM
    dates
)

SELECT  
  *
FROM  
  converted_timezone