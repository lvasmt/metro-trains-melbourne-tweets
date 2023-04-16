{{
  config(
    materialized = "table",
    partition_by={
      "field": "aest_created_date",
      "data_type": "date",
      "granularity": "day"
    }
  )
}}

WITH stg_data AS (
    SELECT 
        time_of_load,
        conversation_id,
        id,
        created_at,
        ts_created_at,
        aest_created_day_of_week,
        aest_created_date,
        aest_created_time,
        text,
        external_tagging,
        info_type,
        train_line,
        info_on_issue,
        reason
    FROM    
        {{ref('stg_metro_unnested_train_lines')}}
)

SELECT 
    *
FROM 
    stg_data