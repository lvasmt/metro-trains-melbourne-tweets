WITH raw_data AS (
  SELECT 
    * 
  FROM 
    {{source('metro_data','raw_metro_trains_twitter')}}
), 

dedupe_key AS (
  SELECT
    *, 
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY time_of_load DESC) as row_num
  FROM
    raw_data
),

deduped_data AS (
  SELECT
    *
    EXCEPT (row_num)
  FROM
    dedupe_key
  WHERE
    row_num = 1
)

SELECT
  *
FROM
  deduped_data
