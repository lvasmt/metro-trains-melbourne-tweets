WITH extracted_info AS (
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
    REGEXP_EXTRACT(text,r'\@[^\s]*') AS external_tagging,
    REGEXP_EXTRACT(text,r'(?i)^([\W]).*') as info_type,
    REGEXP_EXTRACT_ALL(text,r'All lines|Frankston|Pakenham|Cranbourne|Sandringham|Craigieburn|Upfield|Sydenham|Williamstown|W.town|Werribee|Lilydale|Glen Waverley|Belgrave|Alamein|Hurstbridge|Epping|Stony Point|Melton|Sunbury|Mernda|Bell ') as train_lines,
    TRIM(REGEXP_EXTRACT(text,r'(?i)(?:.*lines?\:?|\:\n*|\s\-\s)(.*|Major Delays|Major delays|Delays)(?:due.*|while.*|after.*|trespasser|\n)')) AS info_on_issue,
    TRIM(REGEXP_EXTRACT(text,r'due to [^\n]*|while[^\n]*|after [^\n]*')) AS reason
  FROM
    {{ref('stg_metro_datetime_extraction')}}
)


SELECT
  *
FROM
  extracted_info