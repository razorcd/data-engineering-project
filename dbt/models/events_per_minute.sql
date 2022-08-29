{{ config(materialized='view') }}


SELECT TIMESTAMP_ADD(PARSE_TIMESTAMP("%Y.%m.%d %H:%M", FORMAT_DATETIME('%Y.%m.%d %H:%M',created_at_timestamp)), INTERVAL 1 MINUTE) as time_by_minute,
       COUNT(*) as event_count_per_minute
from {{ ref('github_data_clean') }}
GROUP BY time_by_minute