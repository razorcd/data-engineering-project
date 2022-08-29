{{ config(materialized='view') }}

SELECT type,
       PARSE_TIMESTAMP('%Y.%m.%d %H:%M', FORMAT_DATETIME('%Y.%m.%d %H:00',created_at_timestamp)) as hour,
       COUNT(*) as count
FROM {{ ref('github_data_clean') }}
GROUP BY hour, type