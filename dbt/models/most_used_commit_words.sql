-- CREATE TABLE de_final_data.words_data
-- (
--     github_event_foreign_key STRING,
--     word STRING
--  )
-- CLUSTER BY
--   github_event_foreign_key,
--   word



{{ config(materialized='view') }}

SELECT word, count(word) as count
from {{ source('staging','words_data') }}
GROUP BY word
ORDER BY count DESC
LIMIT 50