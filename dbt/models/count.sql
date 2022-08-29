{{ config(materialized='view') }}

select count(*) as count
from {{ ref('github_data_clean') }}
