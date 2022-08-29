{{ config(
    materialized='table',
    partition_by={
      "field": "created_at_timestamp",
      "data_type": "timestamp"
    }
)}}



SELECT 
    id,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ',created_at) as created_at_timestamp,
    type,
    CAST(actor_id as STRING) as actor_id_string,
    actor_login,
    public,
    CAST(repo_id as STRING) as repo_id_string,
    repo_name,
    payload_commits
from {{ source('staging','github_data') }}
