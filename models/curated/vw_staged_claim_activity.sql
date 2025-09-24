{{ config(materialized='view') }}
with base as (
    select table_data,file_name as source_file
    from {{ source('wca_file', 'RAW_CLAIM_ACTIVITY_RECORD') }}
)
select
    {{ wca_to_columns('table_data') }}
from 
    base