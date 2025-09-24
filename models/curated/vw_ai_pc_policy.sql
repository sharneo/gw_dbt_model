{{ config(
    materialized='view',
    secure=true
) }}


select *
from  {{ ref('ai_pc_policy') }}
