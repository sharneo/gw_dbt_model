-- models/my_dynamic_table.sql

{{ config(
    materialized='dynamic_table',
    target_lag='downstream',
    transient='true',
    snowflake_warehouse='DEV_ETL_WH',
    post_hook="ALTER DYNAMIC TABLE {{ this }} REFRESH", 
) }}
with cte_data as 
(
  SELECT 
     {{ dbt_utils.star(
     from=ref('vw_raw_pc_policy')) }},
  FROM {{ ref('vw_raw_pc_policy') }}
)

select * from cte_data
ORDER BY ID