{{ config(
    materialized = 'incremental',
    unique_key   = 'sys_unique_id'
) }}


with cte_data as 
(
  SELECT 
     {{ dbt_utils.star(
     from=ref('vw_raw_pc_policy')) }}
  FROM {{ ref('vw_raw_pc_policy') }}
)

select * from cte_data

{% if is_incremental() %}
  WHERE cte_data.ingestion_time > (SELECT MAX(this.ingestion_time) FROM {{ this }} as this )
{% endif %}
ORDER BY ID
