{% snapshot aai_pc_policy %}  

{{    
  config(      
    target_schema='P001_GWPC',      
    strategy='check',      
    unique_key='id',      
    check_cols=['sys_unique_id']    
  )  
}}  




  SELECT 
     {{ dbt_utils.star(from=ref('vw_raw_pc_policy')) }}
  FROM {{ ref('vw_raw_pc_policy') }}
{% endsnapshot %}