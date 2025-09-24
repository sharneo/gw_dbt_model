{% snapshot aai_pc_policy %}  

{{    
  config(      
    target_schema='P001_GWPC',      
    strategy='check',      
    unique_key='id',      
    check_cols=['sys_unique_id']    
  )  
}}  



  select *,cast(UpdateTime as timestamp_ntz) as update_time_ntz
  from {{ ref('vw_raw_pc_policy') }}
{% endsnapshot %}