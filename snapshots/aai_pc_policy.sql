{% snapshot aai_pc_policy %}  

{{
  config(      
    target_schema='P001_GWPC',      
    strategy='timestamp',      
    unique_key='sys_unique_id',      
    updated_at='update_time_ntz'    
  )  
}}  

  select *,cast(UpdateTime as timestamp_ntz) as update_time_ntz
  from {{ ref('vw_raw_pc_policy') }}
{% endsnapshot %}