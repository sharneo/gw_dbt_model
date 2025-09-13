
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PUBLICID
from DEV_CURATED_DB.P001_GWPC.vw_ai_pc_policy
where PUBLICID is null



  
  
      
    ) dbt_internal_test