
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select accountid
from DEV_CURATED_DB.P001_GWPC.ai_pc_policy
where accountid is null



  
  
      
    ) dbt_internal_test