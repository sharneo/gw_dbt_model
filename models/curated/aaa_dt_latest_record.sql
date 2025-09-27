-- models/my_dynamic_table.sql

{{ config(
    materialized='dynamic_table',
    target_lag='downstream',
    transient='true',
    refresh_trigger='ON_SCHEDULE',
    snowflake_warehouse='DEV_ETL_WH',
    post_hook="ALTER DYNAMIC TABLE {{ this }} REFRESH", 
) }}
with cte_data as 
(
  SELECT 
    donotdestroy,
    isportalpolicy_icare,
    CAST(publicid AS VARCHAR(300)) as publicid,
    priorpremiums,
    issuedate,
    priorpremiums_cur,
    movedpolicysourceaccountid,
    accountid,
    createtime,
    losshistorytype,
    excludedfromarchive,
    archivestate,
    archiveschemainfo,
    archivefailuredetailsid,
    packagerisk,
    numpriorlosses,
    updatetime,
    primarylanguage,
    donotarchive,
    id,
    primarylocale,
    productcode,
    excludereason,
    groupnumberfromportal,
    createuserid,
    archivefailureid,
    crnnumber_icare,
    originaleffectivedate,
    beanversion,
    archivepartition,
    retired,
    updateuserid,
    priortotalincurred,
    archivedate,
    priortotalincurred_cur,
    producercodeofserviceid,
    newproducercode_ext,
    newclaimschemeagent_icare,
    CAST(movedpolsrcacctpubid AS VARCHAR(300)) as movedpolsrcacctpubid,
    current_record_flag,
    CAST(agencycontactdetails_ext AS VARCHAR(300)) as agencycontactdetails_ext,
    CAST(agencycontactemail_ext AS VARCHAR(300)) as agencycontactemail_ext,
    agencycontactnumber_ext,
    insurancebook_extid,
    ingestion_time,
    sys_unique_id
  FROM {{ ref('vw_raw_pc_policy') }}
)

select *
from cte_data
QUALIFY ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ingestion_time DESC ) = 1
