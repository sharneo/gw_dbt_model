with cte_data as 
(
SELECT 
    $1:DONOTDESTROY::BOOLEAN AS donotdestroy,
    $1:ISPORTALPOLICY_ICARE::BOOLEAN AS isportalpolicy_icare,
    $1:PUBLICID::TEXT AS publicid,
    $1:PRIORPREMIUMS::NUMBER AS priorpremiums,
    $1:ISSUEDATE::TIMESTAMP_TZ AS issuedate,
    $1:PRIORPREMIUMS_CUR::NUMBER AS priorpremiums_cur,
    $1:MOVEDPOLICYSOURCEACCOUNTID::NUMBER AS movedpolicysourceaccountid,
    $1:ACCOUNTID::NUMBER AS accountid,
    $1:CREATETIME::TIMESTAMP_LTZ AS createtime,
    $1:LOSSHISTORYTYPE::NUMBER AS losshistorytype
 from DEV_RAW_DB.L001_GWPC.RAW_PC_POLICY
)

SELECT 
    donotdestroy,
    isportalpolicy_icare,
    priorpremiums,
    CAST(publicid AS VARCHAR(130)) as publicid,
    issuedate,
    accountid,
    current_timestamp() as data_as_at

from cte_data