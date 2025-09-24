with cte_data as 
(
SELECT 
    $1:DONOTDESTROY::BOOLEAN AS donotdestroy,
    $1:ISPORTALPOLICY_ICARE::BOOLEAN AS isportalpolicy_icare,
    $1:PUBLICID::TEXT AS publicid,
    $1:PRIORPREMIUMS::NUMBER AS priorpremiums,
    $1:ISSUEDATE::TIMESTAMP_LTZ AS issuedate,
    $1:PRIORPREMIUMS_CUR::NUMBER AS priorpremiums_cur,
    $1:MOVEDPOLICYSOURCEACCOUNTID::NUMBER AS movedpolicysourceaccountid,
    $1:ACCOUNTID::NUMBER AS accountid,
    $1:CREATETIME::TIMESTAMP_NTZ AS createtime,
    $1:UPDATETIME::TIMESTAMP_NTZ AS updatetime,
    $1:ID::NUMBER AS ID,
    $1:LOSSHISTORYTYPE::NUMBER AS losshistorytype
 from {{ source('gwpc', 'RAW_PC_POLICY') }}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['ID','UpdateTime','donotdestroy', 'isportalpolicy_icare', 'priorpremiums','issuedate','accountid']) }} sys_unique_id,
    ID,
    UpdateTime,
    donotdestroy,
    isportalpolicy_icare,
    priorpremiums,
    CAST(publicid AS VARCHAR(130)) as publicid,
    issuedate,
    accountid
from cte_data
qualify row_number() over (
            partition by ID 
            order by UpdateTime desc
        ) = 1  