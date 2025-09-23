WITH staged AS (
    SELECT *
    FROM {{ source('wca','RAW_CLAIM_ACTIVITY') }}
)

