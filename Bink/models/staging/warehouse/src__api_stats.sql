WITH source AS (
    SELECT * 
    FROM {{ source('SERVICE_DATA', 'FACT_API_RESPONSE_TIME') }}
)

,renamed AS (
    SELECT
        API_ID
        ,DATE_TIME
        ,METHOD
        ,PATH
        ,CHANNEL
        ,RESPONSE_TIME
    FROM
        source
)

select * 
from renamed
