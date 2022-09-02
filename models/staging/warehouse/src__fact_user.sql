WITH source AS (
    SELECT * 
    FROM {{ source('BINK', 'FACT_USER') }}
)

,renamed AS (
    SELECT
        EVENT_ID
        ,EVENT_DATE_TIME
        ,USER_ID
        ,EVENT_TYPE
        ,IS_MOST_RECENT
        ,ORIGIN
        ,CHANNEL
        ,INSERTED_DATE_TIME
        ,UPDATED_DATE_TIME
    FROM source
)

SELECT *
FROM renamed