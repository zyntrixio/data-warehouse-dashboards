WITH source AS (
    SELECT * 
    FROM {{ source('BINK', 'FACT_LOYALTY_CARD_ADD_AUTH') }}
)

,renamed AS (
    SELECT
        EVENT_ID
        ,EVENT_DATE_TIME
        ,EVENT_TYPE
        ,LOYALTY_CARD_ID
        ,LOYALTY_PLAN
        ,IS_MOST_RECENT
        ,CHANNEL
        ,ORIGIN
        ,USER_ID
        ,EMAIL_DOMAIN
        ,INSERTED_DATE_TIME
        ,UPDATED_DATE_TIME
    FROM source
)

SELECT *
FROM renamed