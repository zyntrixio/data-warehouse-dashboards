WITH source AS (
    SELECT * 
    FROM {{ source('BINK', 'FACT_LOYALTY_CARD_STATUS_CHANGE') }}
)

,renamed AS (
    SELECT
        EVENT_ID
        ,EVENT_DATE_TIME
        ,LOYALTY_CARD_ID
        ,LOYALTY_PLAN_ID
        ,LOYALTY_PLAN_NAME
        ,FROM_STATUS_ID
        ,FROM_STATUS
        ,TO_STATUS_ID
        ,TO_STATUS
        ,IS_MOST_RECENT
        ,ORIGIN
        ,CHANNEL
        ,USER_ID
        ,EMAIL_DOMAIN
        ,INSERTED_DATE_TIME
        ,UPDATED_DATE_TIME
    FROM source
)

SELECT *
FROM renamed