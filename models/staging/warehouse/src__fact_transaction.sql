WITH source AS (
    SELECT * 
    FROM {{ source('BINK', 'FACT_TRANSACTION') }}
)

,renamed AS (
    SELECT
        EVENT_ID
        ,EVENT_DATE_TIME
        ,USER_ID
        ,TRANSACTION_ID
        ,PROVIDER_SLUG
        ,LOYALTY_PLAN_NAME
        ,TRANSACTION_DATE
        ,SPEND_AMOUNT
        ,SPEND_CURRENCY
        ,LOYALTY_ID
        ,LOYALTY_CARD_ID
        ,MERCHANT_ID
        ,PAYMENT_ACCOUNT_ID
        ,SETTLEMENT_KEY
        ,INSERTED_DATE_TIME
        ,UPDATED_DATE_TIME
    FROM source
)

SELECT *
FROM renamed