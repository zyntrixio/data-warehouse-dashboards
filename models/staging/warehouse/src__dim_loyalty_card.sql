WITH source AS (
    SELECT *
    FROM {{ source('BINK', 'DIM_LOYALTY_CARD') }}
)

,renamed as (
    SELECT
        LOYALTY_CARD_ID
        ,ADD_AUTH_STATUS
        ,ADD_AUTH_DATE_TIME
        ,JOIN_STATUS
        ,JOIN_DATE_TIME
        ,REGISTER_STATUS
        ,REGISTER_DATE_TIME
        ,UPDATED
        ,STATUS_ID
        ,STATUS
        ,STATUS_TYPE
        ,STATUS_ROLLUP
        ,LINK_DATE
        ,CREATED
        ,ORDERS
        ,ORIGINATING_JOURNEY
        ,IS_DELETED
        ,LOYALTY_PLAN_ID
        ,LOYALTY_PLAN_COMPANY
        ,LOYALTY_PLAN_SLUG
        ,LOYALTY_PLAN_TIER
        ,LOYALTY_PLAN_NAME_CARD
        ,LOYALTY_PLAN_NAME
        ,LOYALTY_PLAN_CATEGORY_ID
        ,LOYALTY_PLAN_CATEGORY
    FROM
        source
)

SELECT *
FROM renamed