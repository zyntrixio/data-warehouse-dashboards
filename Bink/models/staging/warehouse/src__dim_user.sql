WITH source AS (
    SELECT * 
    FROM {{ source('BINK', 'DIM_USER') }}
)

,renamed AS (
    SELECT
        USER_ID
        ,CHANNEL_ID
        ,DATE_JOINED
        ,IS_ACTIVE
        ,IS_STAFF
        ,IS_SUPERUSER
        ,IS_TESTER
        ,LAST_LOGIN
        ,MARKETING_CODE_ID
        ,SALT
    FROM source
)

SELECT *
FROM renamed
