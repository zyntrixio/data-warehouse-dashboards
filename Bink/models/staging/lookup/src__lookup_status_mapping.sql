WITH source AS (
    SELECT * 
    FROM {{ source('BINK_LOOKUP', 'STG_LOOKUP__SCHEME_ACCOUNT_STATUS') }}
)

SELECT *
FROM source