WITH source AS (
    SELECT * 
    FROM {{ source('MERCHANT', 'STG_MERCHANT__WASABI_GRID') }}
)

SELECT *
FROM source