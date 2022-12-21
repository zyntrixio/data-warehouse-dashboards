WITH
stg_users AS (
    SELECT *
    FROM {{ref('src__fact_user')}}
)

,stg_lc AS (
    SELECT *
    FROM {{ref('src__fact_lc_add')}}
)

,map_user_to_brand AS (
    SELECT DISTINCT
        USER_ID
        ,BRAND
    FROM
        stg_users
    WHERE
        NULLIF(BRAND, '') IS NOT NULL
    UNION
    SELECT DISTINCT
        USER_ID
        ,BRAND
    FROM
        stg_lc
    WHERE
        NULLIF(BRAND, '') IS NOT NULL
)

SELECT *
FROM map_user_to_brand