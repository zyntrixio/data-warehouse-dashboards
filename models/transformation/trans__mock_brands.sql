-- Arbitrariy assign each user to a brand for the prupose to segregation

WITH
stg_users AS (
    SELECT *
    FROM {{ref('src__dim_user')}}
)

,mock_brands AS (
    SELECT 
        USER_ID,
        NTILE(7) OVER(ORDER BY SALT) N
    FROM stg_users
)

,label_mock_brands  AS (
    SELECT
        USER_ID
        ,CASE WHEN
            N IN (1,3,7)
                THEN 'LLOYDS'
            WHEN N IN (2,4)
                THEN 'HSBC'
            ELSE 'HALIFAX'
            END AS BRAND
    FROM mock_brands
)

SELECT *
FROM label_mock_brands