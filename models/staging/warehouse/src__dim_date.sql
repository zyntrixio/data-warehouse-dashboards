WITH source AS (
    SELECT * 
    FROM {{ source('BINK', 'DIM_DATE') }}
)

,renamed AS (
    SELECT
        DATE
        ,YEAR
        ,QUARTER
        ,MONTH
        ,MONTHNAME
        ,DAYOFMONTH
        ,DAYOFWEEK
        ,WEEKOFYEAR
        ,DAYOFYEAR
        ,DAYNAME
        ,WEEKPART
        ,DAYNUMBER
        ,YEARNUMBER
        ,QUARTERNUMBER
        ,MONTHNUMBER
        ,YEAR_QUARTER
        ,YEAR_MONTH
        ,START_OF_MONTH
        ,END_OF_MONTH
        ,START_OF_YEAR
        ,END_OF_YEAR
        ,START_OF_QUARTER
        ,END_OF_QUARTER
        ,START_OF_WEEK
        ,END_OF_WEEK
        ,FINANCIAL_YEAR
        ,FINANCIAL_QUARTER
        ,FINANCIAL_MONTH
        ,FINANCIAL_WEEKOFYEAR
        ,FINANCIAL_DAYOFYEAR
        ,FINANCIAL_YEAR_QUARTER
        ,FINANCIAL_YEAR_MONTH
        ,START_OF_FINANCIAL_YEAR
        ,END_OF_FINANCIAL_YEAR
        ,START_OF_FINANCIAL_QUARTER
        ,END_OF_FINANCIAL_QUARTER
    FROM source
)

SELECT *
FROM renamed