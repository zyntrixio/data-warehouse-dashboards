
{#  The test folder is used for custom tests that are specific to this project.

    A test fails whenever it returns one or more rows.

    This example tests to see if the cum of the sales lines matches the total sale
    value for the sale. It's a bit of a silly example - it sounds useful, but when
    you realise a failue in of one sale to add up correctly, it doesn't document
    which record fails. It will also have poor performance.
    
    A more sensible test would be to use the generic test to check that the sum of
    the sales line column matches the sum of the sales column.
#}

{% test saletotal_equals_salelinetotal(model,SaleLineTable) %}

WITH sales AS (
  SELECT
    Sale_ID,
    Sale_Value
  FROM {{ model }}
),

salelines AS (
  SELECT
    Sale_ID,
    SUM(Quantity * Unit_Price) AS Sale_Value
  FROM {{ SaleLineTable }}
  GROUP BY Sale_ID
),

result AS (
  -- Full outer join to ensure that failed joins also return a failed test
  SELECT
    IFNULL(sales.Sale_ID,salelines.Sale_ID) AS Sale_ID
  FROM sales
  FULL OUTER JOIN salelines ON sales.Sale_ID = salelines.Sale_ID
  WHERE sales.Sale_Value != salelines.Sale_Value
    OR sales.Sale_ID IS NULL
    OR salelines.Sale_ID IS NULL
)

SELECT * FROM result

{% endtest %}