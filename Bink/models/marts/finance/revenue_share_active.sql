/*

Created by:         Chris Mitchell
Created date:       2023-03-08
Last modified by:   
Last modified date: 

Description:
	Create table with calcs for revenue share views

Parameters:
    ref_object      

*/

WITH active_user AS (
    SELECT * FROM "BINK"."BINK"."FACT_TRANSACTION"
)

,active_user_channel AS (
    SELECT DISTINCT USER_ID, CHANNEL, EVENT_TYPE FROM "BINK"."BINK"."FACT_USER" WHERE EVENT_TYPE = 'CREATED'
)

,combined_table as (
    SELECT
        DATE(t.EVENT_DATE_TIME) AS DATE
        ,t.LOYALTY_ID
        ,t.USER_ID
        ,uc.CHANNEL
    FROM active_user t
    LEFT JOIN active_user_channel uc ON
        uc.USER_ID = t.USER_ID
)

SELECT * FROM combined_table LIMIT 100;