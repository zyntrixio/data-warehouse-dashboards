/*
Created at: 20/12/2022
Created by: Anand Bhakta
Description: Query to satisfy merchant reporting on joins
Modified at:
Modified by:
*/


WITH wasabi_grid AS (
    SELECT
        *
    FROM
        {{ref('src__lookup_wasabi_grid')}}
)

,active_users AS (
    SELECT
        *
    FROM
        {{ref('active_users')}}
    WHERE
        loyalty_plan_name = 'Wasabi Club'
)

,adds AS (
    SELECT
        *
    FROM
        {{ref('adds')}}
    WHERE
        loyalty_plan_name = 'Wasabi Club'
)

,joins AS (
    SELECT
        *
    FROM
        {{ref('joins')}}
    WHERE
        loyalty_plan_name = 'Wasabi Club'
)

,trans AS (
    SELECT
        *
    FROM
        {{ref('transactions')}}
    WHERE
        loyalty_plan_name = 'Wasabi Club'
)

,selected_metrics AS (
    SELECT
        u.DATE
//        ,'metrics' AS INPUT --delete this
        ,j.J001 AS USERS_JOINED --JOINS__TOTAL__JOINS_SCHEME
        ,t.T003b AS TRANSACTIONS --TRANSACTIONS_FOR_MI_ALL_TRANS_EXC_TESTERS__TOTAL_MATCHED
        ,t.T010b AS SALES --TRANSACTIONS_FOR_MI_ALL_TRANS_EXC_TESTERS__TOTAL_TRANSACTION_VALUE_ACTIVE_USERS
        ,t.T001b AS ATV --TRANSACTIONS_FOR_MI_ALL_TRANS_EXC_TESTERS__AVERAGE_TRANSACTION_VALUE
        ,t.T008b AS ATF --TRANSACTIONS_FOR_MI_ALL_TRANS_EXC_TESTERS__AVERAGE_NUMBER_OF_MATCHED_TRANSACTIONS_PER_CUSTOMER_TOTAL	
        ,t.T009b AS ARPU --NULL
        ,NULL AS MARKETING_OPT_IN_PERCENT --Calc Required
        ,NULL AS LIVE_USERS --NULL
        ,NULL AS ACTIVE_USER_RATE --NULL
        ,u.AU001b AS ACTIVE_USERS --NULL
        ,NULL AS CUMULATIVE_JOINS --JOINS__TOTAL__CUMULATIVE_JOINS_SCHEME_AND_PLL_ENABLED --this is pulling different from total and is misleading pll vs non-pll
        ,NULL AS ADDS_WITH_PC --Calc Required
        ,NULL AS MATCHED_AND_STAMP_AWARDED --TRANSACTIONS_FOR_MI_ALL_TRANS_EXC_TESTERS__MATCHED_STAMP_AWARDED	
        ,NULL AS MATCHED_ONLY --TRANSACTIONS_FOR_MI_ALL_TRANS_EXC_TESTERS__MATCHED_BUT_NO_STAMP	
        ,NULL AS STAMP_AWARDED_PERCENT --TRANSACTIONS_FOR_MI_ALL_TRANS_EXC_TESTERS__TOTAL_MATCHED	
        ,NULL AS ISSUED_VOUCHERS --VOUCHERS_EXC_TESTERS__ISSUED
        ,NULL AS REDEEMED_VOUCHERS --VOUCHERS_EXC_TESTERS__REDEEMED
        ,NULL AS CUMULATIVE_ISSUED_VOUCHERS --VOUCHERS_EXC_TESTERS__TOTAL_ISSUED	
        ,NULL AS CUMULATIVE_REDEEMED_VOUCHERS --VOUCHERS_EXC_TESTERS__TOTAL_REDEEMED	
    FROM
        active_users u
    JOIN
        adds a ON a.DATE = u.DATE
    JOIN
        joins j ON j.DATE = u.DATE
    JOIN
        trans t ON t.DATE = u.DATE
    
)

,selected_grids AS (
    SELECT
        DATE
//        ,'grids' AS INPUT
        ,JOINS__TOTAL__JOINS_SCHEME AS USERS_JOINED
        ,TRANSACTIONS_FOR_MI_ALL_TRANS_EXC_TESTERS__TOTAL_MATCHED AS TRANSACTIONS
        ,TRANSACTIONS_FOR_MI_ALL_TRANS_EXC_TESTERS__TOTAL_TRANSACTION_VALUE_ACTIVE_USERS AS SALES
        ,TRANSACTIONS_FOR_MI_ALL_TRANS_EXC_TESTERS__AVERAGE_TRANSACTION_VALUE AS ATV
        ,TRANSACTIONS_FOR_MI_ALL_TRANS_EXC_TESTERS__AVERAGE_NUMBER_OF_MATCHED_TRANSACTIONS_PER_CUSTOMER_TOTAL AS ATF
        ,NULL AS ARPU
        ,NULL AS MARKETING_OPT_IN_PERCENT --CALC REQUIRED -> Need to reload grid into snowflake with percentages as decimals
        ,NULL AS LIVE_USERS
        ,NULL AS ACTIVE_USER_RATE
        ,NULL AS ACTIVE_USERS
        ,JOINS__TOTAL__CUMULATIVE_JOINS_SCHEME_AND_PLL_ENABLED AS CUMULATIVE_JOINS -- this is pulling different from total and is misleading pll vs non-pll 
        ,ADDS__BINK__ADDS_WITH_PAYMENT_CARD + ADDS__BARCLAYS__ADDS_WITH_PAYMENT_CARD AS ADDS_WITH_PC
        ,TRANSACTIONS_FOR_MI_ALL_TRANS_EXC_TESTERS__MATCHED_STAMP_AWARDED AS MATCHED_AND_STAMP_AWARDED
        ,TRANSACTIONS_FOR_MI_ALL_TRANS_EXC_TESTERS__MATCHED_BUT_NO_STAMP AS MATCHED_ONLY
        ,TRANSACTIONS_FOR_MI_ALL_TRANS_EXC_TESTERS__TOTAL_MATCHED AS STAMP_AWARDED_PERCENT
        ,VOUCHERS_EXC_TESTERS__ISSUED AS ISSUED_VOUCHERS
        ,VOUCHERS_EXC_TESTERS__REDEEMED AS REDEEMED_VOUCHERS
        ,VOUCHERS_EXC_TESTERS__TOTAL_ISSUED AS CUMULATIVE_ISSUED_VOUCHERS
        ,VOUCHERS_EXC_TESTERS__TOTAL_REDEEMED AS CUMULATIVE_REDEEMED_VOUCHERS
    FROM
         wasabi_grid
)

,combine AS (
    SELECT 
        *
    FROM
        selected_metrics
    WHERE
        DATE >= '{{ var("cutoff") }}'
    UNION
    SELECT 
        *
    FROM
        selected_grids
    WHERE
        DATE < '{{ var("cutoff") }}'
  
)
select * from combine
order by DATE
