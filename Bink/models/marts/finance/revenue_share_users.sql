/*
Created by:         Chris Mitchell
Created date:       2023-03-08
Last modified by:   
Last modified date: 

Description:
	Create table with calcs for revenue share JOINS and REG users lloyds

Parameters:
    ref_object      

Formatted by: SQLFMT plugin
*/
with
    joins as (
        select * from "UAT"."BINK"."FACT_LOYALTY_CARD_ADD" where channel = 'LLOYDS'
    ),

    select_joins as (
        select
            date_trunc('month', event_date_time) as report_month,
            auth_type,
            channel,
            event_type,
            loyalty_card_id,
            loyalty_plan_name
        from joins
        where auth_type in ('JOIN', 'REGISTER') and event_type = 'SUCCESS'
    ),
    count_joins as (
        select report_month, channel, loyalty_plan_name, count(distinct loyalty_card_id)
        from select_joins
        group by channel, loyalty_plan_name, report_month
    )

select *
from count_joins
order by report_month, channel, loyalty_plan_name