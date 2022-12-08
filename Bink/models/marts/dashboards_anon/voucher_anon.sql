with VOUCHERS_NON_ANON as (
    select * from 
    {{ref('voucher')}}
)

, mock_brands as (
    select * 
    from {{ref('trans__brands')}}
)





, num as (
SELECT  LOYALTY_CARD_ID
       ,USER_ID
       ,CHANNEL
       ,state
       ,earn_type
       ,voucher_code
       ,REDEMPTION_TRACKED
       ,DATE_REDEEMED
       ,DATE_ISSUED
       ,EXPIRY_DATE
       ,TIME_TO_REDEMPTION
       ,DAYS_VALID_FOR
       ,days_left_on_vouchers
       ,loyalty_plan_company
       ,loyalty_plan_name
       ,loyalty_card_created
FROM VOUCHERS_NON_ANON
)

, anon as (
    select 
        n.LOYALTY_CARD_ID
       ,n.USER_ID
       ,case when n.CHANNEL = 'com.barclays.bmb' then m.brand
                else n.channel
       end as channel
       ,state
       ,earn_type
       ,md5(voucher_code) as voucher_code
       ,REDEMPTION_TRACKED
       ,DATE_REDEEMED
       ,DATE_ISSUED
       ,EXPIRY_DATE
       ,TIME_TO_REDEMPTION
       ,DAYS_VALID_FOR
       ,days_left_on_vouchers
       ,loyalty_plan_company
       ,loyalty_plan_name
       ,loyalty_card_created
from num n
left join mock_brands  m
on n.user_id = m.user_id
)

select *
from anon