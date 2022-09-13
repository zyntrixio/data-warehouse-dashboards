with source as (

    select * from {{ source('BINK', 'FACT_VOUCHER') }}

),

renamed as (

SELECT  
     EVENT_DATE_TIME
    ,loyalty_card_id
    ,current_channel
    ,user_id
    ,state
    ,earn_type
    ,voucher_code
    ,date_redeemed
    ,date_issued
    ,expiry_date
    ,issued
    ,issued_channel
    ,redemed
    ,redeemed_channel
       ,redemption_tracked
       ,time_to_redemption
       ,days_left_on_vouchers
       ,days_valid_for
FROM source

)

select * from renamed