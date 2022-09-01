with source as (

    select * from {{ source('BINK', 'FACT_VOUCHER') }}

),

renamed as (

    select
        voucher_code,
        loyalty_card_id,
        loyalty_plan_id,
        state,
        voucher_type,
        date_redeemed,
        date_issued,
        expiry_date,
        redemption_tracked,
        time_to_redemption,
        days_left_on_vouchers,
        days_valid_for

    from source

)

select * from renamed