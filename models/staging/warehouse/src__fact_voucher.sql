with source as (

    select * from {{ source('BINK', 'FACT_VOUCHER') }}

),

renamed as (

    select
        created,
        loyalty_card_id,
        state,
        earn_type,
        voucher_code,
        redemption_tracked,
        date_redeemed,
        date_issued,
        expiry_date,
        time_to_redemption,
        days_left_on_vouchers,
        days_valid_for

    from source

)

select * from renamed