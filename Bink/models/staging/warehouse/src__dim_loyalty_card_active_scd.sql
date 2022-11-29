with source as (

    select * from {{ source('BINK', 'DIM_LOYALTY_CARD_ACTIVE_SCD') }}

),

renamed as (

    select
        loyalty_card_id,
        user_id,
        channel,
        removed,
        valid_from,
        valid_to

    from source

)

select * from renamed