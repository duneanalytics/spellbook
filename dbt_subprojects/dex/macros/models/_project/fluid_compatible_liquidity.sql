{% macro fluid_liquidity_events( 
    blockchain = null
    , project = 'fluid'
    , version = null 
    , liquidity_pools = null
    , contract_address = null 
    )
%}
-- full credits to @dknugo https://dune.com/dknugo for entire logic for Fluid liquidity, code here mirrors what he has on dune: https://dune.com/queries/5011149

with 

logoperate_raw as (
    select 
        block_time
        , tx_hash
        , index as evt_index
        , varbinary_substring(topic1, 13, 20) as user_address
        , varbinary_substring(topic2, 13, 20) as token_address
        , varbinary_to_int256(varbinary_substring(data, 1, 32)) as supply_amount
        , varbinary_to_int256(varbinary_substring(data, 33, 32)) as borrow_amount
        , varbinary_substring(varbinary_substring(data, 65, 32), 13, 20) as withdraw_to
        , varbinary_substring(varbinary_substring(data, 97, 32), 13, 20) as borrow_to
        , varbinary_to_uint256(varbinary_substring(data, 129, 32)) as total_amounts
        , varbinary_to_uint256(varbinary_substring(data, 161, 32)) as exchange_prices_and_config
        , varbinary_to_uint256(0xff) as x8 
        , varbinary_to_uint256(0x3fff) as X14 
        , varbinary_to_uint256(0x7fff) as X15 
        , varbinary_to_uint256(0xffff) as X16 
        , varbinary_to_uint256(0xffffff) as X24 
        , varbinary_to_uint256(0xffffffffffffffff) as X64 
        , 8 as default_exponent_size
        , varbinary_to_uint256(0xff) as default_exponent_mask
    from 
    {{ source(blockchain, 'logs') }}
    where contract_address = {{ contract_address }}
    and topic0 = 0x4d93b232a24e82b284ced7461bf4deacffe66759d5c24513e6f29e571ad78d15
    and varbinary_substring(topic1, 13, 20) in (select dex from {{ liquidity_pools }} where blockchain = '{{blockchain}}' )
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {% endif %}
)

, logoperate_decoded_step1 as (
    select 
        *
        , bitwise_and(exchange_prices_and_config, X16) /1e4 as borrow_rate 
        , bitwise_and(
            bitwise_right_shift(
                exchange_prices_and_config, 16
            )
            , X14 
        ) / 1e4 as fee 
        , bitwise_and(
            bitwise_right_shift(
                exchange_prices_and_config, 30
            )
            , X14 
        ) / 1e4 as utilization 
        , bitwise_and(
            bitwise_right_shift(
                exchange_prices_and_config, 91
            )
            , X64 
        ) as supply_exchange_price
        -- borrow exchange rate
        , bitwise_and(
            bitwise_right_shift(
                exchange_prices_and_config, 155
            )
            , X64 
        )  as borrow_exchange_price
        , bitwise_and(total_amounts, X64) as supply_raw_bigNumber
        , bitwise_and(
            bitwise_and(
                bitwise_right_shift(total_amounts, 64)
                , X64)
             ,X64) as supply_interest_free_bigNumber
        , bitwise_and(
            bitwise_and(
                bitwise_right_shift(total_amounts, 128)
                , X64)
             ,X64) as borrow_raw_bigNumber
        , bitwise_and(
            bitwise_and(
                bitwise_right_shift(total_amounts, 192)
                , X64)
             ,X64) as borrow_interest_free_bigNumber
    from logoperate_raw
)

, logoperate_decoded_step2 as(
    select 
        *
        , bitwise_left_shift(
             -- coefficient
             bitwise_right_shift(
                 supply_raw_bigNumber
                 , default_exponent_size
             )
             , -- exponent
             cast(
                 bitwise_and(
                     supply_raw_bigNumber
                     , default_exponent_mask
                 )
                as bigint
             )
         ) as supply_interest_raw

        , bitwise_left_shift(
             -- coefficient
             bitwise_right_shift(
                 borrow_raw_bigNumber
                 , default_exponent_size
             )
             , -- exponent
             cast(
                 bitwise_and(
                     borrow_raw_bigNumber
                     , default_exponent_mask
                 )
                as bigint
             )
         ) as borrow_interest_raw

       , bitwise_left_shift(
             -- coefficient
             bitwise_right_shift(
                 supply_interest_free_bigNumber
                 , default_exponent_size
             )
             , -- exponent
             cast(
                 bitwise_and(
                     supply_interest_free_bigNumber
                     , default_exponent_mask
                 )
                as bigint
             )
         ) as supply_interest_free

        , bitwise_left_shift(
             -- coefficient
             bitwise_right_shift(
                 borrow_interest_free_bigNumber
                 , default_exponent_size
             )
             , -- exponent
             cast(
                 bitwise_and(
                     borrow_interest_free_bigNumber
                     , default_exponent_mask
                 )
                as bigint
             )
         ) as borrow_interest_free
    
    from logoperate_decoded_step1
)

, logoperate_decoded as (
    select
        block_time
        , tx_hash
        , evt_index
        , user_address
        , token_address
        , supply_amount
        , borrow_amount
        , withdraw_to
        , borrow_to
        , total_amounts
        , exchange_prices_and_config
        , borrow_rate
        , borrow_rate 
            * (1.0 - fee) 
            * borrow_interest_raw * borrow_exchange_price / 1e12 
            / supply_interest_raw * supply_exchange_price / 1e12 
          as supply_rate
        , fee
        , utilization
        , supply_exchange_price
        , borrow_exchange_price
        , supply_interest_raw
        , borrow_interest_raw
        , supply_interest_free as total_supply_interest_free
        , borrow_interest_free as total_borrow_interest_free
        , supply_interest_raw * supply_exchange_price / 1e12 as total_supply_with_interest
        , borrow_interest_raw * borrow_exchange_price / 1e12 as total_borrow_with_interest
    from 
    logoperate_decoded_step2
)

select 
    '{{blockchain}}' as blockchain
    , '{{version}}' as version 
    , '{{project}}' as project 
    , * 
from 
logoperate_decoded

{% endmacro %}