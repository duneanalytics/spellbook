{% macro oneinch_escrow_dst_creations_macro(blockchain) %}

{% set date_from = '2024-08-20' %}

with

factories as (
    select factory
    from ({{ oneinch_blockchain_macro(blockchain) }}), unnest(escrow_factory_addresses) as f(factory)
)

, createDstEscrow as (
    select
        *
        , substr(keccak(concat(
            0xff
            , factory
            , keccak(concat(
                order_hash
                , hashlock
                , lpad(maker, 32, 0x00)
                , lpad(taker, 32, 0x00)
                , lpad(token, 32, 0x00)
                , cast(amount as varbinary)
                , cast(safety_deposit as varbinary)
                , to_big_endian_32(cast(to_unixtime(block_time) as int))
                , substr(timelocks, 5) -- replace the first 4 bytes with current block time
            ))
            , keccak(concat(
                0x3d602d80600a3d3981f3363d3d373d3d3d363d73
                , substr(keccak(concat(0xd6, 0x94, factory, 0x03)), 13) -- dst nonce = 3
                , 0x5af43d82803e903d91602b57fd5bf3)
            )
        )), 13) as escrow
    from (
        -- will be converted to submitted contracts
        select
            '{{ blockchain }}' as blockchain
            , block_number
            , block_time
            , tx_hash
            , trace_address
            , "to" as factory
            , substr(input, 4 + 32*0 + 1, 32) as order_hash
            , substr(input, 4 + 32*1 + 1, 32) as hashlock
            , substr(input, 4 + 32*2 + 12 + 1, 20) as maker
            , substr(input, 4 + 32*3 + 12 + 1, 20) as taker
            , substr(input, 4 + 32*4 + 12 + 1, 20) as token
            , bytearray_to_uint256(substr(input, 4 + 32*5 + 1, 32)) as amount
            , bytearray_to_uint256(substr(input, 4 + 32*6 + 1, 32)) as safety_deposit
            , substr(input, 4 + 32*7 + 12 + 1, 32) as timelocks
            , success as call_success
        from {{ source(blockchain, 'traces') }}
        where
            starts_with(input, 0xdea024e4) -- createDstEscrow
            and "to" in (select factory from factories)
            {% if is_incremental() %}
                and {{ incremental_predicate('block_time') }}
            {% else %}
                and block_time > timestamp '{{ date_from }}'
            {% endif %}
    )
)

-- output --

select
    blockchain
    , block_number
    , block_time
    , tx_hash
    , trace_address
    , factory
    , escrow
    , order_hash
    , hashlock
    , maker
    , taker
    , token
    , amount
    , safety_deposit
    , timelocks
    , call_success
    , date_trunc('month', block_time) as block_month
from createDstEscrow

{% endmacro %}