{{
    config(
        schema = 'oneinch',
        alias = 'escrow_dst_creations',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['blockchain', 'tx_hash', 'trace_address']
    )
}}

{% set date_from = '2024-08-20' %}

with

createDstEscrow as ({% for factory, factory_data in oneinch_escrow_cfg_factories_macro().items() %}
    select * from ({% for blockchain in factory_data.blockchains %}
        select
            '{{ blockchain }}' as blockchain
            , call_block_number as block_number
            , call_block_time as block_time
            , call_tx_hash as tx_hash
            , call_trace_address as trace_address
            , contract_address as factory
            , {{ factory_data.dst_creation.get("order_hash", "null") }} as order_hash
            , {{ factory_data.dst_creation.get("hashlock", "null") }} as hashlock
            , {{ factory_data.dst_creation.get("maker", "null") }} as maker
            , {{ factory_data.dst_creation.get("taker", "null") }} as taker
            , {{ factory_data.dst_creation.get("token", "null") }} as token
            , {{ factory_data.dst_creation.get("amount", "null") }} as amount
            , {{ factory_data.dst_creation.get("safety_deposit", "null") }} as safety_deposit
            , {{ factory_data.dst_creation.get("timelocks", "null") }} as timelocks
            , call_success
        from (
            select *, cast(json_parse({{ factory_data.dst_creation.get("dstImmutables", '"dstImmutables"') }}) as map(varchar, varchar)) as creation_map
            from {{ source('oneinch_' + blockchain, factory + '_call_' + factory_data.dst_creation.method) }}
            {% if is_incremental() %}
                where {{ incremental_predicate('call_block_time') }}
            {% else %}
                where call_block_time >= greatest(timestamp '{{ factory_data['start'] }}', timestamp {{ oneinch_easy_date() }})
            {% endif %}
        )
        {% if not loop.last %} union all {% endif %}
    {% endfor %})
    {% if not loop.last %} union all {% endif %}
{% endfor %})

, calculations as (
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
    from createDstEscrow
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
from calculations