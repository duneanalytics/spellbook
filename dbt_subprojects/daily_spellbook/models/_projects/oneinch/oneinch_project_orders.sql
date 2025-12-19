{{-  
    config(
        schema = 'oneinch',
        alias = 'project_orders',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        partition_by = ['block_month'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['blockchain', 'block_month', 'block_number', 'tx_hash', 'call_trace_address', 'order_hash', 'call_trade']
    )
-}}

{%- set
    orders_base_columns = [
        'blockchain',
        'block_number',
        'block_time',
        'tx_hash',
        'tx_from',
        'tx_to',
        'method',
        'call_selector',
        'call_trace_address',
        'call_from',
        'call_to',
        'call_gas_used',
        'maker',
        'maker_asset',
        'making_amount',
        'taker_asset',
        'taking_amount',
        'order_hash',
        'flags',
    ]
-%}

{%- set native_addresses = '(0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee)' -%}



with

meta as (
    select 
        blockchain
        , wrapped_native_token_address
        , native_token_symbol as native_symbol
    from {{ source('oneinch', 'blockchains') }}
)

, orders as (
    select *
        , array[
            if(maker_asset in {{native_addresses}}, wrapped_native_token_address, maker_asset)
            , if(taker_asset in {{native_addresses}}, wrapped_native_token_address, taker_asset)
        ] as assets
        , date_trunc('minute', block_time) as minute
        , row_number() over(partition by blockchain, block_number, tx_hash order by call_trace_address, order_hash) as counter
    from (
        {% for blockchain in oneinch_project_swaps_exposed_blockchains_list() %}
            select
                {{ orders_base_columns | join(', ') }}
                , tag
                , project
                , order_start
                , order_end
                , order_deadline
                , call_trade
                , call_trades
            from {{ ref('oneinch_' + blockchain + '_project_orders') }}
            where call_success
            {% if not loop.last %} union all {% endif %}
        {% endfor %}
            
        union all

        select
            blockchain
            , block_number
            , block_time
            , tx_hash
            , tx_from
            , tx_to
            , call_method as method
            , call_selector
            , call_trace_address
            , call_from
            , call_to
            , call_gas_used
            , maker
            , maker_asset
            , making_amount
            , taker_asset
            , taking_amount
            , order_hash
            , flags
            , contract_name as tag
            , '1inch' as project
            , null as order_start
            , null as order_end
            , null as order_deadline
            , 1 as call_trade
            , 1 as call_trades
        from {{ source('oneinch', 'lo') }}
        where call_success
    )
    join meta using(blockchain)
    {% if is_incremental() %}
        where {{ incremental_predicate('block_time') }}
    {% endif %}
)

, trusted_tokens as (
    select
        distinct blockchain
        , contract_address
        , true as trusted
    from {{ source('prices', 'trusted_tokens') }}
)

, prices as (
    select
        blockchain
        , contract_address
        , minute
        , price
        , decimals
        , symbol
    from {{ source('prices', 'usd') }}
    {% if is_incremental() %}
        where {{ incremental_predicate('minute') }}
    {% endif %}
)

, joined as (
    select
        blockchain
        , block_number
        , tx_hash
        , call_trace_address
        , coalesce(order_hash, concat(tx_hash, to_big_endian_32(cast(counter as int)))) as order_hash
        , call_trade
        , any_value(block_time) as block_time
        , any_value(tx_from) as tx_from
        , any_value(tx_to) as tx_to
        , any_value(project) as project
        , any_value(call_selector) as call_selector
        , any_value(call_from) as call_from
        , any_value(call_to) as call_to
        , any_value(call_gas_used) as call_gas_used
        , any_value(maker) as maker
        , any_value(maker_asset) as maker_asset
        , any_value(taker_asset) as taker_asset
        , any_value(if(maker_asset = assets[1], symbol, native_symbol)) filter(where contract_address = assets[1]) as maker_asset_symbol
        , any_value(if(taker_asset = assets[2], symbol, native_symbol)) filter(where contract_address = assets[2]) as taker_asset_symbol
        , any_value(making_amount) as making_amount
        , any_value(taking_amount) as taking_amount
        , any_value(making_amount * price / pow(10, decimals)) filter(where contract_address = assets[1]) as making_amount_usd
        , any_value(taking_amount * price / pow(10, decimals)) filter(where contract_address = assets[2]) as taking_amount_usd
        , any_value(making_amount * price / pow(10, decimals)) filter(where contract_address = assets[1] and trusted) as making_amount_usd_trusted
        , any_value(taking_amount * price / pow(10, decimals)) filter(where contract_address = assets[2] and trusted) as taking_amount_usd_trusted
        , any_value(order_start) as order_start
        , any_value(order_end) as order_end
        , any_value(order_deadline) as order_deadline
        , any_value(call_trades) as call_trades
        , any_value(flags) as flags
        , any_value(tag) as tag
    from (select * from orders, unnest(assets) as a(contract_address))
    left join prices using(blockchain, contract_address, minute)
    left join trusted_tokens using(blockchain, contract_address)
    group by 1, 2, 3, 4, 5, 6
)

-- output --

select
    blockchain
    , block_number
    , block_time
    , tx_hash
    , tx_from
    , tx_to
    , project
    , call_trace_address
    , call_selector
    , call_from
    , call_to
    , call_gas_used
    , maker
    , maker_asset
    , taker_asset
    , maker_asset_symbol
    , taker_asset_symbol
    , making_amount
    , taking_amount
    , making_amount_usd
    , taking_amount_usd
    , coalesce(
        greatest(making_amount_usd_trusted, taking_amount_usd_trusted)
        , making_amount_usd_trusted
        , taking_amount_usd_trusted
        , greatest(making_amount_usd,  taking_amount_usd)
        , making_amount_usd
        , taking_amount_usd
    ) as amount_usd
    , order_hash
    , order_start
    , order_end
    , order_deadline
    , call_trade
    , call_trades
    , flags
    , tag
    , date(date_trunc('month', block_time)) as block_month
from joined