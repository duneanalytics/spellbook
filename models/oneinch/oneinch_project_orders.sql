{{  
    config(
        schema = 'oneinch',
        alias = 'project_orders',
        materialized = 'view',
        unique_key = ['blockchain', 'block_number', 'tx_hash', 'call_trace_address']
    )
}}



with
    
orders as (
    select
        *
        , array[maker_asset, taker_asset] as assets
        , date_trunc('minute', block_time) as minute
        , row_number() over(partition by blockchain, block_number, tx_hash, project order by call_trace_address) as counter
    from (
        {% for blockchain in oneinch_exposed_blockchains_list() %}
            select
                blockchain
                , block_number
                , block_time
                , tx_hash
                , project
                , method
                , call_selector
                , call_trace_address
                , call_from
                , call_to
                , call_gas_used
                , maker
                , maker_asset
                , coalesce(making_amount, if(order_start = uint256 '0', maker_max_amount, maker_max_amount - cast(block_unixtime - order_start as double) / (order_end - order_start) * (maker_max_amount - maker_min_amount))) as making_amount
                , taker_asset
                , coalesce(taking_amount, if(order_start = uint256 '0', taker_max_amount, taker_max_amount - cast(block_unixtime - order_start as double) / (order_end - order_start) * (taker_max_amount - taker_min_amount))) as taking_amount
                , order_hash
                , flags
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
            , '1inch' as project
            , method
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
        from {{ ref('oneinch_lop') }}
        where call_success
    )
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
        , any_value(block_time) as block_time
        , any_value(project) as project
        , any_value(call_selector) as call_selector
        , any_value(call_from) as call_from
        , any_value(call_to) as call_to
        , any_value(call_gas_used) as call_gas_used
        , any_value(maker) as maker
        , any_value(maker_asset) as maker_asset
        , any_value(taker_asset) as taker_asset
        , any_value(symbol) filter(where contract_address = maker_asset) as maker_asset_symbol
        , any_value(symbol) filter(where contract_address = taker_asset) as taker_asset_symbol
        , any_value(making_amount) as making_amount
        , any_value(taking_amount) as taking_amount
        , any_value(making_amount * price / pow(10, decimals)) filter(where contract_address = maker_asset) as making_amount_usd
        , any_value(taking_amount * price / pow(10, decimals)) filter(where contract_address = taker_asset) as taking_amount_usd
        , any_value(coalesce(order_hash, concat(tx_hash, to_big_endian_32(cast(counter as int))))) as order_hash
        , any_value(flags) as flags
    from (select * from orders, unnest(assets) as assets(contract_address))
    left join prices using(blockchain, contract_address, minute)
    group by 1, 2, 3, 4
)

-- output --

select
    blockchain
    , block_number
    , block_time
    , tx_hash
    , call_trace_address
    , project
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
    , greatest(coalesce(making_amount_usd, 0), coalesce(taking_amount_usd, 0)) as amount_usd
    , order_hash
    , flags
    , date(date_trunc('month', block_time)) as block_month
from joined