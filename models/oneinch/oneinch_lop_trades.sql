{{  
    config(
        schema = 'oneinch',
        alias = alias('lop_trades'),
        materialized = 'incremental',
        partition_by = ['block_month'],
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address'],
        tags = ['dunesql'],
    )
}}

{% set lookback_days = -7 %}

with
    
methods as (
    select
        contract_address
        , contract_name
        , blockchain
        , created_at
        , namespaces
        , names
        , json_value(entity, 'lax $.name') as method
        , position('actualMakingAmount' in entity) > 0 as outputs_sign
        , position('order_' in entity) > 0 as order_sign
        , position('interaction' in entity) > 0 as interaction_sign
    from {{ ref('oneinch_exchange_contracts') }}, unnest(abi) as abi(entity)
    where project = '1inch'
        and json_value(entity, 'lax $.type') = 'function'
        and json_value(entity, 'lax $.stateMutability') in ('payable', 'nonpayable')
        and position('fill' in lower(json_value(entity, 'lax $.name'))) > 0
)

, orders as (
    
    {% for row in methods %}
        
        select
            orders.blockchain
            , transactions.block_time
            , date_trunc('day', transactions.block_time) as block_date
            , date_trunc('month', transactions.block_time) as block_month
            , date_trunc('minute', transactions.block_time) as minute
            , '1inch LOP' as project
            , orders.version
            , orders.contract_address as project_contract_address
            , hash as tx_hash
            , transactions."from" as tx_from
            , transactions."to" as tx_to
            , orders.maker
            , from_hex(orders.maker_asset) as src_token_address
            , orders.making_amount as src_amount
            , from_hex(orders.taker_asset) as dst_token_address
            , orders.taking_amount as dst_amount
            , orders.call_trace_address
        from (
            select
                blockchain
                , call_tx_hash as hash
                , call_block_time
                , contract_address
                , call_trace_address
                , call_success
                , order_map['maker'] as maker
                , order_map['makerAsset'] as maker_asset
                , making_amount
                , order_map['takerAsset'] as taker_asset
                , taking_amount
                , cast(cast(substr(contract_name, length(contract_name)) as double) - if(position('limit' in lower(contract_name)) > 0, 0, 2) as varchar) as version
            from (
                select
                    row.blockchain
                    , call_tx_hash
                    , call_block_time
                    , contract_address
                    , call_trace_address
                    , call_success
                    , if(outputs_sign, output_filledMakingAmount, output_0) as making_amount
                    , if(outputs_sign, output_filledTakingAmount, output_1) as taking_amount
                    , cast(json_parse(if(order_sign, "order_", "order")) as map(varchar, varchar)) as order_map
                from {{ source('oneinch_' + row.blockchain, row.contract_name + '_call_' + row.method) }}
                where
                    {% if is_incremental() %}
                        call_block_time >= cast(date_add('day', {{ lookback_days }}, current_timestamp) as timestamp)
                    {% endif %}
            )
        ) as orders
        join {{ source(row.blockchain, 'transactions') }} using(hash)
        where transactions.success and orders.call_success
        
        {% if not loop.last %}
            union all
        {% endif %}

    {% endfor %}
)

, prices_src as (
    select
        blockchain
        , contract_address as src_token_address
        , minute
        , symbol as src_symbol
        , decimals as src_decimals
        , price as src_price
    from {{ source('prices', 'usd') }}
    where
        {% if is_incremental() %}
            minute >= cast(date_add('day', {{ lookback_days }}, current_timestamp) as timestamp)
        {% endif %}
)

, prices_dst as (
    select
        blockchain
        , contract_address as dst_token_address
        , minute
        , symbol as dst_symbol
        , decimals as dst_decimals
        , price as dst_price
    from {{ source('prices', 'usd') }}
    where
        {% if is_incremental() %}
            minute >= cast(date_add('day', {{ lookback_days }}, current_timestamp) as timestamp)
        {% endif %}
)

, additions as (
    select
        blockchain
        , block_time
        , block_date
        , block_month
        , project
        , version
        , project_contract_address
        , tx_hash
        , tx_from
        , tx_to
        , maker
        , null as taker
        , coalesce(src_symbol, '') || '-' || coalesce(dst_symbol, '') as token_pair
        , src_token_address as token_sold_address
        , coalesce(src_symbol, '') as token_sold_symbol
        , src_amount as token_sold_amount_raw
        , cast(src_amount as double) / pow(10, src_decimals) as token_sold_amount
        , dst_token_address as token_bought_address
        , coalesce(dst_symbol, '') as token_bought_symbol
        , dst_amount as token_bought_amount_raw
        , cast(dst_amount as double) / pow(10, dst_decimals) as token_bought_amount
        , coalesce(cast(src_amount as double) / pow(10, src_decimals) * src_price, cast(dst_amount as double) / pow(10, dst_decimals) * dst_price) as amount_usd
        , null as evt_index
        , call_trace_address
    from orders
    left join prices_src using(blockchain, src_token_address, minute)
    left join prices_dst using(blockchain, dst_token_address, minute)
)

select *
from additions