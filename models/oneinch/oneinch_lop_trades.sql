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
    
    orders as (
        select
            blockchain
            , block_time
            , date_trunc('day', transactions.block_time) as block_date
            , date_trunc('month', transactions.block_time) as block_month
            , minute
            , tx_hash
            , tx_from
            , tx_to
            , call_to as project_contract_address
            , protocol_version as version
            , call_trace_address
            , maker
            , maker_asset as src_token_address
            , making_amount as src_amount
            , call_from as taker
            , taker_asset as dst_token_address
            , taking_amount as dst_amount
            , '1inch LOP' as project
        from {{ ref('oneinch_lop') }}
        left join (
            select blockchain, contract_address as call_from, true as fusion
            from {{ ref('oneinch_fusion_settlements') }}
        ) using(blockchain, call_from)
        where tx_success and call_success and fusion is null
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
        {% if is_incremental() %}
            where minute >= cast(date_add('day', {{ lookback_days }}, current_timestamp) as timestamp)
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
        {% if is_incremental() %}
            where minute >= cast(date_add('day', {{ lookback_days }}, current_timestamp) as timestamp)
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