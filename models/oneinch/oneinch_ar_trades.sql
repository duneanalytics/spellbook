{{  
    config(
        schema = 'oneinch',
        alias = 'ar_trades',
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'trace_address']
    )
}}



select
    blockchain
    , '1inch' as project
    , 'AR v' || protocol_version as version
    , date_trunc('day', block_time) as block_date
    , date(date_trunc('month', block_time)) as block_month
    , block_time
    , coalesce(dst_token_symbol, '') as token_bought_symbol
    , coalesce(src_token_symbol, '') as token_sold_symbol
    , array_join(array_sort(array[coalesce(src_token_symbol, ''), coalesce(dst_token_symbol, '')]), '-') as token_pair
    , cast(dst_token_amount as double) / pow(10, dst_token_decimals) as token_bought_amount
    , cast(src_token_amount as double) / pow(10, src_token_decimals) as token_sold_amount
    , dst_token_amount as token_bought_amount_raw
    , src_token_amount as token_sold_amount_raw
    , amount_usd
    , dst_token_address as token_bought_address
    , src_token_address as token_sold_address
    , user as taker
    , cast(null as varbinary) as maker
    , call_to as project_contract_address
    , tx_hash
    , tx_from
    , tx_to
    , call_trace_address as trace_address
    , -1 as evt_index
from {{ ref('oneinch_swaps') }}
where protocol = 'AR'
