{{  
    config(
        schema = 'oneinch',
        alias = 'lop_own_trades',
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'evt_index']
    )
}}



select
    blockchain
    , '1inch-LOP' as project
    , protocol_version as version
    , date(block_time) as block_date
    , block_month
    , block_time
    , block_number
    , coalesce(src_token_symbol, '') as token_bought_symbol
    , coalesce(dst_token_symbol, '') as token_sold_symbol
    , case
        when lower(dst_token_symbol) > lower(src_token_symbol) then concat(src_token_symbol, '-', dst_token_symbol)
        else concat(dst_token_symbol, '-', src_token_symbol)
    end as token_pair
    , cast(src_token_amount as double) / pow(10, src_token_decimals) as token_bought_amount
    , cast(dst_token_amount as double) / pow(10, dst_token_decimals) as token_sold_amount
    , src_token_amount as token_bought_amount_raw
    , dst_token_amount as token_sold_amount_raw
    , amount_usd
    , src_token_address as token_bought_address
    , dst_token_address as token_sold_address
    , call_from as taker
    , user as maker
    , call_to as project_contract_address
    , tx_hash
    , tx_from
    , tx_to
    , row_number() over(partition by tx_hash order by call_trace_address) as evt_index
from {{ ref('oneinch_swaps') }}
where
    protocol = 'LOP'
    and not flags['fusion']
    and not flags['second_side']
    and not flags['cross_chain']