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
    , coalesce(dst_token_symbol, '') as token_bought_symbol
    , coalesce(src_token_symbol, '') as token_sold_symbol
    , case
        when lower(src_token_symbol) > lower(dst_token_symbol) then concat(dst_token_symbol, '-', src_token_symbol)
        else concat(src_token_symbol, '-', dst_token_symbol)
    end as token_pair
    , cast(dst_token_amount as double) / pow(10, dst_token_decimals) as token_bought_amount
    , cast(src_token_amount as double) / pow(10, src_token_decimals) as token_sold_amount
    , dst_token_amount as token_bought_amount_raw
    , src_token_amount as token_sold_amount_raw
    , amount_usd
    , dst_token_address as token_bought_address
    , src_token_address as token_sold_address
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