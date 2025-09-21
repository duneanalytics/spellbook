{{  
    config(
        schema = 'oneinch_evms',
        alias = 'limit_trades',
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'evt_index']
    )
}}



select
    blockchain
    , '1inch-LOP' as project
    , protocol_version as version
    , block_date
    , block_month
    , block_time
    , block_number
    , coalesce(src_executed_symbol, '') as token_bought_symbol
    , coalesce(dst_executed_symbol, '') as token_sold_symbol
    , case
        when lower(src_executed_symbol) > lower(dst_executed_symbol) then concat(dst_executed_symbol, '-', src_executed_symbol)
        else concat(src_executed_symbol, '-', dst_executed_symbol)
    end as token_pair
    , cast(src_executed_amount as double) / pow(10, cast(element_at(complement, 'src_decimals') as bigint)) as token_bought_amount
    , cast(dst_executed_amount as double) / pow(10, cast(element_at(complement, 'dst_decimals') as bigint)) as token_sold_amount
    , src_executed_amount as token_bought_amount_raw
    , dst_executed_amount as token_sold_amount_raw
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
from {{ ref('oneinch_executions') }}
where true
    and mode = 'limits'