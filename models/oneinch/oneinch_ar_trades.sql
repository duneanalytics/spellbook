{{  
    config(
        schema = 'oneinch',
        alias = 'ar_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        partition_by = ['block_month'],
        unique_key = ['blockchain', 'tx_hash', 'trace_address']
    )
}}



{% set native_addresses = '(0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee)' %}
{% set true_native_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %}
{% set src_condition = '(src_token_address in (0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee) and transfer_native or src_token_address = contract_address)' %}
{% set dst_condition = '(dst_token_address in (0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee) and transfer_native or dst_token_address = contract_address)' %}



with

tokens as (
    select 
        blockchain
        , contract_address
        , symbol as token_symbol
        , decimals as token_decimals
    from {{ ref('tokens_erc20') }}
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

, calls as (
    select
        blockchain
        , block_time
        , tx_hash
        , call_trace_address
        
        , any_value(tx_from) as tx_from
        , any_value(tx_to) as tx_to
        , any_value(tx_nonce) as tx_nonce
        , any_value(protocol_version) as protocol_version
        
        , any_value(if(src_token_address in {{ native_addresses }}, {{ true_native_address }}, src_token_address)) as src_token_address
        , any_value(if(dst_token_address in {{ native_addresses }}, {{ true_native_address }}, dst_token_address)) as dst_token_address
        , any_value(if(src_token_address in {{native_addresses}}, native_symbol, coalesce(symbol, token_symbol))) filter(where {{ src_condition }}) as src_token_symbol
        , any_value(if(dst_token_address in {{native_addresses}}, native_symbol, coalesce(symbol, token_symbol))) filter(where {{ dst_condition }}) as dst_token_symbol
        , any_value(coalesce(decimals, token_decimals)) filter(where {{ src_condition }}) as src_token_decimals
        , any_value(coalesce(decimals, token_decimals)) filter(where {{ dst_condition }}) as dst_token_decimals
        , max(amount) filter(where {{ src_condition }} and amount <= src_amount) as src_amount
        , max(amount) filter(where {{ dst_condition }} and amount <= dst_amount) as dst_amount
        , max(amount * price / pow(10, decimals)) filter(where {{ src_condition }} and amount <= src_amount or {{ dst_condition }} and amount <= dst_amount) as sources_amount_usd
        , max(amount * price / pow(10, decimals)) as transfers_amount_usd
    from {{ ref('oneinch_calls_transfers') }}
    left join prices using(blockchain, contract_address, minute)
    left join tokens using(blockchain, contract_address)
    where
        protocol = 'AR'
        {% if is_incremental() %}
            and {{ incremental_predicate('block_time') }}
        {% endif %}
    group by 1, 2, 3, 4
)

-- output --

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
    , cast(dst_amount as double) / pow(10, dst_token_decimals) as token_bought_amount
    , cast(src_amount as double) / pow(10, src_token_decimals) as token_sold_amount
    , dst_amount as token_bought_amount_raw
    , src_amount as token_sold_amount_raw
    , coalesce(sources_amount_usd, transfers_amount_usd) as amount_usd
    , dst_token_address as token_bought_address
    , src_token_address as token_sold_address
    , tx_from as taker
    , cast(null as varbinary) as maker
    , call_to as project_contract_address
    , tx_hash
    , tx_from
    , tx_to
    , call_trace_address as trace_address
    , -1 as evt_index
from trades