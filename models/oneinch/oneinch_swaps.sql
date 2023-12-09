{{  
    config(
        schema = 'oneinch',
        alias = 'swaps',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        partition_by = ['block_month'],
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address', 'second_side'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum", "base"]\',
                                "project",
                                "oneinch",
                                \'["max-morrow", "grkhr"]\') }}'
    )
}}



{% set true_native_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %}
{% set columns = [
    'blockchain',
    'block_number',
    'block_time',
    'tx_hash',
    'tx_from',
    'tx_to',
    'tx_nonce',
    'gas_price',
    'priority_fee',
    'contract_name',
    'protocol',
    'protocol_version',
    'method',
    'call_selector',
    'call_trace_address',
    'call_from',
    'call_to',
    'call_gas_used',
    'maker',
    'receiver',
    'fusion',
    'remains',
    'sources_amount_usd',
    'transfers_amount_usd',
    'transfers',
    'explorer_link'
] %}
{% set columns = columns | join(', ') %}



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
        , block_number
        , tx_hash
        , call_trace_address

        , any_value(block_time) as block_time
        , any_value(tx_from) as tx_from
        , any_value(tx_to) as tx_to
        , any_value(tx_nonce) as tx_nonce
        , any_value(gas_price) as gas_price
        , any_value(contract_name) as contract_name
        , any_value(protocol) as protocol
        , any_value(protocol_version) as protocol_version
        , any_value(method) as method
        , any_value(call_selector) as call_selector
        , any_value(call_from) as call_from
        , any_value(call_to) as call_to
        , any_value(call_gas_used) as call_gas_used
        , any_value(maker) as maker
        , any_value(receiver) as receiver
        , any_value(fusion) as fusion
        , any_value(remains) as remains

        , any_value(if(src_native is null, src_token_address, {{true_native_address}})) filter(where contract_address = src_token_address) as src_token_address
        , any_value(if(dst_native is null, dst_token_address, {{true_native_address}})) filter(where contract_address = dst_token_address) as dst_token_address
        , any_value(coalesce(src_native, coalesce(symbol, token_symbol))) filter(where contract_address = src_token_address) as src_token_symbol
        , any_value(coalesce(dst_native, coalesce(symbol, token_symbol))) filter(where contract_address = dst_token_address) as dst_token_symbol
        , any_value(coalesce(decimals, token_decimals)) filter(where contract_address = src_token_address) as src_token_decimals
        , any_value(coalesce(decimals, token_decimals)) filter(where contract_address = dst_token_address) as dst_token_decimals
        , max(amount) filter(where contract_address = src_token_address and amount <= src_amount) as src_amount
        , max(amount) filter(where contract_address = dst_token_address and amount <= dst_amount) as dst_amount
        , max(amount * price / pow(10, decimals)) filter(where contract_address = src_token_address and amount <= src_amount or contract_address = dst_token_address and amount <= dst_amount) as sources_amount_usd
        , max(amount * price / pow(10, decimals)) as transfers_amount_usd
        , count(*) as transfers
        , any_value(explorer_link) as explorer_link
    from {{ ref('oneinch_calls_transfers') }}
    left join prices using(blockchain, contract_address, minute)
    left join tokens using(blockchain, contract_address)
    {% if is_incremental() %}
        where {{ incremental_predicate('block_time') }}
    {% endif %}
    group by 1, 2, 3, 4
)

, sides as (
    select
        {{ columns }}
        , if(protocol = 'LOP', maker, tx_from) as user
        , src_token_address
        , src_token_symbol
        , src_token_decimals
        , src_amount
        , dst_token_address
        , dst_token_symbol
        , dst_token_decimals
        , dst_amount
        , position('RFQ' in method) > 0 as contracts_only
        , false as second_side
    from calls

    union all
    -- when a direct call LOP method => users from two sides
    select
        {{ columns }}
        , tx_from as user
        , dst_token_address as src_token_address
        , dst_token_symbol as src_token_symbol
        , dst_token_decimals as src_token_decimals
        , dst_amount as src_amount
        , src_token_address as dst_token_address
        , src_token_symbol as dst_token_symbol
        , src_token_decimals as dst_token_decimals
        , src_amount as dst_amount
        , false as contracts_only
        , true as second_side
    from calls
    where
        protocol = 'LOP'
        and cardinality(call_trace_address) = 0
)

-- output --

select
    blockchain
    , block_number
    , block_time
    , tx_hash
    , tx_from
    , tx_to
    , tx_nonce
    , contract_name
    , protocol
    , protocol_version
    , method
    , call_trace_address
    , call_from
    , call_to
    , call_gas_used
    , user
    , receiver
    , fusion
    , contracts_only
    , second_side
    , remains
    , src_token_address
    , dst_token_address
    , src_token_symbol
    , dst_token_symbol
    , src_token_decimals
    , dst_token_decimals
    , src_amount
    , dst_amount
    , coalesce(sources_amount_usd, transfers_amount_usd) as amount_usd
    , sources_amount_usd
    , transfers_amount_usd
    , transfers
    , explorer_link
    , date(date_trunc('month', block_time)) as block_month
from sides