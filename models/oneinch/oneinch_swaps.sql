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
                                \'["grkhr", "max-morrow"]\') }}'
    )
}}



{% set native_addresses = '(0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee)' %}
{% set true_native_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %}



with

settlements as (
    select
        blockchain
        , contract_address as call_from
        , true as fusion
    from {{ ref('oneinch_fusion_settlements') }}
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

, tokens_src as (
    select 
        blockchain
        , contract_address src_token_address
        , symbol src_token_symbol
        , decimals src_decimals
    from {{ ref('tokens_erc20') }}
)

, tokens_dst as (
    select 
        blockchain
        , contract_address dst_token_address
        , symbol dst_token_symbol
        , decimals dst_decimals
    from {{ ref('tokens_erc20') }}
)

, _calls as (
    -- AR calls
    select
        blockchain
        , tx_hash
        , call_trace_address
        , contract_name
        , 'AR' as protocol
        , protocol_version
        , method
        , tx_from as user
        , dst_receiver as receiver
        , if(src_token_address in {{native_addresses}}, wrapped_native_token_address, src_token_address) as src_token_address
        , if(src_token_address in {{native_addresses}}, native_token_symbol) as src_native
        , src_amount
        , if(dst_token_address in {{native_addresses}}, wrapped_native_token_address, dst_token_address) as dst_token_address
        , if(dst_token_address in {{native_addresses}}, native_token_symbol) as dst_native
        , dst_amount
        , false as fusion
        , false as contracts_only
        , false as second_side
        , explorer_link
        , minute
    from {{ ref('oneinch_ar') }}
    join {{ ref('evms_info') }} using(blockchain)
    where
        tx_success
        and call_success
        {% if is_incremental() %}
            and {{ incremental_predicate('block_time') }}
        {% endif %}


    union all
    -- LOP calls - first side (when a direct call LOP method => users from two sides)
    select
        blockchain
        , tx_hash
        , call_trace_address
        , contract_name
        , 'LOP' as protocol
        , protocol_version
        , method
        , maker as user
        , receiver
        , if(maker_asset in {{native_addresses}}, wrapped_native_token_address, maker_asset) as src_token_address
        , if(maker_asset in {{native_addresses}}, native_token_symbol) as src_native
        , making_amount as src_amount
        , if(taker_asset in {{native_addresses}}, wrapped_native_token_address, taker_asset) as dst_token_address
        , if(taker_asset in {{native_addresses}}, native_token_symbol) as dst_native
        , taking_amount as dst_amount
        , coalesce(fusion, false) as fusion
        , position('RFQ' in method) > 0 as contracts_only
        , false as second_side
        , explorer_link
        , minute
    from {{ ref('oneinch_lop') }}
    join {{ ref('evms_info') }} using(blockchain)
    left join settlements using(blockchain, call_from)
    where
        tx_success
        and call_success
        {% if is_incremental() %}
            and {{ incremental_predicate('block_time') }}
        {% endif %}


    union all
    -- LOP calls - second side (when a direct call LOP method => users from two sides)
    select
        blockchain
        , tx_hash
        , call_trace_address
        , contract_name
        , 'LOP' as protocol
        , protocol_version
        , method
        , tx_from as user
        , receiver
        , if(taker_asset in {{native_addresses}}, wrapped_native_token_address, taker_asset) as src_token_address
        , if(taker_asset in {{native_addresses}}, native_token_symbol) as src_native
        , taking_amount as src_amount
        , if(maker_asset in {{native_addresses}}, wrapped_native_token_address, maker_asset) as dst_token_address
        , if(maker_asset in {{native_addresses}}, native_token_symbol) as dst_native
        , making_amount as dst_amount
        , false as fusion
        , false as contracts_only
        , true as second_side
        , explorer_link
        , minute
    from {{ ref('oneinch_lop') }}
    join {{ ref('evms_info') }} using(blockchain)
    where
        tx_success
        and call_success
        and cardinality(call_trace_address) = 0
        {% if is_incremental() %}
            and {{ incremental_predicate('block_time') }}
        {% endif %}
)

, calls as (
    select * from _calls
    left join tokens_src using(blockchain, src_token_address)
    left join tokens_dst using(blockchain, dst_token_address)
)

, swaps as (
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
        , any_value(call_from) as call_from
        , any_value(call_to) as call_to
        , any_value(call_gas_used) as call_gas_used
        , any_value(user) as user
        , any_value(receiver) as receiver
        , any_value(fusion) as fusion
        , any_value(contracts_only) as contracts_only
        , any_value(second_side) as second_side
        , any_value(call_remains) as remains

        , any_value(if(src_native is null, src_token_address, {{true_native_address}})) filter(where contract_address = src_token_address) as src_token_address
        , any_value(if(dst_native is null, dst_token_address, {{true_native_address}})) filter(where contract_address = dst_token_address) as dst_token_address
        , max(amount) filter(where contract_address = src_token_address and amount <= src_amount) as src_amount
        , max(amount) filter(where contract_address = dst_token_address and amount <= dst_amount) as dst_amount
        , any_value(coalesce(src_native, src_token_symbol)) filter(where contract_address = src_token_address) as src_token_symbol
        , any_value(coalesce(dst_native, dst_token_symbol)) filter(where contract_address = dst_token_address) as dst_token_symbol
        , max(amount * price / pow(10, decimals)) filter(where contract_address = src_token_address and amount <= src_amount or contract_address = dst_token_address and amount <= dst_amount) as sources_amount_usd
        , max(amount * price / pow(10, decimals)) as transfers_amount_usd
        , count(*) as transfers
        , any_value(explorer_link) as explorer_link

    from (
        select
            blockchain
            , block_number
            , tx_hash
            , block_time
            , tx_from
            , tx_to
            , tx_nonce
            , gas_price
            , priority_fee
            , call_trace_address
            , call_from
            , call_to
            , contract_address
            , minute
            , amount
            , call_gas_used
            , call_remains
        from {{ ref('oneinch_calls_transfers_amounts') }}
        {% if is_incremental() %}
            where {{ incremental_predicate('block_time') }}
        {% endif %}
    )
    join calls using(blockchain, tx_hash, call_trace_address, minute)
    left join prices using(blockchain, contract_address, minute)
    group by 1, 2, 3, 4
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
    , src_amount
    , dst_amount
    , src_token_symbol
    , dst_token_symbol
    , coalesce(sources_amount_usd, transfers_amount_usd) as amount_usd
    , sources_amount_usd
    , transfers_amount_usd
    , transfers
    , explorer_link
    , date(date_trunc('month', block_time)) as block_month
from swaps