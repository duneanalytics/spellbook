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

prices as (
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
        , decimals src_token_decimals
    from {{ ref('tokens_erc20') }}
)

, tokens_dst as (
    select 
        blockchain
        , contract_address dst_token_address
        , symbol dst_token_symbol
        , decimals dst_token_decimals
    from {{ ref('tokens_erc20') }}
)


-- base columns to not to duplicate in the union
{% set 
    calls_base_columns = [
        'blockchain',
        'block_number',
        'block_time',
        'tx_hash',
        'tx_from',
        'tx_to',
        'tx_nonce',
        'maker',
        'gas_price',
        'priority_fee_per_gas',
        'call_trace_address',
        'call_from',
        'call_to',
        'call_gas_used',
        'contract_name',
        'protocol',
        'protocol_version',
        'method',
        'order_hash',
        'receiver',
        'fusion',
        'remains',
    ]
%}

-- success calls & parsed tokens with native replaced by wrapped for amounts calculation
, calls_base as (
    select 
        *
        , if(src_token_address in {{native_addresses}}, wrapped_native_token_address, src_token_address) as _src_token_address
        , if(src_token_address in {{native_addresses}}, native_token_symbol) as _src_token_native
        , src_token_amount as _src_token_amount
        , if(dst_token_address in {{native_addresses}}, wrapped_native_token_address, dst_token_address) as _dst_token_address
        , if(dst_token_address in {{native_addresses}}, native_token_symbol) as _dst_token_native
        , dst_token_amount as _dst_token_amount
    from {{ ref('oneinch_calls') }}
    join {{ ref('oneinch_blockchains') }} using(blockchain)
    where
        tx_success
        and call_success
        {% if is_incremental() %}
            and {{ incremental_predicate('block_time') }}
        {% endif %}
)

-- union all calls with LOP opposite swaps
, _calls as (
    -- AR + LOP CALLS
    select 
        {{ calls_base_columns | join(', ') }}
        , _src_token_address as src_token_address
        , _src_token_native as src_token_native
        , _src_token_amount as src_token_amount
        , _dst_token_address as dst_token_address
        , _dst_token_native as dst_token_native
        , _dst_token_amount as dst_token_amount
        , false as second_side 
    from calls_base
    
    union all

    -- LOP calls - second side (when a direct call LOP method => users from two sides)
    select 
        {{ calls_base_columns | join(', ') }}
        , _dst_token_address as src_token_address
        , _dst_token_native as src_token_native
        , _dst_token_amount as src_token_amount
        , _src_token_address as dst_token_address
        , _src_token_native as dst_token_native
        , _src_token_amount as dst_token_amount
        , true as second_side -- opposite limit orders
    from calls_base
    where protocol = 'LOP' and cardinality(call_trace_address) = 0
)

-- calls with tokens metadata
, calls as (
    select 
        *
        , if(protocol = 'AR' or second_side, tx_from, maker) as user
        , if(protocol = 'LOP' and second_side = false and position('RFQ' in method) > 0, true, false) as contracts_only -- RFQ calls
    from _calls
    left join tokens_src using(blockchain, src_token_address)
    left join tokens_dst using(blockchain, dst_token_address)
)

, amounts as (
    select 
        blockchain
        , tx_hash
        , call_trace_address
        , block_number
        , max(amount) filter(where contract_address = src_token_address and amount <= src_token_amount) as src_token_amount -- take only amounts lte than in the call
        , max(amount) filter(where contract_address = dst_token_address and amount <= dst_token_amount) as dst_token_amount -- take only amounts lte than in the call
        
        , max(amount * price / pow(10, decimals)) filter(
            where 
                contract_address = src_token_address and amount <= src_token_amount 
                or contract_address = dst_token_address and amount <= dst_token_amount
        ) as sources_amount_usd -- take only amounts lte than in the call
        
        , max(amount * price / pow(10, decimals)) as transfers_amount_usd -- take all amounts

        , sum(amount * if(transfer_from = user, price, -price) / pow(10, decimals)) filter(
            where (
                src_token_native is not null and transfer_native 
                or src_token_address = contract_address
            )  
                and user in (transfer_from, transfer_to)
        ) as _amount_usd_from_user -- amount usd from user to any
        , sum(amount * if(transfer_to = user, price, -price) / pow(10, decimals)) filter(
            where (
                dst_token_native is not null and transfer_native 
                or dst_token_address = contract_address
            )  
                and user in (transfer_from, transfer_to)
        ) as _amount_usd_to_user -- amount usd to user from any
        , sum(amount * if(transfer_to = receiver, price, -price) / pow(10, decimals)) filter(
            where (
                dst_token_native is not null and transfer_native 
                or dst_token_address = contract_address
            )  
                and receiver in (transfer_from, transfer_to)
        ) as _amount_usd_to_receiver -- amount usd to receiver from any

        , count(distinct (contract_address, transfer_native)) as tokens -- count distinct tokens in transfers
        , count(*) as transfers -- count transfers
    from calls 
    join (
        select * from {{ ref('oneinch_call_transfers') }}
        {% if is_incremental() %}
            where {{ incremental_predicate('block_time') }}
        {% endif %}
    ) using(blockchain, tx_hash, call_trace_address, block_number) -- block_number is needed for performance
    left join prices using(blockchain, contract_address, minute)
    group by 1, 2, 3, 4
)



select 
    blockchain
    , block_number
    , block_time
    , tx_hash
    , tx_from
    , tx_to
    , tx_nonce
    , gas_price
    , priority_fee_per_gas
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
    , order_hash
    , remains
    , if(src_token_native is null, src_token_address, {{true_native_address}}) as src_token_address -- replace native with true native address back
    , coalesce(src_token_native, src_token_symbol) as src_token_symbol
    , src_token_decimals
    , amounts.src_token_amount
    , if(dst_token_native is null, dst_token_address, {{true_native_address}}) as dst_token_address -- replace native with true native address back
    , coalesce(dst_token_native, dst_token_symbol) as dst_token_symbol
    , dst_token_decimals
    , amounts.dst_token_amount
    , coalesce(sources_amount_usd, transfers_amount_usd) as amount_usd -- sources amount first if found prices, when connector tokens
    , sources_amount_usd
    , transfers_amount_usd
    , greatest(coalesce(_amount_usd_from_user, 0), coalesce(_amount_usd_to_user, _amount_usd_to_receiver, 0)) as user_amount_usd
    , tokens
    , transfers
    , date_trunc('minute', block_time) as minute
    , date(date_trunc('month', block_time)) as block_month
from calls
join amounts using(blockchain, tx_hash, call_trace_address, block_number)
