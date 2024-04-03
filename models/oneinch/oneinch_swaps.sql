{{  
    config(
        schema = 'oneinch',
        alias = 'swaps',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        partition_by = ['block_month'],
        unique_key = ['unique_key'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "polygon", "arbitrum", "avalanche_c", "gnosis", "fantom", "optimism", "base"]\',
                                "project",
                                "oneinch",
                                \'["max-morrow", "grkhr"]\') }}'
    )
}}



{% set native_addresses = '(0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee)' %}
{% set true_native_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %}

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
        'tx_gas_used',
        'tx_gas_price',
        'tx_priority_fee_per_gas',
        'call_trace_address',
        'call_from',
        'call_to',
        'call_gas_used',
        'call_type',
        'contract_name',
        'protocol',
        'protocol_version',
        'method',
        'order_hash',
        'flags',
        'remains',
        'native_token_symbol',
        '_call_trace_address',
    ]
%}



with

tokens as (
    select 
        blockchain
        , contract_address
        , symbol as token_symbol
        , decimals as token_decimals
    from {{ source('tokens', 'erc20') }}
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
        *
        , if(src_token_address in {{native_addresses}}, wrapped_native_token_address, src_token_address) as _src_token_address
        , if(src_token_address in {{native_addresses}}, true, false) as _src_token_native
        , if(dst_token_address in {{native_addresses}}, wrapped_native_token_address, dst_token_address) as _dst_token_address
        , if(dst_token_address in {{native_addresses}}, true, false) as _dst_token_native
        , array_join(call_trace_address, '') as _call_trace_address
    from {{ ref('oneinch_calls') }}
    join {{ ref('oneinch_blockchains') }} using(blockchain)
    where
        tx_success
        and call_success
        {% if is_incremental() %}
            and {{ incremental_predicate('block_time') }}
        {% endif %}
)

, swaps as (
    -- AR & LOP calls
    select 
        {{ calls_base_columns | join(', ') }}
        , if(protocol = 'AR', tx_from, maker) as user
        , receiver
        , _src_token_address
        , _src_token_native
        , src_token_amount
        , _dst_token_address
        , _dst_token_native
        , dst_token_amount
        , false as second_side
        , protocol = 'LOP' and (
            position('RFQ' in method) > 0
            or coalesce(not element_at(flags, 'multiple') and element_at(flags, 'partial'), false)
        ) as contracts_only
    from calls

    union all

    -- second side of LOP calls (when a direct call LOP method => users from two sides)
    select
        {{ calls_base_columns | join(', ') }}
        , tx_from as user
        , null as receiver
        , _dst_token_address as _src_token_address
        , _dst_token_native as _src_token_native
        , dst_token_amount as src_token_amount
        , _src_token_address as _dst_token_address
        , _src_token_native as _dst_token_native
        , src_token_amount as dst_token_amount
        , true as second_side
        , false as contracts_only
    from calls
    where
        protocol = 'LOP'
        and cardinality(call_trace_address) = 0
)

{% set src_condition = 'contract_address = _src_token_address' %}
{% set dst_condition = 'contract_address = _dst_token_address' %}
{% set symbol = 'coalesce(symbol, token_symbol)' %}
{% set decimals = 'coalesce(decimals, token_decimals)' %}

, amounts as (
    select 
        blockchain
        , block_number
        , tx_hash
        , call_trace_address
        , second_side

        -- what the user actually gave and received, judging by the transfers
        , any_value(if(transfer_native, {{ true_native_address }}, contract_address)) filter(where {{ src_condition }} and transfer_from = user) as _src_token_address_true
        , any_value(if(transfer_native, {{ true_native_address }}, contract_address)) filter(where {{ dst_condition }} and transfer_to = user) as _dst_token_address_to_user
        , any_value(if(transfer_native, {{ true_native_address }}, contract_address)) filter(where {{ dst_condition }} and transfer_to = receiver) as _dst_token_address_to_receiver
        , any_value(if(transfer_native, native_token_symbol, {{ symbol }})) filter(where {{ src_condition }} and transfer_from = user) as _src_token_symbol_true
        , any_value(if(transfer_native, native_token_symbol, {{ symbol }})) filter(where {{ dst_condition }} and transfer_to = user) as _dst_token_symbol_to_user
        , any_value(if(transfer_native, native_token_symbol, {{ symbol }})) filter(where {{ dst_condition }} and transfer_to = receiver) as _dst_token_symbol_to_receiver
        , any_value({{ decimals }}) filter(where {{ src_condition }}) as src_token_decimals
        , any_value({{ decimals }}) filter(where {{ dst_condition }}) as dst_token_decimals

        -- reinsurance for symbols (when transfers from/to user are not found)
        , any_value(if(transfer_native, native_token_symbol, {{ symbol }})) filter(where {{ src_condition }}) as _src_token_symbol
        , any_value(if(transfer_native, native_token_symbol, {{ symbol }})) filter(where {{ dst_condition }}) as _dst_token_symbol

        , max(amount) filter(where {{ src_condition }} and amount <= src_token_amount) as _src_token_amount_true -- take only src token amounts less than in the call
        , max(amount) filter(where {{ dst_condition }} and amount <= dst_token_amount) as _dst_token_amount_true -- take only dst token amounts less than in the call
        , max(amount * price / pow(10, decimals)) filter(where {{ src_condition }} and amount <= src_token_amount or {{ dst_condition }} and amount <= dst_token_amount) as sources_amount_usd
        , max(amount * price / pow(10, decimals)) as transfers_amount_usd

        -- src $ amount from user
        , sum(amount * if(user = transfer_from, price, -price) / pow(10, decimals)) filter(where {{ src_condition }} and user in (transfer_from, transfer_to)) as _amount_usd_from_user
        -- dst $ amount to user
        , sum(amount * if(user = transfer_to, price, -price) / pow(10, decimals)) filter(where {{ dst_condition }} and user in (transfer_from, transfer_to)) as _amount_usd_to_user
        -- dst $ amount to receiver
        , sum(amount * if(receiver = transfer_to, price, -price) / pow(10, decimals)) filter(where {{ dst_condition }} and receiver in (transfer_from, transfer_to)) as _amount_usd_to_receiver

        , count(distinct (contract_address, transfer_native)) as tokens -- count distinct tokens in transfers
        , count(*) as transfers -- count transfers
    from swaps 
    join (
        select * from {{ ref('oneinch_call_transfers') }}
        {% if is_incremental() %}
            where {{ incremental_predicate('block_time') }}
        {% endif %}
    ) using(blockchain, block_number, tx_hash, call_trace_address) -- block_number is needed for performance
    left join prices using(blockchain, contract_address, minute)
    left join tokens using(blockchain, contract_address)
    group by 1, 2, 3, 4, 5
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
    , tx_gas_used
    , tx_gas_price
    , tx_priority_fee_per_gas
    , contract_name
    , protocol
    , protocol_version
    , method
    , call_trace_address
    , call_from
    , call_to
    , call_gas_used
    , call_type
    , user
    , receiver
    , coalesce(element_at(flags, 'fusion'), false) as fusion -- to delete in the next step
    , not second_side and (position('RFQ' in method) > 0 or coalesce(not element_at(flags, 'multiple') and element_at(flags, 'partial'), false)) as contracts_only -- to delete in the next step
    , second_side -- to delete in the next step
    , order_hash
    , map_concat(flags, map_from_entries(array[
        ('direct', cardinality(call_trace_address) = 0)
        , ('second_side', second_side)
        , ('contracts_only', contracts_only)
    ])) as flags
    , remains
    , coalesce(_src_token_address_true, if(_src_token_native, {{ true_native_address }}, _src_token_address)) as src_token_address
    , coalesce(_src_token_symbol_true, _src_token_symbol) as src_token_symbol
    , src_token_decimals
    , coalesce(_src_token_amount_true, src_token_amount) as src_token_amount
    , coalesce(_dst_token_address_to_user, _dst_token_address_to_receiver, if(_dst_token_native, {{ true_native_address }}, _dst_token_address)) as dst_token_address
    , coalesce(_dst_token_symbol_to_user, _dst_token_symbol_to_receiver, _dst_token_symbol) as dst_token_symbol
    , dst_token_decimals
    , coalesce(_dst_token_amount_true, dst_token_amount) as dst_token_amount
    , coalesce(sources_amount_usd, transfers_amount_usd) as amount_usd -- sources $ amount first if found prices, then $ amount of connector tokens
    , sources_amount_usd
    , transfers_amount_usd
    , greatest(coalesce(_amount_usd_from_user, 0), coalesce(_amount_usd_to_user, _amount_usd_to_receiver, 0)) as user_amount_usd -- actual user $ amount
    , tokens
    , transfers
    , date_trunc('minute', block_time) as minute
    , date(date_trunc('month', block_time)) as block_month
    , coalesce(order_hash, tx_hash || from_hex(if(mod(length(_call_trace_address), 2) = 1, '0' || _call_trace_address, _call_trace_address) || '0' || cast(cast(second_side as int) as varchar))) as swap_id
    , {{dbt_utils.generate_surrogate_key(["blockchain", "tx_hash", "array_join(call_trace_address, ',')", "second_side"])}} as unique_key
from swaps
join amounts using(blockchain, block_number, tx_hash, call_trace_address, second_side)