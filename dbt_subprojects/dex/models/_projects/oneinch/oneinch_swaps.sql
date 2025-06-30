{{
    config(
        schema = 'oneinch',
        alias = 'swaps',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        partition_by = ['block_month'],
        unique_key = ['unique_key'],
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "polygon", "arbitrum", "avalanche_c", "gnosis", "fantom", "optimism", "base", "zksync", "linea", "sonic", "unichain"]\',
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
        'src_escrow',
        'hashlock',
        'dst_blockchain',
        'dst_block_number',
        'dst_block_time',
        'dst_tx_hash',
        'dst_escrow',
    ]
%}



with

calls as (
    select
        *
        , if(src_token_address in {{native_addresses}}, wrapped_native_token_address, src_token_address) as _src_token_address
        , if(src_token_address in {{native_addresses}}, true, false) as _src_token_native
        , if(dst_token_address in {{native_addresses}}, coalesce(dst_wrapper, wrapped_native_token_address), dst_token_address) as _dst_token_address
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
            or coalesce(element_at(flags, 'partial') and not element_at(flags, 'multiple'), false)
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
        and flags['direct']
)

{% set src_condition = 'contract_address = _src_token_address' %}
{% set dst_condition = 'contract_address = _dst_token_address' %}

, amounts as (
    select
        blockchain
        , block_number
        , tx_hash
        , call_trace_address
        , second_side

        -- what the user actually gave and received, judging by the transfers
        , any_value(if(transfer_native, {{ true_native_address }}, contract_address)) filter(where {{ src_condition }} and transfer_from = user) as _src_token_address_from_user
        , any_value(if(transfer_native, {{ true_native_address }}, contract_address)) filter(where {{ src_condition }} and transfer_from = call_from) as _src_token_address_from_caller -- when there were no transfers inside the AR indirect call from the user (tx_from <> caller), but there were from the caller
        , any_value(if(transfer_native, {{ true_native_address }}, contract_address)) filter(where {{ dst_condition }} and transfer_to = user) as _dst_token_address_to_user
        , any_value(if(transfer_native, {{ true_native_address }}, contract_address)) filter(where {{ dst_condition }} and transfer_to = receiver) as _dst_token_address_to_receiver
        , any_value(if(transfer_native, {{ true_native_address }}, contract_address)) filter(where {{ dst_condition }} and transfer_to = call_from) as _dst_token_address_to_caller  -- when there were no transfers inside the AR indirect call to the user (tx_from <> caller), but there were to the caller
        , any_value(if(transfer_native, native_token_symbol, symbol)) filter(where {{ src_condition }} and transfer_from = user) as _src_token_symbol_from_user
        , any_value(if(transfer_native, native_token_symbol, symbol)) filter(where {{ src_condition }} and transfer_from = call_from) as _src_token_symbol_from_caller
        , any_value(if(transfer_native, native_token_symbol, symbol)) filter(where {{ dst_condition }} and transfer_to = user) as _dst_token_symbol_to_user
        , any_value(if(transfer_native, native_token_symbol, symbol)) filter(where {{ dst_condition }} and transfer_to = receiver) as _dst_token_symbol_to_receiver
        , any_value(if(transfer_native, native_token_symbol, symbol)) filter(where {{ dst_condition }} and transfer_to = call_from) as _dst_token_symbol_to_caller
        , any_value(decimals) filter(where {{ src_condition }}) as src_token_decimals
        , any_value(decimals) filter(where {{ dst_condition }}) as dst_token_decimals

        -- reinsurance for symbols (when transfers from/to user are not found)
        , any_value(if(transfer_native, native_token_symbol, symbol)) filter(where {{ src_condition }}) as _src_token_symbol
        , any_value(if(transfer_native, native_token_symbol, symbol)) filter(where {{ dst_condition }}) as _dst_token_symbol

        , max(amount) filter(where {{ src_condition }} and amount <= src_token_amount) as _src_token_amount_true -- take only src token amounts less than in the call
        , max(amount) filter(where {{ dst_condition }} and amount <= dst_token_amount) as _dst_token_amount_true -- take only dst token amounts less than in the call
        , max(amount_usd) filter(where {{ src_condition }} and amount <= src_token_amount or {{ dst_condition }} and amount <= dst_token_amount) as sources_amount_usd
        , max(amount_usd) filter(where {{ src_condition }} and amount <= src_token_amount) as src_token_amount_usd
        , max(amount_usd) filter(where {{ dst_condition }} and amount <= dst_token_amount) as dst_token_amount_usd
        , max(amount_usd) filter(where ({{ src_condition }} and amount <= src_token_amount or {{ dst_condition }} and amount <= dst_token_amount) and trusted) as sources_amount_usd_trusted
        , max(amount_usd) as transfers_amount_usd
        , max(amount_usd) filter(where trusted) as transfers_amount_usd_trusted

        -- src $ amount from user
        , sum(amount_usd * if(user = transfer_from, 1, -1)) filter(where {{ src_condition }} and user in (transfer_from, transfer_to)) as _amount_usd_from_user
        -- dst $ amount to user
        , sum(amount_usd * if(user = transfer_to, 1, -1)) filter(where {{ dst_condition }} and user in (transfer_from, transfer_to)) as _amount_usd_to_user
        -- dst $ amount to receiver
        , sum(amount_usd * if(receiver = transfer_to, 1, -1)) filter(where {{ dst_condition }} and receiver in (transfer_from, transfer_to)) as _amount_usd_to_receiver

        -- escrow results
        , sum(amount) filter(where result_escrow = src_escrow and result_method = 'withdraw') as src_withdraw_amount
        , sum(amount) filter(where result_escrow = src_escrow and result_method = 'cancel') as src_cancel_amount
        , sum(amount) filter(where result_escrow = src_escrow and result_method = 'rescueFunds') as src_rescue_amount
        , sum(amount_usd) filter(where result_escrow = src_escrow and result_method = 'withdraw') as src_withdraw_amount_usd
        , sum(amount_usd) filter(where result_escrow = src_escrow and result_method = 'cancel') as src_cancel_amount_usd
        , sum(amount_usd) filter(where result_escrow = src_escrow and result_method = 'rescueFunds') as src_rescue_amount_usd
        , sum(amount) filter(where result_escrow = dst_escrow and result_method = 'withdraw') as dst_withdraw_amount
        , sum(amount) filter(where result_escrow = dst_escrow and result_method = 'cancel') as dst_cancel_amount
        , sum(amount) filter(where result_escrow = dst_escrow and result_method = 'rescueFunds') as dst_rescue_amount
        , sum(amount_usd) filter(where result_escrow = dst_escrow and result_method = 'withdraw') as dst_withdraw_amount_usd
        , sum(amount_usd) filter(where result_escrow = dst_escrow and result_method = 'cancel') as dst_cancel_amount_usd
        , sum(amount_usd) filter(where result_escrow = dst_escrow and result_method = 'rescueFunds') as dst_rescue_amount_usd

        , count(distinct (contract_address, transfer_native)) as tokens -- count distinct tokens in transfers
        , count(*) as transfers -- count transfers
    from swaps
    join (
        select * from {{ ref('oneinch_call_transfers') }}
        {% if is_incremental() %}
            where {{ incremental_predicate('block_time') }}
        {% endif %}
    ) using(blockchain, block_number, tx_hash, call_trace_address) -- block_number is needed for performance
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
    , order_hash
    , map_concat(flags, map_from_entries(array[
        ('second_side', second_side)
        , ('contracts_only', contracts_only)
        , ('cross_chain', hashlock is not null)
    ])) as flags
    , remains
    , coalesce(_src_token_address_from_user, _src_token_address_from_caller, if(_src_token_native, {{ true_native_address }}, _src_token_address)) as src_token_address
    , coalesce(_src_token_symbol_from_user, _src_token_symbol_from_caller, _src_token_symbol) as src_token_symbol
    , src_token_decimals
    , coalesce(_src_token_amount_true, src_token_amount) as src_token_amount
    , src_escrow
    , hashlock
    , dst_blockchain
    , dst_block_number
    , dst_block_time
    , dst_tx_hash
    , coalesce(_dst_token_address_to_user, _dst_token_address_to_receiver, _dst_token_address_to_caller, if(_dst_token_native, {{ true_native_address }}, _dst_token_address)) as dst_token_address
    , coalesce(_dst_token_symbol_to_user, _dst_token_symbol_to_receiver, _dst_token_symbol_to_caller, _dst_token_symbol) as dst_token_symbol
    , dst_token_decimals
    , coalesce(_dst_token_amount_true, dst_token_amount) as dst_token_amount
    , coalesce(sources_amount_usd_trusted, sources_amount_usd, transfers_amount_usd_trusted, transfers_amount_usd) as amount_usd -- sources $ amount first if found prices, then $ amount of connector tokens
    , sources_amount_usd
    , src_token_amount_usd
    , dst_token_amount_usd
    , transfers_amount_usd
    , greatest(coalesce(_amount_usd_from_user, 0), coalesce(_amount_usd_to_user, _amount_usd_to_receiver, 0)) as user_amount_usd -- actual user $ amount
    , tokens
    , transfers
    , map_from_entries(array[
        ('withdraw', cast(row(src_withdraw_amount, src_withdraw_amount_usd, dst_withdraw_amount, dst_withdraw_amount_usd) as row(src_amount uint256, src_amount_usd double, dst_amount uint256, dst_amount_usd double)))
        , ('cancel', cast(row(src_cancel_amount, src_cancel_amount_usd, dst_cancel_amount, dst_cancel_amount_usd) as row(src_amount uint256, src_amount_usd double, dst_amount uint256, dst_amount_usd double)))
        , ('rescue', cast(row(src_rescue_amount, src_rescue_amount_usd, dst_rescue_amount, dst_rescue_amount_usd) as row(src_amount uint256, src_amount_usd double, dst_amount uint256, dst_amount_usd double)))
    ]) as escrow_results
    , date_trunc('minute', block_time) as minute
    , date(date_trunc('month', block_time)) as block_month
    , coalesce(order_hash, tx_hash || from_hex(if(mod(length(_call_trace_address), 2) = 1, '0' || _call_trace_address, _call_trace_address) || '0' || cast(cast(second_side as int) as varchar))) as swap_id
    , {{dbt_utils.generate_surrogate_key(["blockchain", "tx_hash", "array_join(call_trace_address, ',')", "second_side"])}} as unique_key
from swaps
join amounts using(blockchain, block_number, tx_hash, call_trace_address, second_side)