{% macro oneinch_cc_executions_macro(blockchain) %}

{% set stream = 'cc' %}
{% set substream = 'executions' %}
{% set meta = oneinch_meta_cfg_macro() %}
{% set date_from = meta['streams'][stream]['start'][substream] %}
{% set chain_id = meta['blockchains']['chain_id'][blockchain] %}
{% set wrapper = meta['blockchains']['wrapped_native_token_address'][blockchain] %}
{% set same = '0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee, ' + wrapper %}



with

calls as (
    select *
    from {{ ref('oneinch_' + blockchain + '_cc') }}
    where true
        and block_date >= timestamp '{{ date_from }}'
        {% if is_incremental() %}and {{ incremental_predicate('block_time') }}{% endif %}
)

, transfers as (
    select *
        , if(transfer_contract_address in ({{ same }}), array[{{ same }}], array[transfer_contract_address]) as same
    from {{ ref('oneinch_' + blockchain + '_raw_transfers') }}
    where true
        and nested
        and related
        and protocol = 'CC'
        and block_date >= timestamp '{{ date_from }}'
        {% if is_incremental() %}and {{ incremental_predicate('block_time') }}{% endif %}
)

{% set data = 'cast(row(transfer_to, transfer_contract_address, transfer_symbol, transfer_amount, transfer_amount_usd, transfer_decimals, trusted) as row(receiver varbinary, address varbinary, symbol varchar, amount uint256, amount_usd double, decimals bigint, trusted boolean))' %}
{% set condition = 'array_position(same, token) > 0 and call_from not in (transfer_from, transfer_to)' %}

, amounts as (
    select
        block_date
        , block_number
        , tx_hash
        , call_trace_address
        , max_by({{ data }}, transfer_amount) filter(where {{ condition }}) as transfered -- trying to find out what actually transfered
    from calls
    left join transfers using(block_date, block_number, tx_hash, call_trace_address) -- even with missing transfers, as transfers may not have been parsed
    group by 1, 2, 3, 4
)

-- output --

select
    blockchain
    , chain_id
    , block_number
    , block_time
    , tx_hash
    , tx_success
    , tx_from
    , tx_to
    , tx_nonce
    , tx_gas_used
    , tx_gas_price
    , tx_priority_fee_per_gas
    , tx_index -- it is necessary to determine the order of creations in the block
    , call_trace_address
    , call_success
    , call_gas_used
    , call_selector
    , call_method
    , call_from
    , call_to
    , call_output
    , call_error
    , call_type
    , protocol
    , protocol_version
    , contract_name
    , order_hash
    , hashlock
    , flow
    , escrow
    , maker
    , receiver
    , taker
    , token
    , amount
    , safety_deposit
    , timelocks
    , secret
    , transfered
    , transfered.amount_usd as amount_usd
    , native_price * tx_gas_used * tx_gas_price / pow(10, native_decimals) as execution_cost
    , complement
    , remains
    , minute
    , block_date
    , block_month
    , native_price
    , native_decimals
from calls
join amounts using(block_date, block_number, tx_hash, call_trace_address)

{% endmacro %}