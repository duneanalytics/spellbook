{% macro 
    oneinch_calls_macro(
        blockchain
    ) 
%}



{% set columns = [
    'blockchain',
    'block_number',
    'block_time',
    'tx_hash',
    'tx_from',
    'tx_to',
    'tx_success',
    'tx_nonce',
    'tx_gas_used',
    'tx_gas_price',
    'tx_priority_fee_per_gas',
    'contract_name',
    'protocol',
    'protocol_version',
    'method',
    'call_selector',
    'call_trace_address',
    'call_from',
    'call_to',
    'call_success',
    'call_gas_used',
    'call_output',
    'call_error',
    'call_type',
    'remains',
] %}
{% set columns = columns | join(', ') %}



with

calls as (
    select *
    from (
        select
            {{ columns }}
            , cast(null as varbinary) as maker
            , dst_receiver as receiver
            , src_token_address
            , src_token_amount
            , cast(null as varbinary) as src_escrow
            , cast(null as varbinary) as hashlock
            , dst_token_address
            , dst_token_amount
            , blockchain as dst_blockchain
            , cast(null as bigint) as dst_block_number
            , cast(null as timestamp) as dst_block_time
            , cast(null as varbinary) as dst_tx_hash
            , cast(null as varbinary) as dst_escrow
            , cast(null as varbinary) as dst_wrapper
            , cast(null as array(row(blockchain varchar, block_number bigint, block_time timestamp, tx_hash varbinary, trace_address array(bigint), escrow varbinary, amount uint256))) as withdrawals
            , cast(null as array(row(blockchain varchar, block_number bigint, block_time timestamp, tx_hash varbinary, trace_address array(bigint), escrow varbinary, amount uint256))) as cancels
            , cast(null as array(row(blockchain varchar, block_number bigint, block_time timestamp, tx_hash varbinary, trace_address array(bigint), escrow varbinary, amount uint256))) as rescues
            , cast(null as varbinary) as order_hash
            , map_concat(flags, map_from_entries(array[('fusion', false)])) as flags
        from {{ ref('oneinch_' + blockchain + '_ar') }}

        union all

        select
            {{ columns }}
            , maker
            , receiver
            , maker_asset as src_token_address
            , making_amount as src_token_amount
            , src_escrow
            , hashlock
            , taker_asset as dst_token_address
            , taking_amount as dst_token_amount
            , dst_blockchain
            , dst_block_number
            , dst_block_time
            , dst_tx_hash
            , dst_escrow
            , dst_wrapper
            , withdrawals
            , cancels
            , rescues
            , order_hash
            , flags
        from {{ ref('oneinch_lop') }}
        where coalesce(reduce(withdrawals, false, (r, x) -> r or x.blockchain = '{{ blockchain }}', r -> r), blockchain = '{{ blockchain }}') -- withdrawals on the selected blockchain or regular calls on it
    )
)

-- output --

select
    blockchain
    , block_number
    , block_time
    , tx_hash
    , tx_from
    , tx_to
    , tx_success
    , tx_nonce
    , tx_gas_used
    , tx_gas_price
    , tx_priority_fee_per_gas
    , contract_name
    , protocol
    , protocol_version
    , method
    , call_selector
    , call_trace_address
    , call_from
    , call_to
    , call_success
    , call_gas_used
    , call_output
    , call_error
    , call_type
    , remains
    , maker
    , receiver
    , src_token_address
    , src_token_amount
    , src_escrow
    , hashlock
    , dst_token_address
    , dst_token_amount
    , dst_blockchain
    , dst_block_number
    , dst_block_time
    , dst_tx_hash
    , dst_escrow
    , dst_wrapper
    , withdrawals
    , cancels
    , rescues
    , order_hash
    , flags
from calls

{% endmacro %}