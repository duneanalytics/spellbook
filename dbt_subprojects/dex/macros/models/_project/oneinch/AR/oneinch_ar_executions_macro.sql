{%- macro
    oneinch_ar_executions_macro(
        blockchain,
        stream
    )
-%}

{%- set date_from = [blockchain.start, stream.start] | max -%}



with

calls as (
    select *
    from {{ ref('oneinch_' + blockchain.name + '_ar') }}
    where true
        and block_date >= timestamp '{{ date_from }}'
        {% if is_incremental() -%} and {{ incremental_predicate('block_time') }}{% endif %}
)

, transfers as (
    select *
        , row_number() over(partition by block_month, block_date, block_number, tx_hash order by transfer_trace_address desc) as transfer_number_desc
    from {{ ref('oneinch_' + blockchain.name + '_transfers') }}
    where true
        and nested
        and protocol = 'AR'
        and block_date >= timestamp '{{ date_from }}'
        {% if is_incremental() -%} and {{ incremental_predicate('block_time') }}{% endif %}
)

{%- set src_data = 'cast(row(transfer_contract_address, transfer_symbol, transfer_amount, transfer_decimals, transfer_from) as row(address varbinary, symbol varchar, amount uint256, decimals bigint, sender varbinary))' -%}
{%- set dst_data = 'cast(row(transfer_contract_address, transfer_symbol, transfer_amount, transfer_decimals, transfer_to) as row(address varbinary, symbol varchar, amount uint256, decimals bigint, receiver varbinary))' -%}
{%- set src_condition = 'array_position(same, src_token_address) > 0 and transfer_amount <= src_token_amount' -%}
{%- set dst_condition = 'array_position(same, dst_token_address) > 0 and transfer_amount <= dst_token_amount' -%}
{%- set user_condition = 'cardinality(array_intersect(array[transfer_from, transfer_to], array[tx_from, call_from, dst_receiver])) > 0' %}

, executions as (
    select
        block_date
        , block_number
        , tx_hash
        , call_trace_address

        -- source token data --
        , max(transfer_amount) filter(where {{ src_condition }}) as src_amount
        , max(transfer_amount_usd) filter(where {{ src_condition }}) as src_amount_usd
        , max_by({{ src_data }}, (transfer_amount, transfer_number_desc)) filter(where {{ src_condition }} and transfer_from in (tx_from, call_from)) as src_user_data -- trying to find out what the user actually sent, from the related transfers with the greatest transfer amount and the least trace address
        , max_by({{ src_data }}, (transfer_amount, transfer_number_desc)) filter(where {{ src_condition }}) as src_data -- src data from the related transfers with the greatest transfer amount and the least trace address

        -- destination token data --
        , max(transfer_amount) filter(where {{ dst_condition }}) as dst_amount
        , max(transfer_amount_usd) filter(where {{ dst_condition }}) as dst_amount_usd
        , max_by({{ dst_data }}, (transfer_amount, transfer_trace_address)) filter(where {{ dst_condition }} and transfer_to in (tx_from, call_from, dst_receiver)) as dst_user_data -- trying to find out what the user actually received, from the related transfers with the greatest transfer amount and the greatest (the last) trace address
        , max_by({{ dst_data }}, (transfer_amount, transfer_trace_address)) filter(where {{ dst_condition }}) as dst_data -- dst data from the related transfers with the greatest transfer amount and the greatest (the last) trace address

        -- general --
        , max(transfer_amount_usd) filter(where ({{ src_condition }} or {{ dst_condition }}) and trusted) as sources_trusted_amount_usd
        , max(transfer_amount_usd) filter(where ({{ src_condition }} or {{ dst_condition }}) and {{ user_condition }}) as sources_user_amount_usd
        , max(transfer_amount_usd) filter(where ({{ src_condition }} or {{ dst_condition }})) as sources_amount_usd
        , max(transfer_amount_usd) filter(where trusted) as trusted_amount_usd
        , max(transfer_amount_usd) as amount_usd
    from calls
    left join transfers using(blockchain, block_month, block_date, block_number, block_time, tx_hash, call_trace_address, call_selector, call_method, call_to, protocol, contract_name) -- even with missing transfers, as transfers may not have been parsed
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

    , coalesce(null
        , sources_trusted_amount_usd
        , sources_amount_usd
        , trusted_amount_usd
        , amount_usd
    ) as amount_usd
    , native_price * tx_gas_price * if(element_at(flags, 'direct'), tx_gas_used, call_gas_used) / pow(10, native_decimals) as execution_cost

    , tx_from as user
    , dst_receiver as receiver
    , src_token_address
    , src_token_amount
    , coalesce(src_user_data.address, src_data.address) as src_executed_address
    , coalesce(src_user_data.symbol, src_data.symbol) as src_executed_symbol
    , coalesce(src_user_data.amount, src_amount) as src_executed_amount -- first from the user, then only with the correct amount
    , src_amount_usd as src_executed_amount_usd

    , cast(null as varchar) as dst_blockchain
    , dst_token_address
    , dst_token_amount
    , coalesce(dst_user_data.address, dst_data.address) as dst_executed_address
    , coalesce(dst_user_data.symbol, dst_data.symbol) as dst_executed_symbol
    , coalesce(dst_user_data.amount, dst_amount) as dst_executed_amount -- first to the user, then only with the correct amount
    , dst_amount_usd as dst_executed_amount_usd
    
    , cast(null as varbinary) as order_hash
    , cast(null as varbinary) as hashlock
    , cast(null as array(row(action varchar, success boolean, cost double, tx_hash varbinary, escrow varbinary, token varbinary, amount uint256))) as actions
    
    , map_from_entries(array[
        ('sender', cast(coalesce(src_user_data.sender, src_data.sender) as varchar))
        , ('receiver', cast(coalesce(dst_user_data.receiver, dst_data.receiver) as varchar))
        , ('sources_trusted_amount_usd', format('$%,.0f', sources_trusted_amount_usd))
        , ('sources_user_amount_usd', format('$%,.0f', sources_user_amount_usd))
        , ('sources_amount_usd', format('$%,.0f', sources_amount_usd))
        , ('trusted_amount_usd', format('$%,.0f', trusted_amount_usd))
        , ('amount_usd', format('$%,.0f', amount_usd))
        , ('src_decimals', cast(coalesce(src_user_data.decimals, src_data.decimals) as varchar))
        , ('dst_decimals', cast(coalesce(dst_user_data.decimals, dst_data.decimals) as varchar))
    ]) as complement

    , remains
    , flags
    , minute
    , block_date
    , block_month
    , native_price
    , native_decimals
from calls
join executions using(block_date, block_number, tx_hash, call_trace_address)

{%- endmacro -%}