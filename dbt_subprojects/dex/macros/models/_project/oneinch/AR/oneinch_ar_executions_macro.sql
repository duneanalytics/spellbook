{%- macro
    oneinch_ar_executions_macro(
        blockchain,
        stream
    )
-%}

{%- set date_from = [blockchain.start, stream.start] | max -%}
{%- set wrapper = blockchain.wrapped_native_token_address -%}
{%- set nsymbol = blockchain.native_token_symbol -%}
{%- set native = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' -%}
{%- set nullss = '0x0000000000000000000000000000000000000000' -%}
{%- set src_calldata_token = 'if(src_token_address in (' ~ nullss ~ ', ' ~ native ~ '), ' ~ wrapper ~ ', src_token_address)' -%}
{%- set dst_calldata_token = 'if(dst_token_address in (' ~ nullss ~ ', ' ~ native ~ '), ' ~ wrapper ~ ', dst_token_address)' -%}
{%- set settlement_vaults = oneinch_ar_settlement_vaults_cfg_macro().get(blockchain.name, []) -%}



with

calls as (
    select *
        -- v6 swaps built by external routing proxies (e.g. the Binance wallet) carry a synthetic marker in the decoded desc
        -- (srcToken = dstToken = srcReceiver = dstReceiver = marker address, amounts = 1) with the real route packed only in
        -- the opaque executor data, so the token legs of these calls are recovered from the nested transfers instead
        , coalesce(protocol_version = 6 and src_token_address = dst_token_address, false) as degenerate_params
    from {{ ref('oneinch_' + blockchain.name + '_ar') }}
    where true
        and block_date >= timestamp '{{ date_from }}'
        {% if is_incremental() -%} and {{ incremental_predicate('block_time') }} {%- endif %}
)

, transfers as (
    select *
        , row_number() over(partition by block_month, block_date, block_number, tx_hash order by transfer_trace_address desc) as transfer_number_desc
    from {{ ref('oneinch_' + blockchain.name + '_transfers') }}
    where true
        and nested
        and protocol = 'AR'
        and block_date >= timestamp '{{ date_from }}'
        {% if is_incremental() -%} and {{ incremental_predicate('block_time') }} {%- endif %}
)

, tokens as ( -- token metadata fallback by the calldata token addresses for calls without matched transfers (e.g. internal calls of the router from wallet proxies)
    select
        contract_address
        , symbol as token_symbol
        , decimals as token_decimals
    from {{ source('tokens', 'erc20') }}
    where blockchain = '{{ blockchain.name }}'
)

, venues as ( -- venue-side parties for the degenerate netting: registered pools + flash-accounting singletons that hold the pool funds
    select pool from {{ ref('dex_raw_pools') }} where blockchain = '{{ blockchain.name }}'
    {%- for vault in settlement_vaults %}
    union all select {{ vault }}
    {%- endfor %}
)

, degenerate_netting as ( -- per-token net flow into the venue side of the nested transfers of degenerate calls
    select
        block_date
        , block_number
        , tx_hash
        , call_trace_address
        , if(cardinality(same) > 1, {{ wrapper }}, transfer_contract_address) as token_address -- the native leg as the wrapped native token
        , max(transfer_symbol) as token_symbol
        , max(transfer_decimals) as token_decimals
        , sum(if(transfer_to in (select pool from venues), cast(transfer_amount as int256), int256 '0'))
        - sum(if(transfer_from in (select pool from venues), cast(transfer_amount as int256), int256 '0')) as venue_net
        , max(transfer_amount_usd) as token_amount_usd
    from calls
    join transfers using(blockchain, block_month, block_date, block_number, block_time, tx_hash, call_trace_address, call_selector, call_method, call_to, protocol, contract_name)
    where degenerate_params
        and transfer_contract_address <> src_token_address -- skip the marker token dust
    group by 1, 2, 3, 4, 5
)

{%- set net_row_type = 'row(address varbinary, symbol varchar, amount uint256, decimals bigint)' -%}

, degenerate_executions as ( -- src = the token flowing into the venues, dst = the token flowing out of them; the biggest by USD when several
    select
        block_date
        , block_number
        , tx_hash
        , call_trace_address
        , max_by(cast(row(token_address, token_symbol, cast(if(venue_net > int256 '0', venue_net) as uint256), token_decimals) as {{ net_row_type }}), coalesce(token_amount_usd, 0)) filter(where venue_net > int256 '0') as net_src_data
        , max_by(cast(row(token_address, token_symbol, cast(if(venue_net < int256 '0', -venue_net) as uint256), token_decimals) as {{ net_row_type }}), coalesce(token_amount_usd, 0)) filter(where venue_net < int256 '0') as net_dst_data
        , max(token_amount_usd) filter(where venue_net > int256 '0') as net_src_amount_usd
        , max(token_amount_usd) filter(where venue_net < int256 '0') as net_dst_amount_usd
    from degenerate_netting
    group by 1, 2, 3, 4
)

{%- set src_data = 'cast(row(transfer_contract_address, transfer_symbol, transfer_amount, transfer_decimals, transfer_from) as row(address varbinary, symbol varchar, amount uint256, decimals bigint, sender varbinary))' -%}
{%- set dst_data = 'cast(row(transfer_contract_address, transfer_symbol, transfer_amount, transfer_decimals, transfer_to) as row(address varbinary, symbol varchar, amount uint256, decimals bigint, receiver varbinary))' -%}
{%- set src_condition = 'not degenerate_params and array_position(same, src_token_address) > 0 and transfer_amount <= src_token_amount' -%}
{%- set dst_condition = 'not degenerate_params and array_position(same, dst_token_address) > 0 and transfer_amount <= dst_token_amount' -%}
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
    , if(degenerate_params, net_src_data.address, src_token_address) as src_token_address -- the marker address of degenerate calls is replaced with the netted one
    , src_token_amount
    , coalesce(src_user_data.address, src_data.address, net_src_data.address, if(protocol_version = 6 and not degenerate_params, {{ src_calldata_token }})) as src_executed_address
    , coalesce(src_user_data.symbol, src_data.symbol, net_src_data.symbol, if(protocol_version = 6 and not degenerate_params, if(src_token_address in ({{ nullss }}, {{ native }}), {{ nsymbol }}, src_token.token_symbol))) as src_executed_symbol
    , coalesce(src_user_data.amount, src_amount, net_src_data.amount, if(protocol_version = 6 and not degenerate_params, src_token_amount)) as src_executed_amount -- first from the user, then only with the correct amount, then netted from the nested transfers, then from the decoded call params
    , coalesce(src_amount_usd, net_src_amount_usd) as src_executed_amount_usd

    , cast(null as varchar) as dst_blockchain
    , if(degenerate_params, net_dst_data.address, dst_token_address) as dst_token_address -- the marker address of degenerate calls is replaced with the netted one
    , dst_token_amount
    , coalesce(dst_user_data.address, dst_data.address, net_dst_data.address, if(protocol_version = 6 and not degenerate_params, {{ dst_calldata_token }})) as dst_executed_address
    , coalesce(dst_user_data.symbol, dst_data.symbol, net_dst_data.symbol, if(protocol_version = 6 and not degenerate_params, if(dst_token_address in ({{ nullss }}, {{ native }}), {{ nsymbol }}, dst_token.token_symbol))) as dst_executed_symbol
    , coalesce(dst_user_data.amount, dst_amount, net_dst_data.amount, if(protocol_version = 6 and not degenerate_params, dst_token_amount)) as dst_executed_amount -- first to the user, then only with the correct amount, then netted from the nested transfers, then from the decoded call output
    , coalesce(dst_amount_usd, net_dst_amount_usd) as dst_executed_amount_usd
    
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
        , ('src_decimals', cast(coalesce(src_user_data.decimals, src_data.decimals, net_src_data.decimals, src_token.token_decimals) as varchar))
        , ('dst_decimals', cast(coalesce(dst_user_data.decimals, dst_data.decimals, net_dst_data.decimals, dst_token.token_decimals) as varchar))
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
left join degenerate_executions using(block_date, block_number, tx_hash, call_trace_address)
left join tokens as src_token on src_token.contract_address = {{ src_calldata_token }} and protocol_version = 6 and not degenerate_params
left join tokens as dst_token on dst_token.contract_address = {{ dst_calldata_token }} and protocol_version = 6 and not degenerate_params

{%- endmacro -%}
