{%- macro
    oneinch_lo_executions_macro(
        blockchain,
        stream
    )
-%}

{%- set date_from = [blockchain.start, stream.start] | max -%}



with

calls as (
    select *
    from {{ ref('oneinch_' + blockchain.name + '_lo') }}
    where true
        and not flags['cross_chain']
        and block_date >= timestamp '{{ date_from }}'
        {% if is_incremental() -%} and {{ incremental_predicate('block_time') }} {%- endif %}
)

, transfers as (
    select *
    from {{ ref('oneinch_' + blockchain.name + '_transfers') }}
    where true
        and nested
        and protocol = 'LO'
        and block_date >= timestamp '{{ date_from }}'
        {% if is_incremental() -%} and {{ incremental_predicate('block_time') }} {%- endif %}
)

{%- set data = 'cast(row(transfer_contract_address, transfer_symbol, transfer_amount, transfer_amount_usd, transfer_decimals) as row(address varbinary, symbol varchar, amount uint256, amount_usd double, decimals bigint))' -%}
{%- set src_condition = 'array_position(same, maker_asset) > 0 and transfer_from = maker' -%}
{%- set dst_condition = 'array_position(same, taker_asset) > 0 and transfer_to in (maker, receiver)' -%}
{%- set user_condition = 'cardinality(array_intersect(array[transfer_from, transfer_to], array[maker, receiver])) > 0' %}

, executions as (
    select
        block_date
        , block_number
        , tx_hash
        , call_trace_address

        -- source token data --
        , max_by({{ data }}, transfer_amount) filter(where {{ src_condition }}) as src_data -- trying to find out what the user actually sent, from the related transfers with the greatest transfer amount
        , max_by({{ data }}, transfer_amount) filter(where transfer_from = maker) as src_user_data -- trying to find out what the user actually sent

        -- destination token data --
        , max_by({{ data }}, transfer_amount) filter(where {{ dst_condition }}) as dst_data -- trying to find out what the user actually received, from the related transfers with the greatest transfer amount
        , max_by({{ data }}, transfer_amount) filter(where transfer_to in (maker, receiver)) as dst_user_data -- trying to find out what the user actually received

        -- general --
        , max(transfer_amount_usd) filter(where ({{ src_condition }} or {{ dst_condition }}) and trusted) as sources_trusted_amount_usd
        , max(transfer_amount_usd) filter(where ({{ src_condition }} or {{ dst_condition }})) as sources_amount_usd
        , max(transfer_amount_usd) filter(where {{ user_condition }} and trusted) as user_trusted_amount_usd
        , max(transfer_amount_usd) filter(where {{ user_condition }}) as user_amount_usd
        , max(transfer_amount_usd) filter(where trusted) as trusted_amount_usd
        , max(transfer_amount_usd) as amount_usd
        -- carries the winning trusted transfer's own amount/decimals alongside its usd value,
        -- so the amount itself (not its dollar value) can be checked for plausibility below --
        , max_by({{ data }}, transfer_amount_usd) filter(where ({{ src_condition }} or {{ dst_condition }}) and trusted) as sources_trusted_data
        , max_by({{ data }}, transfer_amount_usd) filter(where {{ user_condition }} and trusted) as user_trusted_data
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

    -- a trusted leg normally wins outright, and MUST keep winning when it legitimately
    -- has a smaller dollar value than the other leg -- that is exactly the case (an
    -- untrusted, illiquid token's inflated/unreliable price) trusted-preference exists to
    -- guard against, confirmed against real DATA/USDC trades where the untrusted leg
    -- overstates value by 100-1000x and the trusted (smaller) side is correct.
    -- The only case worth overriding is when the trusted leg's own transferred AMOUNT
    -- (not its dollar value) is degenerate -- a maker can set a sentinel making_amount
    -- (seen as literal 1 wei on-chain, confirmed via output_0, not a decode error) while
    -- genuinely trading real value on the taker side. 1e-9 of a token is far above the
    -- observed 1-wei (1e-18 for an 18-decimal token) bug pattern and far below any
    -- plausible real trade, so it should not catch legitimate small trades.
    , case
        when coalesce(user_trusted_data, sources_trusted_data) is not null
         and cast(coalesce(user_trusted_data, sources_trusted_data).amount as double)
             / pow(10, coalesce(user_trusted_data, sources_trusted_data).decimals) < 0.000000001
        then coalesce(user_amount_usd, sources_amount_usd, trusted_amount_usd, amount_usd)
        else coalesce(user_trusted_amount_usd, sources_trusted_amount_usd, user_amount_usd, sources_amount_usd, trusted_amount_usd, amount_usd)
    end as amount_usd
    , native_price * tx_gas_price * if(element_at(flags, 'direct'), tx_gas_used, call_gas_used) / pow(10, native_decimals) as execution_cost

    , maker as user
    , receiver
    , maker_asset as src_token_address
    , maker_amount as src_token_amount
    , coalesce(src_data.address, src_user_data.address) as src_executed_address
    , coalesce(src_data.symbol, src_user_data.symbol) as src_executed_symbol
    , coalesce(src_data.amount, src_user_data.amount) as src_executed_amount
    , coalesce(src_data.amount_usd, src_user_data.amount_usd) as src_executed_amount_usd

    , cast(null as varchar) as dst_blockchain
    , taker_asset as dst_token_address
    , taker_amount as dst_token_amount
    , coalesce(dst_data.address, dst_user_data.address) as dst_executed_address
    , coalesce(dst_data.symbol, dst_user_data.symbol) as dst_executed_symbol
    , coalesce(dst_data.amount, dst_user_data.amount) as dst_executed_amount
    , coalesce(dst_data.amount_usd, dst_user_data.amount_usd) as dst_executed_amount_usd
    
    , order_hash
    , cast(null as varbinary) as hashlock
    , cast(null as array(row(action varchar, success boolean, cost double, tx_hash varbinary, escrow varbinary, token varbinary, amount uint256))) as actions
    
    , map_from_entries(array[
        ('making_amount', cast(making_amount as varchar))
        , ('taking_amount', cast(taking_amount as varchar))
        , ('user_trusted_amount_usd', format('$%,.0f', user_trusted_amount_usd))
        , ('user_amount_usd', format('$%,.0f', user_amount_usd))
        , ('sources_trusted_amount_usd', format('$%,.0f', sources_trusted_amount_usd))
        , ('sources_amount_usd', format('$%,.0f', sources_amount_usd))
        , ('trusted_amount_usd', format('$%,.0f', trusted_amount_usd))
        , ('amount_usd', format('$%,.0f', amount_usd))
        , ('src_decimals', cast(coalesce(src_data.decimals, src_user_data.decimals) as varchar))
        , ('dst_decimals', cast(coalesce(dst_data.decimals, dst_user_data.decimals) as varchar))
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
