{% macro oneinch_lo_executions_macro(blockchain) %}

{% set meta = oneinch_meta_cfg_macro() %}
{% set date_from = [meta['blockchains']['start'][blockchain], meta['streams']['lo']['start']['executions']] | max %}

{% set wrapper = meta['blockchains']['wrapped_native_token_address'][blockchain] %}
{% set same = '(0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee, ' + wrapper + ')' %}



with

calls as (
    select *
    from {{ ref('oneinch_' + blockchain + '_lo') }}
    where true
        and not flags['cross_chain']
        and block_date >= timestamp '{{ date_from }}'
        {% if is_incremental() %}and {{ incremental_predicate('block_time') }}{% endif %}
)

, transfers as (
    select *
        , if(transfer_contract_address in {{ same }}, {{ same }}, (transfer_contract_address)) as same
    from {{ ref('oneinch_' + blockchain + '_raw_transfers') }}
    where true
        and nested
        and related
        and protocol = 'LO'
        and block_date >= timestamp '{{ date_from }}'
        {% if is_incremental() %}and {{ incremental_predicate('block_time') }}{% endif %}
)

, executions as (
    select
        block_date
        , block_number
        , tx_hash
        , call_trace_address

        -- source token data --
        , max_by(cast(row(transfer_contract_address, transfer_symbol, transfer_amount, transfer_amount_usd, transfer_decimals) as row(address varbinary, symbol varchar, amount uint256, amount_usd double, decimals bigint)), transfer_amount) filter(where maker_asset in same and transfer_from = maker) as src_executed -- trying to find out what the user actually sent, from the related transfers with the greatest transfer amount

        -- destination token data --
        , max_by(cast(row(transfer_contract_address, transfer_symbol, transfer_amount, transfer_amount_usd, transfer_decimals) as row(address varbinary, symbol varchar, amount uint256, amount_usd double, decimals bigint)), transfer_amount) filter(where taker_asset in same and transfer_to in (maker, receiver)) as dst_executed -- trying to find out what the user actually received, from the related transfers with the greatest transfer amount

        -- general --
        , max(transfer_amount_usd) filter(where user_related and trusted) as sources_executed_trusted_amount_usd
        , max(transfer_amount_usd) filter(where user_related) as sources_executed_amount_usd
        , max(transfer_amount_usd) as executed_amount_usd
    from (
        select *
            , cardinality(array_intersect(array[transfer_from, transfer_to], array[maker, receiver])) > 0 as user_related
        from calls
        left join transfers using(blockchain, block_month, block_date, block_time, block_number, tx_hash, call_trace_address, call_to, protocol, contract_name, call_selector, call_method) -- even with missing transfers, as transfers may not have been parsed
    )
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
    , contract_address
    , contract_name

    , coalesce(null
        , sources_executed_trusted_amount_usd
        , if(sources_executed_amount_usd - least(src_executed.amount_usd, dst_executed.amount_usd) > least(src_executed.amount_usd, dst_executed.amount_usd), least(src_executed.amount_usd, dst_executed.amount_usd)) -- i.e. if the slippadge/difference > ~50% then the least of src/dst, for minimize price errors
        , sources_executed_amount_usd -- if previous is null or false
        , executed_amount_usd
    ) as amount_usd
    , tx_gas_price * if(element_at(flags, 'direct'), tx_gas_used, call_gas_used) * native_price / pow(10, native_decimals) as execution_cost

    , maker as user
    , receiver
    , maker_asset as src_token_address
    , maker_amount as src_token_amount
    , src_executed.address as src_executed_address
    , src_executed.symbol as src_executed_symbol
    , src_executed.amount as src_executed_amount
    , src_executed.amount_usd as src_executed_amount_usd

    , null as dst_blockchain
    , taker_asset as dst_token_address
    , taker_amount as dst_token_amount
    , dst_executed.address as dst_executed_address
    , dst_executed.symbol as dst_executed_symbol
    , dst_executed.amount as dst_executed_amount
    , dst_executed.amount_usd as dst_executed_amount_usd
    
    , order_hash
    , cast(null as varbinary) as hashlock
    , cast(null as row(action varchar, success boolean, tx_fee double, tx_hash varbinary, escrow varbinary, token varbinary, amount uint256)) as actions

    , map_from_entries(array[
        ('making_amount', cast(making_amount as varchar))
        , ('taking_amount', cast(taking_amount as varchar))
        , ('sources_trusted_amount_usd', cast(sources_executed_trusted_amount_usd as varchar))
        , ('sources_amount_usd', cast(sources_executed_amount_usd as varchar))
        , ('amount_usd', cast(executed_amount_usd as varchar))
        , ('src_decimals', cast(src_executed.decimals as varchar))
        , ('dst_decimals', cast(dst_executed.decimals as varchar))
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

{% endmacro %}