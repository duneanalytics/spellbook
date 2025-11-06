{%- set blockchain = oneinch_solana_cfg_macro() -%}

{{-
    config(
        schema = 'oneinch_' + blockchain.name,
        alias = 'lo_executions',
        materialized = 'incremental',
        partition_by = ['block_month'],
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month', 'tx_hash', 'call_trace_address'],
    )
-}}

-- temporary implementation -- TO DO: redesign to streams logic

select
    blockchain
    , {{ blockchain.get('chain_id', 'null') }} as chain_id
    , block_slot as block_number
    , block_time
    , from_base58(tx_id) as tx_hash
    , cast(null as boolean) as tx_success -- TO DO
    , from_base58(tx_signer) as tx_from
    , from_base58(outer_executing_account) as tx_to
    , cast(null as bigint) as tx_nonce
    , tx_gas_used
    , tx_gas_price
    , tx_priority_fee_per_gas
    , 1 as tx_index -- TO DO
    , call_trace_address
    , cast(null as boolean) as call_success -- TO DO
    , cast(null as bigint) as call_gas_used
    , cast(null as varbinary) as call_selector
    , method as call_method -- TO DO
    , from_base58(resolver) as call_from
    , from_base58(outer_executing_account) as call_to
    , cast(null as varbinary) as call_output
    , cast(null as varchar) as call_error
    , cast(null as varchar) as call_type
    , 'INTENTS' as protocol
    , coalesce(try(cast(version as double)), 1.0) as protocol_version -- TO DO
    , program_name as contract_name

    , amount_usd
    , cast(0 as double) as execution_cost -- TO DO
    , from_base58(user) as user
    , from_base58(maker_receiver) as receiver
    , from_base58(src_token_mint) as src_token_address
    , src_token_amount
    , from_base58(src_token_mint) as src_executed_address
    , src_token_symbol as src_executed_symbol
    , src_token_amount as src_executed_amount
    , src_token_amount_usd as src_executed_amount_usd
    , cast(null as varchar) as dst_blockchain
    , from_base58(dst_token_mint) as dst_token_address
    , dst_token_amount
    , from_base58(dst_token_mint) as dst_executed_address
    , dst_token_symbol as dst_executed_symbol
    , dst_token_amount as dst_executed_amount
    , dst_token_amount_usd as dst_executed_amount_usd
    , order_hash
    , cast(null as varbinary) as hashlock
    , cast(null as array(row(action varchar, success boolean, cost double, tx_hash varbinary, escrow varbinary, token varbinary, amount uint256))) as actions
    , cast(null as map(varchar, varchar)) as complement

    , cast(null as array(bigint)) as remains
    , map_from_entries(array[
        ('direct', false)
        , ('fusion', true)
    ]) as flags
    , date_trunc('minute', block_time) as minute
    , date(block_time) as block_date
    , block_month
    , cast(null as double) as native_price -- TO DO
    , cast(null as bigint) as native_decimals -- TO DO
from {{ source('oneinch_solana', 'swaps') }} -- TO DO: swaps -> executions