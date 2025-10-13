{% set blockchain = 'solana' %}

{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'lo',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month', 'tx_hash', 'call_trace_address'],
    )
}}

{% set meta = oneinch_meta_cfg_macro()['blockchains'] %}
{% set chain_id = meta['blockchains']['chain_id'][blockchain] %}

-- temporary implementation -- TO DO: redesign to streams logic

select
    blockchain
    , {{ chain_id }} as chain_id
    , block_slot as block_number
    , block_time
    , from_base58(tx_id) as tx_hash
    , null as tx_success -- TO DO
    , from_base58(tx_signer) as tx_from
    , from_base58(outer_executing_account) as tx_to
    , null as tx_nonce
    , tx_gas_used
    , tx_gas_price
    , tx_priority_fee_per_gas
    , 1 as tx_index -- TO DO
    , call_trace_address
    , null as call_success -- TO DO
    , null as call_gas_used
    , cast(null as varbinary) as call_selector
    , method as call_method -- TO DO
    , from_base58(resolver) as call_from
    , from_base58(outer_executing_account) as call_to
    , cast(null as varbinary) as call_output
    , null as call_error
    , null as call_type
    , 'INTENTS' as protocol
    , version as protocol_version -- TO DO
    , program_name as contract_name

    , order_hash
    , from_base58(user) as maker
    , from_base58(maker_receiver) as receiver
    , from_base58(src_token_mint) as maker_asset
    , src_token_amount as maker_amount
    , null as making_amount
    , from_base58(dst_token_mint) as taker_asset
    , dst_token_amount as taker_amount
    , null as taking_amount

    , cast(null as array(varbinary)) as remains
    , cast(null as map(varchar, boolean)) as flags
    , map_from_entries(array[
        ('direct', false)
        , ('fusion', true)
    ]) as flags
    , date_trunc('minute', block_time) as minute
    , date(block_time) as block_date
    , block_month
    , null as native_price -- TO DO
    , null as native_decimals -- TO DO
from {{ source('oneinch_solana', 'swaps') }} -- TO DO: swaps -> executions