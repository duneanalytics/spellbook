{% set substream = 'executions' %}

{{
    config(
        schema = 'oneinch_evms',
        alias = substream,
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        partition_by = ['block_month'],
        unique_key = ['id']
    )
}}



with

executions as (
    {% for blockchain, category in meta['blockchains']['category'].items() if category == 'evms' and blockchain in meta['blockchains']['exposed'] %}
        select *
            , flags as _flags
            , 'classic' as mode
        from {{ ref('oneinch_' + blockchain + '_ar_' + substream) }}
        
        union all
        
        select *
            , map_concat(flags, map_from_entries(array[('contracts_only', position('RFQ' in method) > 0 or flags['partial'] and not flags['multiple'])])) as _flags
            , if(flags['fusion'], 'fusion', 'limits') as mode
        from {{ ref('oneinch_' + blockchain + '_lo_' + substream) }}
        
        union all
        
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
            , tx_index
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
            , amount_usd
            , execution_cost
            , tx_from as user
            , receiver
            , dst_token_address as src_token_address
            , dst_token_amount as src_token_amount
            , dst_executed_address as src_executed_address
            , dst_executed_symbol as src_executed_symbol
            , dst_executed_amount as src_executed_amount
            , dst_executed_amount_usd as src_executed_amount_usd
            , dst_blockchain
            , src_token_address as dst_token_address
            , src_token_amount as dst_token_amount
            , src_executed_address as dst_executed_address
            , src_executed_symbol as dst_executed_symbol
            , src_executed_amount as dst_executed_amount
            , src_executed_amount_usd as dst_executed_amount_usd
            , actions
            , order_hash
            , hashlock
            , complement
            , remains
            , flags
            , minute
            , block_date
            , block_month
            , native_price
            , native_decimals
            , map_concat(flags, map_from_entries(array[('second_side', true)])) as _flags
            , 'classic' as mode
        from {{ ref('oneinch_' + blockchain + '_lo_' + substream) }}
        where true -- second side of LOP calls (when a direct call LOP method => users from two sides)
            and protocol = 'LOP'
            and flags['direct']
        
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
    union all

    select *
        , flags as _flags
        , 'cross-chain' as mode
    from {{ ref('oneinch_cc_' + substream) }}

    union all
    
    select
        blockchain
        , {{ oneinch_meta_cfg_macro()['blockchains']['chain_id'].get('solana', 'null') }} as chain_id
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
        , cast(null as varbinary) as contract_address -- TO DO
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

        , null as dst_blockchain
        , from_base58(dst_token_mint) as dst_token_address
        , dst_token_amount
        , from_base58(dst_token_mint) as dst_executed_address
        , dst_token_symbol as dst_executed_symbol
        , dst_token_amount as dst_executed_amount
        , dst_token_amount_usd as dst_executed_amount_usd

        , cast(null as row(action varchar, success boolean, tx_fee double, tx_hash varbinary, escrow varbinary, token varbinary, amount uint256)) as actions -- TO DO
        , order_hash
        , cast(null as varbinary) as hashlock
        , cast(null as map(varchar, varchar)) as complement
        , cast(null as array(varbinary)) as remains
        , cast(null as map(varchar, boolean)) as flags

        , date_trunc('minute', block_time) as minute
        , date(block_time) as block_date
        , block_month
        , null as native_price -- TO DO
        , null as native_decimals -- TO DO
        , cast(null as map(varchar, boolean)) as _flags
        , 'fusion' as mode -- TO DO
    from {{ source('oneinch_solana', 'swaps') }} -- TO DO: swaps -> executions
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
    , mode
    , protocol
    , protocol_version
    , contract_address
    , contract_name
    , amount_usd
    , execution_cost
    , user
    , receiver
    , src_token_address
    , src_token_amount
    , src_executed_address
    , src_executed_symbol
    , src_executed_amount
    , src_executed_amount_usd
    , dst_blockchain
    , dst_token_address
    , dst_token_amount
    , dst_executed_address
    , dst_executed_symbol
    , dst_executed_amount
    , dst_executed_amount_usd
    , actions
    , order_hash
    , hashlock
    , complement
    , remains
    , _flags as flags
    , block_date
    , block_month
    , native_price
    , native_decimals
    , {{ dbt_utils.generate_surrogate_key(["blockchain", "tx_hash", "array_join(call_trace_address, '')", "mode"]) }} as id -- TO DO: make it orderly (with block_number & tx_index)
from executions
{% if is_incremental() %}
    where {{ incremental_predicate('block_time') }}
{% endif %}