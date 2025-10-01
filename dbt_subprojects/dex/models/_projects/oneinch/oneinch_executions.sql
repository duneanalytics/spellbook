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
        unique_key = ['block_date', 'execution_id']
    )
}}



with

executions as (
    {% for stream, stream_data in oneinch_meta_cfg_macro()['streams'].items() %}
        select *
            , {{ stream_data['mode'] }} as mode
            , false as second_side
        from {{ ref('oneinch_' + stream + '_' + substream) }}
        {% if stream == 'lo' %}
            union all -- second side of LO calls (when a direct call LO method => users from two sides)
            select *
                , 'classic' as mode
                , true as second_side
            from {{ ref('oneinch_' + stream + '_' + substream) }}
            where true
                and protocol = 'LO'
                and flags['direct']
        {% endif %}
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
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
    , contract_name
    , amount_usd
    , execution_cost
    , if(second_side, tx_from, user) as user
    , if(second_side, cast(null as varbinary), receiver) as receiver
    , if(second_side, dst_token_address, src_token_address) as src_token_address
    , if(second_side, dst_token_amount, src_token_amount) as src_token_amount
    , if(second_side, dst_executed_address, src_executed_address) as src_executed_address
    , if(second_side, dst_executed_symbol, src_executed_symbol) as src_executed_symbol
    , if(second_side, dst_executed_amount, src_executed_amount) as src_executed_amount
    , if(second_side, dst_executed_amount_usd, src_executed_amount_usd) as src_executed_amount_usd
    , dst_blockchain
    , if(second_side, src_token_address, dst_token_address) as dst_token_address
    , if(second_side, src_token_amount, dst_token_amount) as dst_token_amount
    , if(second_side, src_executed_address, dst_executed_address) as dst_executed_address
    , if(second_side, src_executed_symbol, dst_executed_symbol) as dst_executed_symbol
    , if(second_side, src_executed_amount, dst_executed_amount) as dst_executed_amount
    , if(second_side, src_executed_amount_usd, dst_executed_amount_usd) as dst_executed_amount_usd
    , actions
    , order_hash
    , hashlock
    , complement
    , remains
    , map_concat(flags, map_from_entries(array[('second_side', second_side)])) as flags
    , block_date
    , block_month
    , native_price
    , native_decimals
    , sha1(to_utf8(concat_ws('|'
        , blockchain
        , cast(tx_hash as varchar)
        , array_join(call_trace_address, ',') -- ',' is necessary to avoid similarities after concatenation // array_join(array[1, 0], '') = array_join(array[10], '')
        , mode
    ))) as execution_id -- TO DO: try to make it orderly (with block_number & tx_index)
from executions
{% if is_incremental() %}
    where {{ incremental_predicate('block_time') }}
{% endif %}