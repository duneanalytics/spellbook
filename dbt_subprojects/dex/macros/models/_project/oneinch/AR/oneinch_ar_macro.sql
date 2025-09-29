{% macro oneinch_ar_macro(blockchain) %}

{% set stream = 'ar' %}
{% set substream = '_initial' %}
{% set meta = oneinch_meta_cfg_macro() %}
{% set contracts = meta['streams'][stream]['contracts'] %}
{% set date_from = [meta['blockchains']['start'][blockchain], meta['streams'][stream]['start'][substream]] | max %}
{% set wrapper = meta['blockchains']['wrapped_native_token_address'][blockchain] %}
{% set chain_id = meta['blockchains']['chain_id'][blockchain] %}
{% set native = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %}



with

raw_calls as (
    select *
        , substr(call_input, call_input_length - mod(call_input_length - 4, 32) + 1) as call_input_remains
    from {{ ref('oneinch_' + blockchain + '_ar_raw_calls') }}
    where true
        and block_date >= timestamp '{{ date_from }}' -- it is only needed for simple/easy dates
        {% if is_incremental() %}and {{ incremental_predicate('block_time') }}{% endif %}
)

, decoded as (
    {% for contract, contract_data in contracts.items() if blockchain in contract_data.blockchains %}
        {% for method, method_data in contract_data.methods.items() if blockchain in method_data.get('blockchains', contract_data.blockchains) and not method_data.get('auxiliary', false) %}{# method-level blockchains override contract-level blockchains #}
            select
                call_block_number as block_number
                , call_block_date as block_date
                , call_tx_hash as tx_hash
                , call_trace_address
                , {{ method_data.get("src_token_address", "null") }} as src_token_address
                , {{ method_data.get("dst_token_address", "null") }} as dst_token_address
                , {{ method_data.get("src_receiver", "null") }} as src_receiver
                , {{ method_data.get("dst_receiver", "null") }} as dst_receiver
                , {{ method_data.get("src_token_amount", "null") }} as src_token_amount
                , {{ method_data.get("dst_token_amount", "null") }} as dst_token_amount
                , {{ method_data.get("dst_token_amount_min", "null") }} as dst_token_amount_min
                , {{ method_data.get("pools", "array[]") }} as raw_pools
                , {{ method_data.get("pool_type_mask", "null") }} as pool_type_mask
                , {{ method_data.get("pool_type_offset", "null") }} as pool_type_offset
                , {{ method_data.get("direction_mask", "null") }} as direction_mask
                , {{ method_data.get("unwrap_mask", "null") }} as unwrap_mask
                , {{ method_data.get("src_token_mask", "null") }} as src_token_mask
                , {{ method_data.get("src_token_offset", "null") }} as src_token_offset
                , {{ method_data.get("dst_token_mask", "null") }} as dst_token_mask
                , {{ method_data.get("dst_token_offset", "null") }} as dst_token_offset
                , {{ method_data.get("router_type", "null") }} as router_type
            from {{ source('oneinch_' + blockchain, contract + '_call_' + method) }}
            join raw_calls using(block_date, block_number, tx_hash, call_trace_address)
            where true
                and call_block_date >= timestamp '{{ date_from }}' -- it is only needed for simple/easy dates
                {% if is_incremental() %}and {{ incremental_predicate('call_block_time') }}{% endif %}
            {% if not loop.last %}union all{% endif %}
        {% endfor %}
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

, auxiliary as (
    select
        block_number
        , block_date
        , tx_hash
        , min_by(call_input_remains, call_trace_address) as auxiliary_remains
        , min_by(call_from, call_trace_address) as auxiliary_call_from
        , min_by(call_to, call_trace_address) as auxiliary_call_to
        , min(trace_address) as auxiliary_trace_address
    from raw_calls
    where auxiliary
    group by 1, 2, 3, 4
)

, pools_list as (
    select
        pool
        , tokens
    from {{ ref('dex_raw_pools') }}
    where true
        and blockchain = '{{ blockchain }}'
        and type in ('uniswap_compatible', 'curve_compatible')
    group by 1, 2
)

, processing as (
    select *
        , coalesce(
            src_token_address -- src_token_address from params
            , try(case -- try to get src_token_address from first pool: pools[1]
                when pools[1]['pool_type'] = 2 then first_pool_tokens[cast(pools[1]['src_token_index'] as int) + 1] -- when pool type = 2, i.e Curve pool, than get src token address from first_pool_tokens by src token index
                else first_pool_tokens[cast(pools[1]['direction'] as int) + 1] -- when other cases, i.e. Uniswap compatible pool, than get src token address from first_pool_tokens by direction
            end)
        ) as pool_src_token_address
        , coalesce(
            dst_token_address -- dst_token_address from params
            , try(case -- try to get dst_token_address from last pool: reverse(pools)[1]
                when reverse(pools)[1]['pool_type'] = 2 then last_pool_tokens[cast(reverse(pools)[1]['dst_token_index'] as int) + 1] -- when pool type = 2, i.e Curve pool, than get dst token address from last_pool_tokens by dst token index
                else last_pool_tokens[cast(bitwise_xor(reverse(pools)[1]['direction'], 1) as int) + 1] -- when other cases, i.e. Uniswap compatible pool, than get dst token address from last_pool_tokens by direction
            end)
        ) as pool_dst_token_address
        , transform(parsed_pools, x -> map_from_entries(array[
            ('type', substr(cast(x['pool_type'] as varbinary), 32))
            , ('info', substr(cast(x['pool'] as varbinary), 1, 12))
            , ('unwrap', substr(reverse(cast(x['unwrap'] as varbinary)), 1, 1))
            , ('address', substr(cast(x['pool'] as varbinary), 13))
        ])) as pools
    from (
        select *
            , try(substr(cast(call_pools[1] as varbinary), 13)) as first_pool
            , try(substr(cast(reverse(call_pools)[1] as varbinary), 13)) as last_pool
            , transform(call_pools, x -> map_from_entries(array[
                ('pool', x) -- raw pool data in uint256
                , ('pool_type', bitwise_right_shift(bitwise_and(x, pool_type_mask), pool_type_offset))
                , ('direction', bitwise_xor(bit_count(bitwise_and(x, direction_mask), 256), if(protocol_version < 6, 0, 1))) -- until v6, the set direction bit meant the reverse direction, starting from v6, the set direction bit means the ordinary direction
                , ('unwrap', bit_count(bitwise_and(x, unwrap_mask), 256))
                , ('src_token_index', bitwise_right_shift(bitwise_and(x, src_token_mask), src_token_offset))
                , ('dst_token_index', bitwise_right_shift(bitwise_and(x, dst_token_mask), dst_token_offset))
            ])) as parsed_pools
            , if(slice(call_trace_address, 1, length(auxiliary_trace_address)) = auxiliary_trace_address, auxiliary_remains, call_input_remains) as actual_remains
            , if(slice(call_trace_address, 1, length(auxiliary_trace_address)) = auxiliary_trace_address, auxiliary_call_from, call_from) as actual_call_from
            , if(slice(call_trace_address, 1, length(auxiliary_trace_address)) = auxiliary_trace_address, auxiliary_call_to, call_to) as actual_call_to
        from (
            select *
                , if(router_type = 'unoswap' and cardinality(raw_pools) = 0
                    , array[bytearray_to_uint256(substr(call_input, call_input_length - 32 - mod(call_input_length - 4, 32) + 1, 32))] -- last 32 bytes of input without remains
                    , raw_pools
                ) as call_pools
                , if(router_type = 'unoswap', cardinality(raw_pools) > 0) as ordinary -- true if call pools is not empty, null for generic
            from decoded
            join raw_calls using(block_date, block_number, tx_hash, call_trace_address) -- to avoid listing all raw_calls columns in the previous step (in decoded cte)
        )
        left join auxiliary using(block_date, block_number, tx_hash)
    )
    left join (select pool as first_pool, tokens as first_pool_tokens from pools_list) using(first_pool)
    left join (select pool as last_pool, tokens as last_pool_tokens from pools_list) using(last_pool)
)

, native_prices as ( -- joining prices at this level, not on "raw_transfers", because there could be a call without transfers for which the tx cost needs to be calculated
    select
        minute
        , price
        , decimals
    from {{ source('prices', 'usd') }}
    where true
        and blockchain = '{{ blockchain }}'
        and contract_address = {{ wrapper }}
        and minute >= timestamp '{{ date_from }}'
        {% if is_incremental() %}and {{ incremental_predicate('minute') }}{% endif %}
)

-- output --

select
    blockchain
    , {{ chain_id }} as chain_id
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
    , tx_index -- it is necessary to determine the order in the block
    , call_trace_address
    , call_success
    , call_gas_used
    , call_selector
    , call_method
    , actual_call_from as call_from
    , actual_call_to as call_to
    , call_output
    , call_error
    , call_type
    , protocol
    , protocol_version
    , contract_name
    , src_receiver
    , dst_receiver
    , if(element_at(pools[1], 'unwrap') = 0x01 and pool_src_token_address = {{ wrapper }} and call_value > uint256 '0', {{ native }}, pool_src_token_address) as src_token_address
    , if(element_at(reverse(pools)[1], 'unwrap') = 0x01 and pool_dst_token_address = {{ wrapper }}, {{ native }}, pool_dst_token_address) as dst_token_address
    , src_token_amount
    , dst_token_amount
    , dst_token_amount_min
    , router_type
    , pools
    , coalesce(try(transform(sequence(1, length(actual_remains), 4), x -> bytearray_to_bigint(reverse(substr(reverse(actual_remains), x, 4))))), array[]) as remains
    , map_from_entries(array[
        ('ordinary', ordinary)
        , ('direct', _call_from = tx_from and _call_to = tx_to) -- == cardinality(call_trace_address) = 0, but due to zksync trace structure, it is necessary to switch to this
    ]) as flags
    , minute
    , block_date
    , block_month
    , price as native_price
    , decimals as native_decimals
from ({{
    add_tx_columns(
        model_cte = 'processing'
        , blockchain = blockchain
        , columns = ['from', 'to', 'success', 'nonce', 'gas_price', 'priority_fee_per_gas', 'gas_used', 'index']
    )
}}) as t
left join native_prices using(minute)

{% endmacro %}