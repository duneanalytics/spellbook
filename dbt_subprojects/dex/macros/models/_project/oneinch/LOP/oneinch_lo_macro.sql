{% macro oneinch_lo_macro(blockchain) %}

{% set stream = 'lo' %}
{% set substream = '_initial' %}
{% set meta = oneinch_meta_cfg_macro() %}
{% set contracts = meta['streams'][stream]['contracts'] %}
{% set date_from = [meta['blockchains']['start'][blockchain], meta['streams'][stream]['start'][substream]] | max %}
{% set wrapper = meta['blockchains']['wrapped_native_token_address'][blockchain] %}
{% set chain_id = meta['blockchains']['chain_id'][blockchain] %}
{% set settlements = meta['blockchains']['fusion_settlement_addresses'][blockchain] | join(', ') %}
{% set factories = meta['blockchains']['escrow_factory_addresses'][blockchain] | join(', ') %}



with

decoded as (
    {% for contract, contract_data in contracts.items() if blockchain in contract_data['blockchains'] %}
        -- CONTRACT: {{ contract }} --
        {% for method, method_data in contract_data.methods.items() %}
            select
                call_block_number as block_number
                , call_block_date as block_date
                , call_tx_hash as tx_hash
                , call_trace_address
                , {{ method_data.get("maker", "cast(null as varbinary)") }} as maker
                , {{ method_data.get("receiver", "cast(null as varbinary)") }} as receiver
                , {{ method_data.get("maker_asset", "cast(null as varbinary)") }} as maker_asset
                , {{ method_data.get("taker_asset", "cast(null as varbinary)") }} as taker_asset
                , {{ method_data.get("maker_amount", "cast(null as varbinary)") }} as maker_amount
                , {{ method_data.get("taker_amount", "cast(null as varbinary)") }} as taker_amount
                , {{ method_data.get("making_amount", "cast(null as varbinary)") }} as making_amount
                , {{ method_data.get("taking_amount", "cast(null as varbinary)") }} as taking_amount
                , {{ method_data.get("order_hash", "cast(null as varbinary)") }} as order_hash
                , {{ method_data.get("order_remains", "0x0000000000") }} as order_remains
                , {% if method_data.args %}reduce(array[{{ settlements }}], false, (r, x) -> r or coalesce(varbinary_position({{ method_data.args }}, x), 0) > 0, r -> r){% else %}false{% endif %} as settlement_in_args
                , {% if method_data.args %}reduce(array[{{ factories }}], false, (r, x) -> r or coalesce(varbinary_position({{ method_data.args }}, x), 0) > 0, r -> r){% else %}false{% endif %} as factory_in_args
                , {% if 'partial_bit' in method_data %}
                    try(bitwise_and( -- binary AND to allocate significant bit: necessary byte & mask (i.e. * bit weight)
                        bytearray_to_bigint(substr({{ method_data.maker_traits }}, {{ method_data.partial_bit }} / 8 + 1, 1)) -- current byte: partial_bit / 8 + 1 -- integer division
                        , cast(pow(2, {{ method_data.partial_bit }} - {{ method_data.partial_bit }} / 8 * 8) as bigint) -- 2 ^ (partial_bit - partial_bit / 8 * 8) -- bit_weights = array[128, 64, 32, 16, 8, 4, 2, 1]
                    ) = 0) -- if set, the order does not allow partial fills
                {% else %} null {% endif %} as partial
                , {% if 'multiple_bit' in method_data %}
                    try(bitwise_and( -- binary AND to allocate significant bit: necessary byte & mask (i.e. * bit weight)
                        bytearray_to_bigint(substr({{ method_data.maker_traits }}, {{ method_data.multiple_bit }} / 8 + 1, 1)) -- current byte: multiple_bit / 8 + 1 -- integer division
                        , cast(pow(2, {{ method_data.multiple_bit }} - {{ method_data.multiple_bit }} / 8 * 8) as bigint) -- 2 ^ (multiple_bit - multiple_bit / 8 * 8) -- bit_weights = array[128, 64, 32, 16, 8, 4, 2, 1]
                    ) > 0) -- if set, the order permits multiple fills
                {% else %} null {% endif %} as multiple
                , row_number() over(partition by call_tx_hash order by call_trace_address) as tx_call_id
            from (
                select *
                    , cast(json_parse({{ method_data.get("order", '"order"') }}) as map(varchar, varchar)) as order_map
                from {{ source('oneinch_' + blockchain, contract + '_call_' + method) }}
                where true
                    and call_block_date >= timestamp '{{ date_from }}' -- there are only calls after the contract creation in the decoded table
                    {% if is_incremental() %}and {{ incremental_predicate('call_block_time') }}{% endif %}
            )
            {% if not loop.last %}union all{% endif %}
        {% endfor %}
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

, raw_calls as (
    select *
        , substr(call_input, call_input_length - mod(call_input_length - 4, 32) + 1) as call_input_remains
        , call_from in ({{ settlements }}) as call_from_settlement
    from {{ ref('oneinch_' + blockchain + '_lo_raw_calls') }}
    where true
        and block_date >= timestamp '{{ date_from }}'
        {% if is_incremental() %}and {{ incremental_predicate('block_time') }}{% endif %}
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
    , call_from
    , call_to
    , call_output
    , call_error
    , call_type
    , protocol
    , protocol_version
    , contract_name
    , coalesce(order_hash, concat(tx_hash, substr(to_big_endian_64(tx_call_id), 5))) as order_hash
    , maker
    , receiver
    , maker_asset
    , maker_amount
    , coalesce(making_amount, try(cast(maker_amount * (cast(taking_amount as double) / taker_amount) as uint256))) as making_amount
    , taker_asset
    , taker_amount
    , taking_amount
    , concat(
        bytearray_to_bigint(order_remains)
        , coalesce(try(transform(sequence(1, length(call_input_remains), 4), x -> bytearray_to_bigint(reverse(substr(reverse(call_input_remains), x, 4))))), array[])
    ) as remains
    , map_from_entries(array[
        ('direct', call_from = tx_from and call_to = tx_to) -- == cardinality(call_trace_address) = 0, but because of zksync trace structure switched to this
        , ('fusion', call_from_settlement or settlement_in_args)
        , ('cross_chain', factory_in_args)
        , ('partial', partial)
        , ('multiple', multiple)
        , ('contracts_only', position('RFQ' in call_method) > 0 or partial and not multiple)
    ]) as flags
    , minute
    , block_date
    , block_month
    , price as native_price
    , decimals as native_decimals
from ({{
    add_tx_columns(
        model_cte = 'decoded'
        , blockchain = blockchain
        , columns = ['from', 'to', 'nonce', 'gas_price', 'priority_fee_per_gas', 'gas_used', 'index']
    )
}}) as t
join raw_calls using(block_date, block_number, tx_hash, call_trace_address)
left join native_prices using(minute)

{% endmacro %}