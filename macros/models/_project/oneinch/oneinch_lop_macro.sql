{% macro 
    oneinch_lop_macro(
        blockchain
    ) 
%}



-- METHODS SAMPLES
{%
    set samples = {
        "v2": {
            "maker":         "from_hex(order_map['maker'])",
            "maker_asset":   "from_hex(order_map['makerAsset'])",
            "taker_asset":   "from_hex(order_map['takerAsset'])",
            "making_amount": "output_0",
            "taking_amount": "output_1",
        },
        "v4": {
            "maker":         "substr(cast(cast(order_map['maker'] as uint256) as varbinary), 13)",
            "receiver":      "substr(cast(cast(order_map['receiver'] as uint256) as varbinary), 13)",
            "maker_asset":   "substr(cast(cast(order_map['makerAsset'] as uint256) as varbinary), 13)",
            "taker_asset":   "substr(cast(cast(order_map['takerAsset'] as uint256) as varbinary), 13)",
            "making_amount": "output_0",
            "taking_amount": "output_1",
            "order_hash":    "output_2",
            "maker_traits":  "cast(cast(order_map['makerTraits'] as uint256) as varbinary)",
            "partial_bit":   "1",
            "multiple_bit":  "2",
        }
    }
%}
-- partial_bit & multiple_bit: the number of the bit on the left starting from 1 to 256 in 32 bytes in MakerTraits struct
-- [the number of the bit] = 256 - [solidity offset (~ PARTIAL_FILLS & ~ MULTIPLE_FILLS)]

-- CONTRACTS CONFIG
{%
    set cfg = {
        "LimitOrderProtocolV1": {
            "version": "1",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "optimism"],
            "start": "2021-06-03",
            "methods": {
                "fillOrder":    dict(samples["v2"], maker="substr(from_hex(order_map['makerAssetData']), 4 + 12 + 1, 20)"),
                "fillOrderRFQ": dict(
                    samples["v2"],
                    maker=        "substr(from_hex(order_map['makerAssetData']), 4 + 12 + 1, 20)",
                    making_amount="bytearray_to_uint256(substr(from_hex(order_map['makerAssetData']), 4 + 32*2 + 1, 32))",
                    taking_amount="bytearray_to_uint256(substr(from_hex(order_map['takerAssetData']), 4 + 32*2 + 1, 32))",
                ),
            },
        },
        "LimitOrderProtocolV2": {
            "version": "2",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "avalanche_c", "gnosis", "optimism"],
            "start": "2021-11-26",
            "methods": {
                "fillOrder":                samples["v2"],
                "fillOrderTo":              dict(samples["v2"], receiver="from_hex(order_map['receiver'])"),
                "fillOrderToWithPermit":    dict(samples["v2"], receiver="from_hex(order_map['receiver'])"),
                "fillOrderRFQ":             samples["v2"],
                "fillOrderRFQTo":           samples["v2"],
                "fillOrderRFQToWithPermit": samples["v2"],
            },
        },
        "AggregationRouterV4": {
            "version": "2",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "avalanche_c", "gnosis", "optimism", "fantom"],
            "start": "2021-11-05",
            "methods": {
                "fillOrderRFQ":             samples["v2"],
                "fillOrderRFQTo":           samples["v2"],
                "fillOrderRFQToWithPermit": samples["v2"],
            },
        },
        "AggregationRouterV5": {
            "version": "3",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "avalanche_c", "gnosis", "optimism", "fantom", "base", "zksync"],
            "start": "2022-11-04",
            "methods": {
                "fillOrder":                dict(samples["v2"], order_hash="output_2"),
                "fillOrderTo":              dict(
                    samples["v2"],
                    order =         '"order_"',
                    making_amount = "output_actualMakingAmount",
                    taking_amount = "output_actualTakingAmount",
                    order_hash =    "output_orderHash",
                    receiver =      "from_hex(order_map['receiver'])",
                ),
                "fillOrderToWithPermit":    dict(samples["v2"], order_hash="output_2", receiver="from_hex(order_map['receiver'])"),
                "fillOrderRFQ":             dict(samples["v2"], order_hash="output_2"),
                "fillOrderRFQTo":           dict(
                    samples["v2"],
                    making_amount = "output_filledMakingAmount",
                    taking_amount = "output_filledTakingAmount",
                    order_hash =    "output_orderHash",
                ),
                "fillOrderRFQToWithPermit": dict(samples["v2"], order_hash="output_2"),
                "fillOrderRFQCompact":      dict(
                    samples["v2"],
                    making_amount = "output_filledMakingAmount",
                    taking_amount = "output_filledTakingAmount",
                    order_hash =    "output_orderHash",
                ),
            },
        },
        "AggregationRouterV6": {
            "version": "4",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "avalanche_c", "gnosis", "optimism", "fantom", "base"],
            "start": "2024-02-12",
            "methods": {
                "fillOrder":             samples["v4"],
                "fillOrderArgs":         samples["v4"],
                "fillContractOrder":     samples["v4"],
                "fillContractOrderArgs": samples["v4"],
            },
        },
    }
%}



with

orders as (
    {% for contract, contract_data in cfg.items() if blockchain in contract_data['blockchains'] %}
        select * from ({% for method, method_data in contract_data.methods.items() %}
            select
                call_block_number as block_number
                , call_block_time as block_time
                , call_tx_hash as tx_hash
                , '{{ contract }}' as contract_name
                , '{{ contract_data['version'] }}' as protocol_version
                , '{{ method }}' as method
                , call_trace_address
                , contract_address as call_to
                , call_success
                , {{ method_data.get("maker", "null") }} as maker
                , {{ method_data.get("receiver", "null") }} as receiver
                , {{ method_data.get("maker_asset", "null") }} as maker_asset
                , {{ method_data.get("taker_asset", "null") }} as taker_asset
                , {{ method_data.get("making_amount", "null") }} as making_amount
                , {{ method_data.get("taking_amount", "null") }} as taking_amount
                , {{ method_data.get("order_hash", "null") }} as order_hash
                , {% if 'partial_bit' in method_data %}
                    try(bitwise_and( -- binary AND to allocate significant bit: necessary byte & mask (i.e. * bit weight)
                        bytearray_to_bigint(substr({{ method_data.maker_traits }}, {{ method_data.partial_bit }} / 8 + 1, 1)) -- current byte: partial_bit / 8 + 1 -- integer division
                        , cast(pow(2, {{ method_data.partial_bit }} - {{ method_data.partial_bit }} / 8 * 8) as bigint) -- 2 ^ (partial_bit - partial_bit / 8 * 8) -- bit_weights = array[128, 64, 32, 16, 8, 4, 2, 1]
                    ) = 0) -- if set, the order does not allow partial fills
                {% else %} null {% endif %} as _partial
                , {% if 'multiple_bit' in method_data %}
                    try(bitwise_and( -- binary AND to allocate significant bit: necessary byte & mask (i.e. * bit weight)
                        bytearray_to_bigint(substr({{ method_data.maker_traits }}, {{ method_data.multiple_bit }} / 8 + 1, 1)) -- current byte: multiple_bit / 8 + 1 -- integer division
                        , cast(pow(2, {{ method_data.multiple_bit }} - {{ method_data.multiple_bit }} / 8 * 8) as bigint) -- 2 ^ (multiple_bit - multiple_bit / 8 * 8) -- bit_weights = array[128, 64, 32, 16, 8, 4, 2, 1]
                    ) = 1) -- if set, the order permits multiple fills
                {% else %} null {% endif %} as _multiple
            from (
                select *, cast(json_parse({{ method_data.get("order", '"order"') }}) as map(varchar, varchar)) as order_map
                from {{ source('oneinch_' + blockchain, contract + '_call_' + method) }}
                {% if is_incremental() %} 
                    where {{ incremental_predicate('call_block_time') }}
                {% endif %}
            )
            {% if not loop.last %} union all {% endif %}
        {% endfor %})
        join (
            select
                block_number
                , tx_hash
                , trace_address as call_trace_address
                , "from" as call_from
                , substr(input, 1, 4) as call_selector
                , gas_used as call_gas_used
                , substr(input, length(input) - mod(length(input) - 4, 32) + 1) as remains
                , output as call_output
                , error as call_error
                , call_type
            from {{ source(blockchain, 'traces') }}
            where
                {% if is_incremental() %} 
                    {{ incremental_predicate('block_time') }}
                {% else %}
                    block_time >= timestamp '{{ contract_data['start'] }}'
                {% endif %}
        ) using(block_number, tx_hash, call_trace_address)
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

-- output --

select
    '{{ blockchain }}' as blockchain
    , block_number
    , block_time
    , tx_hash
    , tx_from
    , tx_to
    , tx_success
    , tx_nonce
    , tx_gas_used
    , tx_gas_price
    , tx_priority_fee_per_gas
    , contract_name
    , 'LOP' as protocol
    , protocol_version
    , method
    , call_selector
    , call_trace_address
    , call_from
    , call_to
    , call_success
    , call_gas_used
    , call_output
    , call_error
    , call_type
    , maker
    , receiver
    , maker_asset
    , making_amount
    , taker_asset
    , taking_amount
    , order_hash
    , map_from_entries(array[
        ('partial', _partial)
        , ('multiple', _multiple)
        , ('first', row_number() over(partition by coalesce(order_hash, tx_hash) order by block_number, tx_index, call_trace_address) = 1)
    ]) as flags
    , concat(cast(length(remains) as bigint), if(length(remains) > 0
        , transform(sequence(1, length(remains), 4), x -> bytearray_to_bigint(reverse(substr(reverse(remains), x, 4))))
        , array[bigint '0']
    )) as remains
    , date_trunc('minute', block_time) as minute
    , date(date_trunc('month', block_time)) as block_month
from (
    {{
        add_tx_columns(
            model_cte = 'orders'
            , blockchain = blockchain
            , columns = ['from', 'to', 'success', 'nonce', 'gas_price', 'priority_fee_per_gas', 'gas_used', 'index']
        )
    }}
)

{% endmacro %}
