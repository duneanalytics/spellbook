{% macro 
    oneinch_ar_macro(
        blockchain
    ) 
%}



{% set native = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %}
-- METHODS SAMPLES
{%
    set samples = {
        "aggregate": {
            "src_token_address":    "fromToken",
            "dst_token_address":    "toToken",
            "src_token_amount":     "tokensAmount",
            "dst_token_amount":     "output_returnAmount",
            "dst_token_amount_min": "minTokensAmount",
            "router_type":          "generic",
        },
        "swap": {
            "kit":                  "cast(json_parse(desc) as map(varchar, varchar))",
            "src_token_address":    "from_hex(kit['srcToken'])",
            "dst_token_address":    "from_hex(kit['dstToken'])",
            "src_receiver":         "from_hex(kit['srcReceiver'])",
            "dst_receiver":         "from_hex(kit['dstReceiver'])",
            "src_token_amount":     "cast(kit['amount'] as uint256)",
            "dst_token_amount":     "output_returnAmount",
            "dst_token_amount_min": "cast(kit['minReturnAmount'] as uint256)",
            "router_type":          "generic",
        },
        "unoswap v3-v5": {
            "pools":                "pools",
            "src_token_amount":     "amount",
            "dst_token_amount":     "output_returnAmount",
            "dst_token_amount_min": "minReturn",
            "direction_mask":       "bytearray_to_uint256(0x8000000000000000000000000000000000000000000000000000000000000000)",
            "unwrap_mask":          "bytearray_to_uint256(0x4000000000000000000000000000000000000000000000000000000000000000)",
            "router_type":          "unoswap",
        },
        "unoswap v6": {
            "src_token_address":    "substr(cast(token as varbinary), 13)",
            "src_token_amount":     "amount",
            "dst_token_amount":     "output_returnAmount",
            "dst_token_amount_min": "minReturn",
            "pool_type_mask":       "bytearray_to_uint256(0xe000000000000000000000000000000000000000000000000000000000000000)",
            "pool_type_offset":     "253",
            "direction_mask":       "bytearray_to_uint256(0x0080000000000000000000000000000000000000000000000000000000000000)",
            "unwrap_mask":          "bytearray_to_uint256(0x1000000000000000000000000000000000000000000000000000000000000000)",
            "src_token_mask":       "bytearray_to_uint256(0x000000ff00000000000000000000000000000000000000000000000000000000)",
            "src_token_offset":     "224",
            "dst_token_mask":       "bytearray_to_uint256(0x0000ff0000000000000000000000000000000000000000000000000000000000)",
            "dst_token_offset":     "232",
            "router_type":          "unoswap",
        },
        "clipper": {
            "src_token_address":    "srcToken",
            "dst_token_address":    "dstToken",
            "src_token_amount":     "inputAmount",
            "dst_token_amount":     "output_returnAmount",
            "router_type":          "clipper",
        },
    }
%}

-- CONTRACTS CONFIG
{%
    set contracts = {
        "ExchangeV1": {
            "version": "0.1",
            "blockchains": ["ethereum"],
            "start": "2019-06-03",
            "end": "2020-09-18",
            "methods": {
                "aggregate": samples["aggregate"],
            },
        },
        "ExchangeV2": {
            "version": "0.2",
            "blockchains": ["ethereum"],
            "start": "2019-06-10",
            "end": "2020-09-18",
            "methods": {
                "aggregate": samples["aggregate"],
            },
        },
        "ExchangeV3": {
            "version": "0.3",
            "blockchains": ["ethereum"],
            "start": "2019-06-18",
            "end": "2020-09-18",
            "methods": {
                "aggregate": samples["aggregate"],
            },
        },
        "ExchangeV4": {
            "version": "0.4",
            "blockchains": ["ethereum"],
            "start": "2019-07-18",
            "end": "2020-09-18",
            "methods": {
                "aggregate": samples["aggregate"],
            },
        },
        "ExchangeV5": {
            "version": "0.5",
            "blockchains": ["ethereum"],
            "start": "2019-07-18",
            "end": "2020-09-18",
            "methods": {
                "aggregate": samples["aggregate"],
            },
        },
        "ExchangeV6": {
            "version": "0.6",
            "blockchains": ["ethereum"],
            "start": "2019-07-19",
            "end": "2020-09-18",
            "methods": {
                "aggregate": samples["aggregate"],
            },
        },
        "ExchangeV7": {
            "version": "0.7",
            "blockchains": ["ethereum"],
            "start": "2019-09-17",
            "end": "2019-09-29",
            "methods": {
                "swap": dict(samples["aggregate"], src_token_amount="fromTokenAmount", dst_token_amount_min="minReturnAmount"),
            },
        },
        "AggregationRouterV1": {
            "version": "1",
            "blockchains": ["ethereum"],
            "start": "2019-09-28",
            "methods": {
                "swap": dict(samples["aggregate"], src_token_amount="fromTokenAmount", dst_token_amount_min="minReturnAmount"),
            },
        },
        "AggregationRouterV2": {
            "version": "2",
            "blockchains": ["ethereum", "bnb"],
            "start": "2020-11-04",
            "methods": {
                "swap":           samples["swap"],
                "discountedSwap": samples["swap"],
            },
        },
        "AggregationRouterV3": {
            "version": "3",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "optimism"],
            "start": "2021-03-14",
            "methods": {
                "swap":              samples["swap"],
                "discountedSwap":    samples["swap"],
                "unoswap":           dict(samples["unoswap v3-v5"], blockchains=["ethereum","bnb","polygon","arbitrum"], src_token_address="srcToken", pools="transform(_0, x -> bytearray_to_uint256(x))"),
                "unoswapWithPermit": dict(samples["unoswap v3-v5"], blockchains=["ethereum","bnb","polygon","arbitrum"], src_token_address="srcToken", pools="transform(pools, x -> bytearray_to_uint256(x))"),
            },
        },
        "AggregationRouterV4": {
            "version": "4",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom"],
            "start": "2021-11-05",
            "methods": {
                "swap":                      samples["swap"],
                "discountedSwap":            dict(samples["swap"], blockchains=["bnb", "polygon"]),
                "clipperSwap":               dict(samples["clipper"], src_token_amount="amount", dst_token_amount_min="minReturn", blockchains=["ethereum"]),
                "clipperSwapTo":             dict(samples["clipper"], src_token_amount="amount", dst_token_amount_min="minReturn", blockchains=["ethereum"], dst_receiver="recipient"),
                "clipperSwapToWithPermit":   dict(samples["clipper"], src_token_amount="amount", dst_token_amount_min="minReturn", blockchains=["ethereum"], dst_receiver="recipient"),
                "unoswap":                   dict(samples["unoswap v3-v5"], src_token_address="srcToken", pools="transform(pools, x -> bytearray_to_uint256(x))"),
                "unoswapWithPermit":         dict(samples["unoswap v3-v5"], src_token_address="srcToken", pools="transform(pools, x -> bytearray_to_uint256(x))"),
                "uniswapV3Swap":             dict(samples["unoswap v3-v5"]),
                "uniswapV3SwapTo":           dict(samples["unoswap v3-v5"], dst_receiver="recipient"),
                "uniswapV3SwapToWithPermit": dict(samples["unoswap v3-v5"], dst_receiver="recipient"),
            },
        },
        "AggregationRouterV5": {
            "version": "5",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom", "base", "zksync"],
            "start": "2022-11-04",
            "methods": {
                "swap":                      samples["swap"],
                "clipperSwap":               samples["clipper"],
                "clipperSwapTo":             dict(samples["clipper"], dst_receiver="recipient"),
                "clipperSwapToWithPermit":   dict(samples["clipper"], dst_receiver="recipient"),
                "unoswap":                   dict(samples["unoswap v3-v5"], src_token_address="srcToken"),
                "unoswapTo":                 dict(samples["unoswap v3-v5"], src_token_address="srcToken", dst_receiver="recipient"),
                "unoswapToWithPermit":       dict(samples["unoswap v3-v5"], src_token_address="srcToken", dst_receiver="recipient"),
                "uniswapV3Swap":             dict(samples["unoswap v3-v5"]),
                "uniswapV3SwapTo":           dict(samples["unoswap v3-v5"], dst_receiver="recipient"),
                "uniswapV3SwapToWithPermit": dict(samples["unoswap v3-v5"], dst_receiver="recipient"),
            },
        },
        "AggregationRouterV6": {
            "version": "6",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom", "base", "zksync"],
            "start": "2024-02-12",
            "methods": {
                "swap":          dict(samples["swap"], src_token_amount="output_spentAmount"),
                "clipperSwap":   dict(samples["clipper"], src_token_address="substr(cast(srcToken as varbinary), 13)", blockchains=["ethereum","bnb","polygon","arbitrum","optimism","avalanche_c","gnosis","fantom","base"]),
                "clipperSwapTo": dict(samples["clipper"], src_token_address="substr(cast(srcToken as varbinary), 13)", blockchains=["ethereum","bnb","polygon","arbitrum","optimism","avalanche_c","gnosis","fantom","base"], dst_receiver="recipient"),
                "ethUnoswap":    dict(samples["unoswap v6"], src_token_address=native, src_token_amount="call_value", pools="array[dex]"),
                "ethUnoswap2":   dict(samples["unoswap v6"], src_token_address=native, src_token_amount="call_value", pools="array[dex,dex2]"),
                "ethUnoswap3":   dict(samples["unoswap v6"], src_token_address=native, src_token_amount="call_value", pools="array[dex,dex2,dex3]"),
                "ethUnoswapTo":  dict(samples["unoswap v6"], src_token_address=native, src_token_amount="call_value", dst_receiver='substr(cast("to" as varbinary), 13)', pools="array[dex]"),
                "ethUnoswapTo2": dict(samples["unoswap v6"], src_token_address=native, src_token_amount="call_value", dst_receiver='substr(cast("to" as varbinary), 13)', pools="array[dex,dex2]"),
                "ethUnoswapTo3": dict(samples["unoswap v6"], src_token_address=native, src_token_amount="call_value", dst_receiver='substr(cast("to" as varbinary), 13)', pools="array[dex,dex2,dex3]"),
                "unoswap":       dict(samples["unoswap v6"], pools="array[dex]"),
                "unoswap2":      dict(samples["unoswap v6"], pools="array[dex,dex2]"),
                "unoswap3":      dict(samples["unoswap v6"], pools="array[dex,dex2,dex3]"),
                "unoswapTo":     dict(samples["unoswap v6"], dst_receiver='substr(cast("to" as varbinary), 13)', pools="array[dex]"),
                "unoswapTo2":    dict(samples["unoswap v6"], dst_receiver='substr(cast("to" as varbinary), 13)', pools="array[dex,dex2]"),
                "unoswapTo3":    dict(samples["unoswap v6"], dst_receiver='substr(cast("to" as varbinary), 13)', pools="array[dex,dex2,dex3]"),
            },
        },
    }
%}



with 
-- pools tokens for unoswap/uniswap methods tokens parsing
pools_list as (
    select
        pool as pool_address
        , tokens
    from {{ ref('dex_raw_pools') }}
    where type in ('uniswap_compatible', 'curve_compatible')
    group by 1, 2
)


, calls as (
    {% for contract, contract_data in contracts.items() if blockchain in contract_data.blockchains %}
    
    select * from (
        with traces_cte as (
            select
                block_number as call_block_number
                , tx_hash as call_tx_hash
                , trace_address as call_trace_address
                , "from" as call_from
                , substr(input, 1, 4) as call_selector
                , gas_used as call_gas_used
                , input as call_input
                , length(input) as call_input_length
                , substr(input, length(input) - mod(length(input) - 4, 32) + 1) as remains
                , output as call_output
                , error as call_error
                , value as call_value
                , call_type
            from {{ source(blockchain, 'traces') }}
            where
                {% if is_incremental() %}
                    {{ incremental_predicate('block_time') }}
                {% else %}
                    block_time >= timestamp '{{ contract_data['start'] }}'
                {% endif %}
        )

    
        {% for method, method_data in contract_data.methods.items() if blockchain in method_data.get('blockchains', contract_data.blockchains) %} -- method-level blockchains override contract-level blockchains
            {% if method_data.router_type in ['generic', 'clipper'] %}
                {{ 
                    oneinch_ar_handle_generic(
                        contract=contract,
                        contract_data=contract_data,
                        method=method,
                        method_data=method_data,
                        blockchain=blockchain,
                        traces_cte=traces_cte,
                        start_date=contract_data['start'],
                    ) 
                }} 
            {% elif method_data.router_type in ['unoswap'] %}
                {{
                    oneinch_ar_handle_unoswap(
                        contract=contract,
                        contract_data=contract_data,
                        method=method,
                        method_data=method_data,
                        blockchain=blockchain,
                        traces_cte=traces_cte,
                        pools_list=pools_list,
                        native=native,
                        start_date=contract_data['start'],
                    )
                }}
            {% endif %}
        {% if not loop.last %} union all {% endif %}
        {% endfor %}
    )
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

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
    , 'AR' as protocol
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
    , src_receiver
    , dst_receiver
    , src_token_address
    , dst_token_address
    , src_token_amount
    , dst_token_amount
    , dst_token_amount_min
    , map_from_entries(array[('ordinary', ordinary)]) as flags
    , pools
    , router_type
    , concat(cast(length(remains) as bigint), if(length(remains) > 0
        , transform(sequence(1, length(remains), 4), x -> bytearray_to_bigint(reverse(substr(reverse(remains), x, 4))))
        , array[bigint '0']
    )) as remains
    , date_trunc('minute', block_time) as minute
    , date(date_trunc('month', block_time)) as block_month
from (
    {{
        add_tx_columns(
            model_cte = 'calls'
            , blockchain = blockchain
            , columns = ['from', 'to', 'success', 'nonce', 'gas_price', 'priority_fee_per_gas', 'gas_used']
        )
    }}
)

{% endmacro %}