{% macro 
    oneinch_ar_macro(
        blockchain
    ) 
%}



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
        "unoswap_v1": {
            "pools":                "pools",
            "src_token_address":    "srcToken",
            "src_token_amount":     "amount",
            "dst_token_amount":     "output_returnAmount",
            "dst_token_amount_min": "minReturn",
            "direction_bit":        "1",
            "router_type":          "unoswap",
        },
        "unoswap_v2": {
            "src_token_address":    "substr(cast(token as varbinary), 13)",
            "src_token_amount":     "amount",
            "dst_token_amount":     "output_returnAmount",
            "dst_token_amount_min": "minReturn",
            "direction_bit":        "9",
            "router_type":          "unoswap",
        },
        "ethunoswap": {
            "src_token_address":    "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            "src_token_amount":     "call_value",
            "dst_token_amount":     "output_returnAmount",
            "dst_token_amount_min": "minReturn",
            "direction_bit":        "9",
            "router_type":          "unoswap",
        },
        "uniswap": {
            "pools":                "pools",
            "src_token_amount":     "amount",
            "dst_token_amount":     "output_returnAmount",
            "dst_token_amount_min": "minReturn",
            "direction_bit":        "1",
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
-- direction_bit: the number of the bit on the left starting from 1 to 256 in 32 bytes with pool address that indicates the direction of exchange
-- direction_bit = 256 - [solidity offset (~ ZERO_FOR_ONE)]

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
                "unoswap":           dict(samples["unoswap_v1"], blockchains=["ethereum","bnb","polygon","arbitrum"], pools="_0"),
                "unoswapWithPermit": dict(samples["unoswap_v1"], blockchains=["ethereum","bnb","polygon","arbitrum"]),
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
                "unoswap":                   samples["unoswap_v1"],
                "unoswapWithPermit":         samples["unoswap_v1"],
                "uniswapV3Swap":             samples["uniswap"],
                "uniswapV3SwapTo":           dict(samples["uniswap"], dst_receiver="recipient"),
                "uniswapV3SwapToWithPermit": dict(samples["uniswap"], dst_receiver="recipient"),
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
                "unoswap":                   samples["unoswap_v1"],
                "unoswapTo":                 dict(samples["unoswap_v1"], dst_receiver="recipient"),
                "unoswapToWithPermit":       dict(samples["unoswap_v1"], dst_receiver="recipient"),
                "uniswapV3Swap":             samples["uniswap"],
                "uniswapV3SwapTo":           dict(samples["uniswap"], dst_receiver="recipient"),
                "uniswapV3SwapToWithPermit": dict(samples["uniswap"], dst_receiver="recipient"),
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
                "ethUnoswap":    dict(samples["ethunoswap"], pools="array[dex]"),
                "ethUnoswap2":   dict(samples["ethunoswap"], pools="array[dex,dex2]"),
                "ethUnoswap3":   dict(samples["ethunoswap"], pools="array[dex,dex2,dex3]"),
                "ethUnoswapTo":  dict(samples["ethunoswap"], dst_receiver='substr(cast("to" as varbinary), 13)', pools="array[dex]"),
                "ethUnoswapTo2": dict(samples["ethunoswap"], dst_receiver='substr(cast("to" as varbinary), 13)', pools="array[dex,dex2]"),
                "ethUnoswapTo3": dict(samples["ethunoswap"], dst_receiver='substr(cast("to" as varbinary), 13)', pools="array[dex,dex2,dex3]"),
                "unoswap":       dict(samples["unoswap_v2"], pools="array[dex]"),
                "unoswap2":      dict(samples["unoswap_v2"], pools="array[dex,dex2]"),
                "unoswap3":      dict(samples["unoswap_v2"], pools="array[dex,dex2,dex3]"),
                "unoswapTo":     dict(samples["unoswap_v2"], dst_receiver='substr(cast("to" as varbinary), 13)', pools="array[dex]"),
                "unoswapTo2":    dict(samples["unoswap_v2"], dst_receiver='substr(cast("to" as varbinary), 13)', pools="array[dex,dex2]"),
                "unoswapTo3":    dict(samples["unoswap_v2"], dst_receiver='substr(cast("to" as varbinary), 13)', pools="array[dex,dex2,dex3]"),
            },
        },
    }
%}



with 
-- pools tokens for unoswap/uniswap methods tokens parsing
pools_list as (
    select
        pool as pool_address
        , token0
        , token1
    from {{ ref('dex_raw_pools') }}
    where type = 'uniswap_compatible'
    group by 1, 2, 3
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
            from {{ source(blockchain, 'traces') }}
            where
                {% if is_incremental() %}
                    {{ incremental_predicate('block_time') }}
                {% else %}
                    block_time >= timestamp '{{ contract_data['start'] }}'
                {% endif %}
                    and call_type = 'call'
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
                        traces_cte=traces_cte
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
                        pools_list=pools_list
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
    , src_receiver
    , dst_receiver
    , src_token_address
    , dst_token_address
    , src_token_amount
    , dst_token_amount
    , dst_token_amount_min
    , ordinary
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