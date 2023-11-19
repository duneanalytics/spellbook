{% macro 
    oneinch_ar_macro(
        blockchain
    ) 
%}



-- METHODS CONFIG
{%
    set samples = {
        "aggregate": {
            "src_token_address": "fromToken",
            "dst_token_address": "toToken",
            "src_amount":        "tokensAmount",
            "dst_amount":        "output_returnAmount",
            "dst_amount_min":    "minTokensAmount",
            "router_type":       "generic",
        },
        "swap_1": {
            "src_token_address": "fromToken",
            "dst_token_address": "toToken",
            "src_amount":        "fromTokenAmount",
            "dst_amount":        "output_returnAmount",
            "dst_amount_min":    "minReturnAmount",
            "router_type":       "generic",
        },
        "swap_2": {
            "kit":               "cast(json_parse(desc) as map(varchar, varchar))",
            "src_token_address": "from_hex(kit['srcToken'])",
            "dst_token_address": "from_hex(kit['dstToken'])",
            "src_receiver":      "from_hex(kit['srcReceiver'])",
            "dst_receiver":      "from_hex(kit['dstReceiver'])",
            "src_amount":        "cast(kit['amount'] as uint256)",
            "dst_amount":        "output_returnAmount",
            "dst_amount_min":    "cast(kit['minReturnAmount'] as uint256)",
            "router_type":       "generic",
        },
        "unoswap_1": {
            "pools":             "pools",
            "src_token_address": "srcToken",
            "src_amount":        "amount",
            "dst_amount":        "output_returnAmount",
            "dst_amount_min":    "minReturn",
            "direction_bit":     "1",
            "router_type":       "unoswap",
        },
        "uniswap_1": {
            "pools":             "pools",
            "src_amount":        "amount",
            "dst_amount":        "output_returnAmount",
            "dst_amount_min":    "minReturn",
            "direction_bit":     "1",
            "router_type":       "unoswap",
        },
        "clipper_1": {
            "src_token_address": "srcToken",
            "dst_token_address": "dstToken",
            "src_amount":        "amount",
            "dst_amount":        "output_returnAmount",
            "dst_amount_min":    "minReturn",
            "router_type":       "clipper",
        },
        "clipper_2": {
            "src_token_address": "srcToken",
            "dst_token_address": "dstToken",
            "src_amount":        "inputAmount",
            "dst_amount":        "output_returnAmount",
            "dst_amount_min":    "goodUntil",
            "router_type":       "clipper",
        },
    }
%}
-- direction_bit: the number of the bit on the left in 32 bytes with pool address that indicates the direction of exchange



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
                "swap": samples["swap_1"],
            },
        },
        "AggregationRouterV1": {
            "version": "1",
            "blockchains": ["ethereum"],
            "start": "2019-09-28",
            "methods": {
                "swap": samples["swap_1"],
            },
        },
        "AggregationRouterV2": {
            "version": "2",
            "blockchains": ["ethereum", "bnb"],
            "start": "2020-11-04",
            "methods": {
                "swap":           samples["swap_2"],
                "discountedSwap": samples["swap_2"],
            },
        },
        "AggregationRouterV3": {
            "version": "3",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "optimism"],
            "start": "2021-03-14",
            "methods": {
                "swap":              samples["swap_2"],
                "discountedSwap":    samples["swap_2"],
                "unoswap":           dict(samples["unoswap_1"], pools="_0", blockchains=["ethereum", "bnb", "polygon", "arbitrum"]),
                "unoswapWithPermit": dict(samples["unoswap_1"], blockchains=["ethereum", "bnb", "polygon", "arbitrum"]),
            },
        },
        "AggregationRouterV4": {
            "version": "4",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom"],
            "start": "2021-11-05",
            "methods": {
                "swap":                      samples["swap_2"],
                "discountedSwap":            dict(samples["swap_2"], blockchains=["bnb", "polygon"]),
                "clipperSwap":               dict(samples["clipper_1"], blockchains=["ethereum"]),
                "clipperSwapTo":             dict(samples["clipper_1"], blockchains=["ethereum"], dst_receiver="recipient"),
                "clipperSwapToWithPermit":   dict(samples["clipper_1"], blockchains=["ethereum"], dst_receiver="recipient"),
                "unoswap":                   samples["unoswap_1"],
                "unoswapWithPermit":         samples["unoswap_1"],
                "uniswapV3Swap":             samples["uniswap_1"],
                "uniswapV3SwapTo":           dict(samples["uniswap_1"], dst_receiver="recipient"),
                "uniswapV3SwapToWithPermit": dict(samples["uniswap_1"], dst_receiver="recipient"),
            },
        },
        "AggregationRouterV5": {
            "version": "5",
            "blockchains": ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom", "base"],
            "start": "2022-11-04",
            "methods": {
                "swap":                      samples["swap_2"],
                "clipperSwap":               samples["clipper_2"],
                "clipperSwapTo":             dict(samples["clipper_2"], dst_receiver="recipient"),
                "clipperSwapToWithPermit":   dict(samples["clipper_2"], dst_receiver="recipient"),
                "unoswap":                   samples["unoswap_1"],
                "unoswapTo":                 dict(samples["unoswap_1"], dst_receiver="recipient"),
                "unoswapToWithPermit":       dict(samples["unoswap_1"], dst_receiver="recipient"),
                "uniswapV3Swap":             samples["uniswap_1"],
                "uniswapV3SwapTo":           dict(samples["uniswap_1"], dst_receiver="recipient"),
                "uniswapV3SwapToWithPermit": dict(samples["uniswap_1"], dst_receiver="recipient"),
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
    , contract_name
    , protocol_version
    , method
    , call_from
    , call_to
    , call_trace_address
    , call_selector
    , src_token_address
    , dst_token_address
    , src_receiver
    , dst_receiver
    , src_amount
    , dst_amount
    , dst_amount_min
    , ordinary
    , pools
    , router_type
    , call_success
    , call_gas_used
    , concat(cast(length(remains) as bigint), if(length(remains) > 0
        , transform(sequence(1, length(remains), 4), x -> bytearray_to_bigint(reverse(substr(reverse(remains), x, 4))))
        , array[bigint '0']
    )) as remains
    , call_output
    , date_trunc('minute', block_time) as minute
    , date(date_trunc('month', block_time)) as block_month
from (
    {{
        add_tx_columns(
            model_cte = 'calls'
            , blockchain = blockchain
            , columns = ['from', 'to', 'success']
        )
    }}
)

{% endmacro %}