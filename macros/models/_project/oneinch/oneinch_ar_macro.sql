{% macro 
    oneinch_ar_macro(
        blockchain
    ) 
%}



{%
    set contracts = {
        'ExchangeV1': {
            'version': '0.1',
            'blockchains': ["ethereum"],
            'start': '2019-06-03',
            'end': '2020-09-18',
            'methods': {
                'aggregate': {
                    'src_token_address': 'fromToken',
                    'dst_token_address': 'toToken',
                    'src_amount': 'tokensAmount',
                    'dst_amount': 'output_returnAmount',
                    'dst_amount_min': 'minTokensAmount',
                    'blockchains': ["ethereum"],
                    'type': 'generic'
                }
            }
        },
        'ExchangeV2': {
            'version': '0.2',
            'blockchains': ["ethereum"],
            'start': '2019-06-10',
            'end': '2020-09-18',
            'methods': {
                'aggregate': {
                    'src_token_address': 'fromToken',
                    'dst_token_address': 'toToken',
                    'src_amount': 'tokensAmount',
                    'dst_amount': 'output_returnAmount',
                    'dst_amount_min': 'minTokensAmount',
                    'blockchains': ["ethereum"],
                    'type': 'generic'
                }
            }
        },
        'ExchangeV3': {
            'version': '0.3',
            'blockchains': ["ethereum"],
            'start': '2019-06-18',
            'end': '2020-09-18',
            'methods': {
                'aggregate': {
                    'src_token_address': 'fromToken',
                    'dst_token_address': 'toToken',
                    'src_amount': 'tokensAmount',
                    'dst_amount': 'output_returnAmount',
                    'dst_amount_min': 'minTokensAmount',
                    'blockchains': ["ethereum"],
                    'type': 'generic'
                }
            }
        },
        'ExchangeV4': {
            'version': '0.4',
            'blockchains': ["ethereum"],
            'start': '2019-07-18',
            'end': '2020-09-18',
            'methods': {
                'aggregate': {
                    'src_token_address': 'fromToken',
                    'dst_token_address': 'toToken',
                    'src_amount': 'tokensAmount',
                    'dst_amount': 'output_returnAmount',
                    'dst_amount_min': 'minTokensAmount',
                    'blockchains': ["ethereum"],
                    'type': 'generic'
                }
            }
        },
        'ExchangeV5': {
            'version': '0.5',
            'blockchains': ["ethereum"],
            'start': '2019-07-18',
            'end': '2020-09-18',
            'methods': {
                'aggregate': {
                    'src_token_address': 'fromToken',
                    'dst_token_address': 'toToken',
                    'src_amount': 'tokensAmount',
                    'dst_amount': 'output_returnAmount',
                    'dst_amount_min': 'minTokensAmount',
                    'blockchains': ["ethereum"],
                    'type': 'generic'
                }
            }
        },
        'ExchangeV6': {
            'version': '0.6',
            'blockchains': ["ethereum"],
            'start': '2019-07-19',
            'end': '2020-09-18',
            'methods': {
                'aggregate': {
                    'src_token_address': 'fromToken',
                    'dst_token_address': 'toToken',
                    'src_amount': 'tokensAmount',
                    'dst_amount': 'output_returnAmount',
                    'dst_amount_min': 'minTokensAmount',
                    'blockchains': ["ethereum"],
                    'type': 'generic'
                }
            }
        },
        'ExchangeV7': {
            'version': '0.7',
            'blockchains': ["ethereum"],
            'start': '2019-09-17',
            'end': '2019-09-29',
            'methods': {
                'swap': {
                    'src_token_address': 'fromToken',
                    'dst_token_address': 'toToken',
                    'src_amount': 'fromTokenAmount',
                    'dst_amount': 'output_returnAmount',
                    'dst_amount_min': 'minReturnAmount',
                    'blockchains': ["ethereum"],
                    'type': 'generic'
                }
            }
        },
        'AggregationRouterV1': {
            'version': '1',
            'blockchains': ["ethereum"],
            'start': '2019-09-28',
            'methods': {
                'swap': {
                    'src_token_address': 'fromToken',
                    'dst_token_address': 'toToken',
                    'src_amount': 'fromTokenAmount',
                    'dst_amount': 'output_returnAmount',
                    'dst_amount_min': 'minReturnAmount',
                    'blockchains': ["ethereum"],
                    'type': 'generic'
                }
            }
        },
        'AggregationRouterV2': {
            'version': '2',
            'blockchains': ["ethereum", "bnb"],
            'start': '2020-11-04',
            'methods': {
                'swap': {
                    'kit': "cast(json_parse(desc) as map(varchar, varchar))",
                    'src_token_address': "from_hex(kit['srcToken'])",
                    'dst_token_address': "from_hex(kit['dstToken'])",
                    'src_receiver': "from_hex(kit['srcReceiver'])",
                    'dst_receiver': "from_hex(kit['dstReceiver'])",
                    'src_amount': "cast(kit['amount'] as uint256)",
                    'dst_amount': 'output_returnAmount',
                    'dst_amount_min': "cast(kit['minReturnAmount'] as uint256)",
                    'blockchains': ["ethereum", "bnb"],
                    'type': 'generic'
                },
                'discountedSwap': {
                    'kit': "cast(json_parse(desc) as map(varchar, varchar))",
                    'src_token_address': "from_hex(kit['srcToken'])",
                    'dst_token_address': "from_hex(kit['dstToken'])",
                    'src_receiver': "from_hex(kit['srcReceiver'])",
                    'dst_receiver': "from_hex(kit['dstReceiver'])",
                    'src_amount': "cast(kit['amount'] as uint256)",
                    'dst_amount': 'output_returnAmount',
                    'dst_amount_min': "cast(kit['minReturnAmount'] as uint256)",
                    'blockchains': ["ethereum", "bnb"],
                    'type': 'generic'
                }
            }
        },
        'AggregationRouterV3': {
            'version': '3',
            'blockchains': ["ethereum", "bnb", "polygon", "arbitrum", "optimism"],
            'start': '2021-03-14',
            'methods': {
                'swap': {
                    'kit': "cast(json_parse(desc) as map(varchar, varchar))",
                    'src_token_address': "from_hex(kit['srcToken'])",
                    'dst_token_address': "from_hex(kit['dstToken'])",
                    'src_receiver': "from_hex(kit['srcReceiver'])",
                    'dst_receiver': "from_hex(kit['dstReceiver'])",
                    'src_amount': "cast(kit['amount'] as uint256)",
                    'dst_amount': 'output_returnAmount',
                    'dst_amount_min': "cast(kit['minReturnAmount'] as uint256)",
                    'blockchains': ["ethereum", "bnb", "polygon", "arbitrum", "optimism"],
                    'type': 'generic'
                },
                'discountedSwap': {
                    'kit': "cast(json_parse(desc) as map(varchar, varchar))",
                    'src_token_address': "from_hex(kit['srcToken'])",
                    'dst_token_address': "from_hex(kit['dstToken'])",
                    'src_receiver': "from_hex(kit['srcReceiver'])",
                    'dst_receiver': "from_hex(kit['dstReceiver'])",
                    'src_amount': "cast(kit['amount'] as uint256)",
                    'dst_amount': 'output_returnAmount',
                    'dst_amount_min': "cast(kit['minReturnAmount'] as uint256)",
                    'blockchains': ["ethereum", "bnb", "polygon", "arbitrum", "optimism"],
                    'type': 'generic'
                },
                'unoswap': {
                    'pools': "_0",
                    'src_token_address': "srcToken",
                    'src_amount': "amount",
                    'dst_amount': "output_returnAmount",
                    'dst_amount_min': "minReturn",
                    'blockchains': ["ethereum", "bnb", "polygon", "arbitrum"],
                    'type': 'unoswap'
                },
                'unoswapWithPermit': {
                    'pools': "pools",
                    'src_token_address': "srcToken",
                    'src_amount': "amount",
                    'dst_amount': "output_returnAmount",
                    'dst_amount_min': "minReturn",
                    'blockchains': ["ethereum", "bnb", "polygon", "arbitrum"],
                    'type': 'unoswap'
                }
            }
        },
        'AggregationRouterV4': {
            'version': '4',
            'blockchains': ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom"],
            'start': '2021-11-05',
            'methods': {
                'swap': {
                    'kit': "cast(json_parse(desc) as map(varchar, varchar))",
                    'src_token_address': "from_hex(kit['srcToken'])",
                    'dst_token_address': "from_hex(kit['dstToken'])",
                    'src_receiver': "from_hex(kit['srcReceiver'])",
                    'dst_receiver': "from_hex(kit['dstReceiver'])",
                    'src_amount': "cast(kit['amount'] as uint256)",
                    'dst_amount': 'output_returnAmount',
                    'dst_amount_min': "cast(kit['minReturnAmount'] as uint256)",
                    'blockchains': ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom"],
                    'type': 'generic'
                },
                'discountedSwap': {
                    'kit': "cast(json_parse(desc) as map(varchar, varchar))",
                    'src_token_address': "from_hex(kit['srcToken'])",
                    'dst_token_address': "from_hex(kit['dstToken'])",
                    'src_receiver': "from_hex(kit['srcReceiver'])",
                    'dst_receiver': "from_hex(kit['dstReceiver'])",
                    'src_amount': "cast(kit['amount'] as uint256)",
                    'dst_amount': 'output_returnAmount',
                    'dst_amount_min': "cast(kit['minReturnAmount'] as uint256)",
                    'blockchains': ["bnb", "polygon"],
                    'type': 'generic'
                },
                'clipperSwap': {
                    'src_token_address': "srcToken",
                    'dst_token_address': "dstToken",
                    'src_amount': "amount",
                    'dst_amount': 'output_returnAmount',
                    'dst_amount_min': "minReturn",
                    'blockchains': ["ethereum"],
                    'type': 'clipper'
                },
                'clipperSwapTo': {
                    'src_token_address': "srcToken",
                    'dst_token_address': "dstToken",
                    'dst_receiver': "recipient",
                    'src_amount': "amount",
                    'dst_amount': 'output_returnAmount',
                    'dst_amount_min': "minReturn",
                    'blockchains': ["ethereum"],
                    'type': 'clipper'
                },
                'clipperSwapToWithPermit': {
                    'src_token_address': "srcToken",
                    'dst_token_address': "dstToken",
                    'dst_receiver': "recipient",
                    'src_amount': "amount",
                    'dst_amount': 'output_returnAmount',
                    'dst_amount_min': "minReturn",
                    'blockchains': ["ethereum"],
                    'type': 'clipper'
                },
                'unoswap': {
                    'pools': "pools",
                    'src_token_address': "srcToken",
                    'src_amount': "amount",
                    'dst_amount': "output_returnAmount",
                    'dst_amount_min': "minReturn",
                    'blockchains': ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom"],
                    'type': 'unoswap'
                },
                'unoswapWithPermit': {
                    'pools': "pools",
                    'src_token_address': "srcToken",
                    'src_amount': "amount",
                    'dst_amount': "output_returnAmount",
                    'dst_amount_min': "minReturn",
                    'blockchains': ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom"],
                    'type': 'unoswap'
                },
                'uniswapV3Swap': {
                    'pools': "pools",
                    'src_amount': "amount",
                    'dst_amount': "output_returnAmount",
                    'dst_amount_min': "minReturn",
                    'blockchains': ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom"],
                    'type': 'unoswap'
                },
                'uniswapV3SwapTo': {
                    'pools': "pools",
                    'dst_receiver': "recipient",
                    'src_amount': "amount",
                    'dst_amount': "output_returnAmount",
                    'dst_amount_min': "minReturn",
                    'blockchains': ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom"],
                    'type': 'unoswap'
                },
                'uniswapV3SwapToWithPermit': {
                    'pools': "pools",
                    'dst_receiver': "recipient",
                    'src_amount': "amount",
                    'dst_amount': "output_returnAmount",
                    'dst_amount_min': "minReturn",
                    'blockchains': ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom"],
                    'type': 'unoswap'
                }
            }
        },
        'AggregationRouterV5': {
            'version': '5',
            'blockchains': ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom", "base"],
            'start': '2022-11-04',
            'methods': {
                'swap': {
                    'kit': "cast(json_parse(desc) as map(varchar, varchar))",
                    'src_token_address': "from_hex(kit['srcToken'])",
                    'dst_token_address': "from_hex(kit['dstToken'])",
                    'src_receiver': "from_hex(kit['srcReceiver'])",
                    'dst_receiver': "from_hex(kit['dstReceiver'])",
                    'src_amount': "cast(kit['amount'] as uint256)",
                    'dst_amount': 'output_returnAmount',
                    'dst_amount_min': "cast(kit['minReturnAmount'] as uint256)",
                    'blockchains': ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom", "base"],
                    'type': 'generic'
                },
                'clipperSwap': {
                    'src_token_address': "srcToken",
                    'dst_token_address': "dstToken",
                    'src_amount': "inputAmount",
                    'dst_amount': 'output_returnAmount',
                    'dst_amount_min': "goodUntil",
                    'blockchains': ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom", "base"],
                    'type': 'clipper'
                },
                'clipperSwapTo': {
                    'src_token_address': "srcToken",
                    'dst_token_address': "dstToken",
                    'dst_receiver': "recipient",
                    'src_amount': "inputAmount",
                    'dst_amount': 'output_returnAmount',
                    'dst_amount_min': "goodUntil",
                    'blockchains': ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom", "base"],
                    'type': 'clipper'
                },
                'clipperSwapToWithPermit': {
                    'src_token_address': "srcToken",
                    'dst_token_address': "dstToken",
                    'dst_receiver': "recipient",
                    'src_amount': "inputAmount",
                    'dst_amount': 'output_returnAmount',
                    'dst_amount_min': "goodUntil",
                    'blockchains': ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom", "base"],
                    'type': 'clipper'
                },
                'unoswap': {
                    'pools': "pools",
                    'src_token_address': "srcToken",
                    'src_amount': "amount",
                    'dst_amount': "output_returnAmount",
                    'dst_amount_min': "minReturn",
                    'blockchains': ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom", "base"],
                    'type': 'unoswap'
                },
                'unoswapTo': {
                    'pools': "pools",
                    'src_token_address': "srcToken",
                    'dst_receiver': "recipient",
                    'src_amount': "amount",
                    'dst_amount': "output_returnAmount",
                    'dst_amount_min': "minReturn",
                    'blockchains': ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom", "base"],
                    'type': 'unoswap'
                },
                'unoswapToWithPermit': {
                    'pools': "pools",
                    'src_token_address': "srcToken",
                    'dst_receiver': "recipient",
                    'src_amount': "amount",
                    'dst_amount': "output_returnAmount",
                    'dst_amount_min': "minReturn",
                    'blockchains': ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom", "base"],
                    'type': 'unoswap'
                },
                'uniswapV3Swap': {
                    'pools': "pools",
                    'src_amount': "amount",
                    'dst_amount': "output_returnAmount",
                    'dst_amount_min': "minReturn",
                    'blockchains': ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom", "base"],
                    'type': 'unoswap'
                },
                'uniswapV3SwapTo': {
                    'pools': "pools",
                    'dst_receiver': "recipient",
                    'src_amount': "amount",
                    'dst_amount': "output_returnAmount",
                    'dst_amount_min': "minReturn",
                    'blockchains': ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom", "base"],
                    'type': 'unoswap'
                },
                'uniswapV3SwapToWithPermit': {
                    'pools': "pools",
                    'dst_receiver': "recipient",
                    'src_amount': "amount",
                    'dst_amount': "output_returnAmount",
                    'dst_amount_min': "minReturn",
                    'blockchains': ["ethereum", "bnb", "polygon", "arbitrum", "optimism", "avalanche_c", "gnosis", "fantom", "base"],
                    'type': 'unoswap'
                }
            }
        }
    }
%}



with

pools as (
    select
        substr(data, if(topic3 is null, 13, 45), 20) as pool_address
        , substr(topic1, 13) as token0
        , substr(topic2, 13) as token1
    from {{ source(blockchain, 'logs') }}
    where topic0 in (
              0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9 -- PairCreated -- uniswapV2
            , 0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118 -- PoolCreated -- uniswapV3
        )
    group by 1, 2, 3
)

, calls as (
    {% for contract, contract_data in contracts.items() if blockchain in contract_data.blockchains %}
    select *
    from (
    {% for method, method_data in contract_data.methods.items() if blockchain in method_data.blockchains %}
    {% if method_data.type in ['generic', 'clipper'] %}
        select *
        from (
            select
                call_block_number as block_number
                , call_block_time as block_time
                , call_tx_hash as tx_hash
                , '{{ contract }}' as contract_name
                , '{{ contract_data['version'] }}' as protocol_version
                , '{{ method }}' as method
                , call_from
                , contract_address as call_to
                , call_trace_address
                , call_success
                , call_selector
                , {{ method_data.get("src_token_address", "null") }} as src_token_address
                , {{ method_data.get("dst_token_address", "null") }} as dst_token_address
                , {{ method_data.get("src_receiver", "null") }} as src_receiver
                , {{ method_data.get("dst_receiver", "null") }} as dst_receiver
                , {{ method_data.get("src_amount", "null") }} as src_amount
                , {{ method_data.get("dst_amount", "null") }} as dst_amount
                , {{ method_data.get("dst_amount_min", "null") }} as dst_amount_min
                , call_gas_used
                , call_output
                , null as ordinary
                , null as pools
                , remains
                , '{{ method_data.type }}' as router
            from (
                select *, {{ method_data.get("kit", "null") }} as kit
                from {{ source('oneinch_' + blockchain, contract + '_call_' + method) }}
                {% if is_incremental() %} 
                    where {{ incremental_predicate('call_block_time') }}
                {% endif %}
            )
            join (
                select
                    block_number as call_block_number
                    , tx_hash as call_tx_hash
                    , trace_address as call_trace_address
                    , "from" as call_from
                    , substr(input, 1, 4) as call_selector
                    , gas_used as call_gas_used
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
            ) using(call_block_number, call_tx_hash, call_trace_address)
        )
    {% elif method_data.type in ['unoswap'] %}
        select
            block_number
            , block_time
            , tx_hash
            , '{{ contract }}' as contract_name
            , '{{ contract_data['version'] }}' as protocol_version
            , '{{ method }}' as method
            , call_from
            , call_to
            , call_trace_address
            , call_success
            , call_selector
            , if(src_token_address is null, if(first_direction = 0, first_token0, first_token1), src_token_address) as src_token_address
            , if(last_direction is null, if(first_direction = 0, first_token1, first_token0), if(last_direction = 0, last_token1, last_token0)) as dst_token_address
            , src_receiver
            , dst_receiver
            , src_amount
            , dst_amount
            , dst_amount_min
            , call_gas_used
            , call_output
            , ordinary
            , pools
            , remains
            , '{{ method_data.type }}' as router
        from (
            select
                call_block_number as block_number
                , call_block_time as block_time
                , call_tx_hash as tx_hash
                , call_from
                , contract_address as call_to
                , call_trace_address
                , call_success
                , call_selector
                , {{ method_data.get("src_token_address", "null") }} as src_token_address
                , {{ method_data.get("dst_token_address", "null") }} as dst_token_address
                , {{ method_data.get("src_receiver", "null") }} as src_receiver
                , {{ method_data.get("dst_receiver", "null") }} as dst_receiver
                , {{ method_data.get("src_amount", "null") }} as src_amount
                , {{ method_data.get("dst_amount", "null") }} as dst_amount
                , {{ method_data.get("dst_amount_min", "null") }} as dst_amount_min
                , call_gas_used
                , call_output
                , if(cardinality(poolss) > 0, true, false) as ordinary
                , if(cardinality(poolss) > 0
                    , try(substr(cast(poolss[1] as varbinary), 13))
                    , substr(call_input, call_input_length - 20 - mod(call_input_length - 4, 32) + 1, 20)
                ) as first_pool
                , if(cardinality(poolss) > 1
                    , try(substr(cast(poolss[cardinality(poolss)] as varbinary), 13))
                ) as last_pool
                , if(cardinality(poolss) > 0
                    , try(cast(substr(to_base(bytearray_to_bigint(substr(cast(poolss[1] as varbinary), 1, 1)), 2), 1, 1) as int))
                    , try(cast(substr(to_base(bytearray_to_bigint(substr(call_input, call_input_length - 32 - mod(call_input_length - 4, 32) + 1, 1)), 2), 1, 1) as int))
                ) as first_direction
                , if(cardinality(poolss) > 1
                    , try(cast(substr(to_base(bytearray_to_bigint(substr(cast(poolss[cardinality(poolss)] as varbinary), 1, 1)), 2), 1, 1) as int))
                ) as last_direction
                , if(cardinality(poolss) > 0
                    , transform(poolss, x -> cast(x as varbinary))
                    , array[substr(call_input, call_input_length - 32 - mod(call_input_length - 4, 32) + 1, 32)]
                ) as pools
                , remains
            from (
                select *, {{ method_data["pools"] }} as poolss
                from {{ source('oneinch_' + blockchain, contract + '_call_' + method) }}
                {% if is_incremental() %}
                    where {{ incremental_predicate('call_block_time') }}
                {% endif %}
            )
            join (
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
            ) using(call_block_number, call_tx_hash, call_trace_address)
        )
        left join (select pool_address as first_pool, token0 as first_token0, token1 as first_token1 from pools) using(first_pool)
        left join (select pool_address as last_pool, token0 as last_token0, token1 as last_token1 from pools) using(last_pool)
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
    , router
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