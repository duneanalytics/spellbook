{{  
    config(
        schema = 'oneinch',
        alias = alias('lop'),
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address'],
        tags = ['dunesql'],
    )
}}

{% set lookback_days = -7 %}

{%
    set cfg = {
        'LimitOrderProtocolV1': {
            'version': '"1"'
            , 'blockchains': ['ethereum', 'bnb', 'polygon', 'arbitrum', 'optimism']
            , 'methods': {
                'fillOrder': {'empty': 'empty'
                    , 'maker': 'substr(order_map["makerAssetData"], 4 + 12 + 1, 20)'
                }
                , 'fillOrderRFQ': {'empty': 'empty'
                    , 'maker': 'substr(order_map["makerAssetData"], 4 + 12 + 1, 20)'
                    , 'making_amount': 'null'
                    , 'taking_amount': 'null'
                }
            }
        }
        , 'LimitOrderProtocolV2': {
            'version': '"2"'
            , 'blockchains': ['ethereum', 'bnb', 'polygon', 'arbitrum', 'avalanche_c', 'gnosis', 'optimism']
            , 'methods': {
                'fillOrder': {'empty': 'empty'}
                , 'fillOrderTo': {'empty': 'empty'}
                , 'fillOrderToWithPermit': {'empty': 'empty'}
                , 'fillOrderRFQ': {'empty': 'empty'}
                , 'fillOrderRFQTo': {'empty': 'empty'}
                , 'fillOrderRFQToWithPermit': {'empty': 'empty'}
            }
        }
        , 'AggregationRouterV4': {
            'version': '"2"'
            , 'blockchains': ['ethereum', 'bnb', 'polygon', 'arbitrum', 'avalanche_c', 'gnosis', 'optimism', 'fantom']
            , 'methods': {
                'fillOrderRFQ': {'empty': 'empty'}
                , 'fillOrderRFQTo': {'empty': 'empty'}
                , 'fillOrderRFQToWithPermit': {'empty': 'empty'}
            }
        }
        , 'AggregationRouterV5': {
            'version': '"3"'
            , 'blockchains': ['ethereum', 'bnb', 'polygon', 'arbitrum', 'avalanche_c', 'gnosis', 'optimism', 'fantom', 'base']
            , 'methods': {
                'fillOrder': {'order_hash': 'output_3'}
                , 'fillOrderTo': {'empty': 'empty'
                    , 'order': '"order_"'
                    , 'making_amount': 'output_actualMakingAmount'
                    , 'taking_amount': 'output_actualTakingAmount'
                    , 'order_hash': 'output_orderHash'
                }
                , 'fillOrderToWithPermit': {'order_hash': 'output_3'}
                , 'fillOrderRFQ': {'order_hash': 'output_3'}
                , 'fillOrderRFQTo': {'empty': 'empty'
                    , 'making_amount': 'output_filledMakingAmount'
                    , 'taking_amount': 'output_filledTakingAmount'
                    , 'order_hash': 'output_orderHash'
                }
                , 'fillOrderRFQToWithPermit': {'order_hash': 'output_3'}
                , 'fillOrderRFQCompact': {'empty': 'empty'
                    , 'making_amount': 'output_filledMakingAmount'
                    , 'taking_amount': 'output_filledTakingAmount'
                    , 'order_hash': 'output_orderHash'
                }
            }
        }
    }
%}

with

    orders as (

        {% for contract, contract_data in cfg.items() %}
            {% for blockchain in contract_data.blockchains %}
                {% for method, method_data in contract_data.methods.items() %}
                

                    select
                        -- block
                        {{ blockchain }} as blockchain
                        , transactions.block_time
                        
                        -- tx
                        , hash as tx_hash
                        , transactions."from" as tx_from
                        , transactions."to" as tx_to
                        , transactions.success as tx_success

                        -- contract & method
                        , {{ contract }} as contract_name
                        , {{ contract_data['version'] }} as protocol_version
                        , {{ method }} as method

                        -- call
                        , traces."from" as call_from
                        , orders.contract_address as call_to
                        , trace_address as call_trace_address
                        , substr(traces.input, 1, 4) as call_selector
                        , {% if 'maker' in method_data.keys() %} {{ method_data['maker'] }} {% else %} order_map['maker'] {% endif %} as maker
                        , from_hex(order_map['makerAsset']) as maker_asset
                        , if(orders.making_amount is null, bytearray_to_uint256(substr(order_map['makerAssetData'], 4 + 32*2 + 1, 32)), orders.making_amount) as making_amount
                        , from_hex(order_map['takerAsset']) as taker_asset
                        , if(orders.taking_amount is null, bytearray_to_uint256(substr(order_map['takerAssetData'], 4 + 32*2 + 1, 32)), orders.taking_amount) as taking_amount
                        , orders.order_hash as order_hash
                        , traces.success as call_success
                        , traces.gas_used as call_gas_used
                        , traces.input as call_input
                        , traces.output as call_output

                        -- ext
                        , date_trunc('minute', transactions.block_time) as minute
                    from (
                        select
                            call_tx_hash as tx_hash
                            , contract_address
                            , call_trace_address as trace_address
                            , {% if 'making_amount' in method_data.keys() %} {{ method_data['making_amount'] }} {% else %} output_0 {% endif %} as making_amount
                            , {% if 'taking_amount' in method_data.keys() %} {{ method_data['taking_amount'] }} {% else %} output_1 {% endif %} as taking_amount
                            , {% if 'order_hash' in method_data.keys() %} {{ method_data['order_hash'] }} {% else %} null {% endif %} as order_hash
                            , cast(json_parse({% if 'order' in method_data.keys() %} {{ method_data['order'] }} {% else %} "order" {% endif %}) as map(varchar, varchar)) as order_map
                        from {{ source('oneinch_' + blockchain, contract + '_call_' + method) }}
                        {% if is_incremental() %}
                            where call_block_time >= cast(date_add('day', {{ lookback_days }}, current_timestamp) as timestamp)
                        {% endif %}
                    ) as orders
                    join {{ source(row.blockchain, 'transactions') }} on transactions.hash = orders.tx_hash
                    join {{ source(row.blockchain, 'traces') }} using(tx_hash, trace_address)

                    {% if not loop.last %} union all {% endif %}
                    
                {% endfor %}
            {% endfor %}
        {% endfor %}

    )

select *
from orders