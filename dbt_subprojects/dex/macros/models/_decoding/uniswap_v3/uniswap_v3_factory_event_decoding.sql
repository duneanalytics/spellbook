{% macro uniswap_v3_factory_event_decoding(logs) %}

{%- 
    set abi_config = {
        '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118': {
            'info': 'original uniswap v3 factory event',
            'abi': '{
                "name": "PoolCreated",
                "type": "event",
                "inputs": [
                    {
                        "name": "token0",
                        "type": "address", 
                        "indexed": true,
                        "internalType": "address"
                    },
                    {
                        "name": "token1",
                        "type": "address",
                        "indexed": true,
                        "internalType": "address"
                    },
                    {
                        "name": "fee",
                        "type": "uint24",
                        "indexed": true,
                        "internalType": "uint24"
                    },
                    {
                        "name": "tickSpacing",
                        "type": "int24",
                        "indexed": false,
                        "internalType": "int24"
                    },
                    {
                        "name": "pool",
                        "type": "address",
                        "indexed": false,
                        "internalType": "address"
                    }
                ],
                "anonymous": false
            }'
        },

        '0xab0d57f0df537bb25e80245ef7748fa62353808c54d6e528a9dd20887aed9ac2': {
            'info': 'solidly_v2, aerodrome, velodrome factory event',
            'abi': '{
                "name": "PoolCreated",
                "type": "event",
                "inputs": [
                    {
                        "name": "token0",
                        "type": "address",
                        "indexed": true,
                        "internalType": "address"
                    },
                    {
                        "name": "token1",
                        "type": "address",
                        "indexed": true,
                        "internalType": "address"
                    },
                    {
                        "name": "tickSpacing",
                        "type": "int24",
                        "indexed": true,
                        "internalType": "int24"
                    },
                    {
                        "name": "pool",
                        "type": "address",
                        "indexed": false,
                        "internalType": "address"
                    }
                ],
                "anonymous": false
            }'
        }
    }
-%}

{%- for topic0, config in abi_config.items() -%}
    {%- if not loop.first %}
    union all 
    {%- endif %}
    select 
        token0
        , token1
        , pool
        , contract_address
        , topic0 as factory_topic0
        , '{{ config['info'] }}' as factory_info
        , block_time
        , block_date
        , block_month
        , block_number
        , tx_hash
        , tx_from
        , tx_to
        , tx_index
        , evt_index
    from (
        {{ evm_event_decoding_base(logs, config.abi, topic0) }}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
        {% endif %}
    )
{%- endfor -%}

{% endmacro %}

