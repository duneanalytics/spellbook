{% macro uniswap_v2_factory_event_decoding(logs) %}

{%- 
    set abi_config = {
        '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9': {
            'info': 'original uniswap v2 factory event',
            'abi': '{
                "anonymous": false,
                "inputs": [
                    {
                        "indexed": true,
                        "internalType": "address",
                        "name": "token0",
                        "type": "address"
                    },
                    {
                        "indexed": true,
                        "internalType": "address",
                        "name": "token1",
                        "type": "address"
                    },
                    {
                        "indexed": false,
                        "internalType": "address",
                        "name": "pair",
                        "type": "address"
                    },
                    {
                        "indexed": false,
                        "internalType": "uint256",
                        "name": "",
                        "type": "uint256"
                    }
                ],
                "name": "PairCreated",
                "type": "event"
            }'
        },

        '0x41f8736f924f57e464ededb08bf71f868f9d142885bbc73a1516db2be21fc428': {
            'info': 'weighted pool factory event',
            'abi': '{
                "anonymous": false,
                "inputs": [
                    {
                        "indexed": true,
                        "internalType": "address",
                        "name": "token0",
                        "type": "address"
                    },
                    {
                        "indexed": true,
                        "internalType": "address",
                        "name": "token1",
                        "type": "address"
                    },
                    {
                        "indexed": false,
                        "internalType": "address",
                        "name": "pair",
                        "type": "address"
                    },
                    {
                        "indexed": false,
                        "internalType": "uint32",
                        "name": "tokenWeight0",
                        "type": "uint32"
                    },
                    {
                        "indexed": false,
                        "internalType": "uint32",
                        "name": "swapFee",
                        "type": "uint32"
                    },
                    {
                        "indexed": false,
                        "internalType": "uint256",
                        "name": "",
                        "type": "uint256"
                    }
                ],
                "name": "PairCreated",
                "type": "event"
            }'
        },

        '0x3541d8fea55be35f686281f975bf8b7ab8fbb500c1c7ddd6c4e714655e9cd4e2': {
            'info': 'sushiswap trident, trader joe factory event',
            'abi': '{
                "anonymous": false,
                "inputs": [
                    {
                        "indexed": true,
                        "internalType": "address",
                        "name": "token0",
                        "type": "address"
                    },
                    {
                        "indexed": true,
                        "internalType": "address",
                        "name": "token1",
                        "type": "address"
                    },
                    {
                        "indexed": false,
                        "internalType": "address",
                        "name": "pair",
                        "type": "address"
                    },
                    {
                        "indexed": false,
                        "internalType": "uint32",
                        "name": "swapFee",
                        "type": "uint32"
                    },
                    {
                        "indexed": false,
                        "internalType": "uint256",
                        "name": "",
                        "type": "uint256"
                    }
                ],
                "name": "PairCreated",
                "type": "event"
            }'
        },

        '0xc4805696c66d7cf352fc1d6bb633ad5ee82f6cb577c453024b6e0eb8306c6fc9': {
            'info': 'solidly_v1 factory event',
            'abi': '{
                "anonymous": false,
                "inputs": [
                    {
                        "indexed": true,
                        "internalType": "address",
                        "name": "token0",
                        "type": "address"
                    },
                    {
                        "indexed": true,
                        "internalType": "address",
                        "name": "token1",
                        "type": "address"
                    },
                    {
                        "indexed": false,
                        "internalType": "bool",
                        "name": "stable",
                        "type": "bool"
                    },
                    {
                        "indexed": false,
                        "internalType": "address",
                        "name": "pair",
                        "type": "address"
                    },
                    {
                        "indexed": false,
                        "internalType": "uint256",
                        "name": "",
                        "type": "uint256"
                    }
                ],
                "name": "PairCreated",
                "type": "event"
            }'
        },

        '0x25bc54a32c894b07fd47ed3cc4296ec7d97a974e5ebd17c9f5163afddaf107fa': {
            'info': 'velodrome_v2, aerodrome_v2 factory event',
            'abi': '{
                "anonymous": false,
                "inputs": [
                    {
                        "indexed": true,
                        "internalType": "address",
                        "name": "token0",
                        "type": "address"
                    },
                    {
                        "indexed": true,
                        "internalType": "address",
                        "name": "token1",
                        "type": "address"
                    },
                    {
                        "indexed": false,
                        "internalType": "bool",
                        "name": "stable",
                        "type": "bool"
                    },
                    {
                        "indexed": false,
                        "internalType": "address",
                        "name": "pair",
                        "type": "address"
                    },
                    {
                        "indexed": false,
                        "internalType": "uint256",
                        "name": "",
                        "type": "uint256"
                    },
                    {
                        "indexed": false,
                        "internalType": "uint256",
                        "name": "fee",
                        "type": "uint256"
                    }
                ],
                "name": "PairCreated",
                "type": "event"
            }'
        }
    }
-%}

{%- for topic0, config in abi_config.items() -%}
    {%- if not loop.first %}
    union all 
    {%- endif %}
    select 
        pair
        , token0
        , token1
        , contract_address
        , topic0 as pool_topic0
        , '{{ config['info'] }}' as factory_info
        , topic0 as factory_topic0
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