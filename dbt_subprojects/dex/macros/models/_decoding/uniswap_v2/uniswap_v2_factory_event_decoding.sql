/*

{% macro uniswap_v2_factory_event_decoding_old(logs) %}

{% set abi = '{
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
            "name": "pair_index",
            "type": "uint256"
        }
    ],
    "name": "PairCreated",
    "type": "event"
}' %}

{% set topic0 = '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9' %}

{{ evm_event_decoding_base(logs, abi, topic0) }} 
{% if is_incremental() %}
WHERE {{ incremental_predicate('block_time') }}
{% endif %}

{% endmacro %}
*/

{% macro uniswap_v2_factory_event_decoding(logs) %}

{% 
    set abi_config = {
        '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9': '{
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
        }',

        '0x41f8736f924f57e464ededb08bf71f868f9d142885bbc73a1516db2be21fc428': '{
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
        }',

        '0x3541d8fea55be35f686281f975bf8b7ab8fbb500c1c7ddd6c4e714655e9cd4e2': '{
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
} 
%}

{% for topic0, abi in abi_config.items() %}
    {% if not loop.first %}
    union all 
    {% endif %}
    select 
        pair
        , token0
        , token1
        , contract_address
        , block_time
        , block_date
        , block_number
        , tx_hash
        , tx_from
        , tx_to
        , tx_index
        , index
    from (
        {{ evm_event_decoding_base(logs, abi, topic0) }}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
        {% endif %}
    )
{% endfor %}

{% endmacro %}