{% macro uniswap_v2_factory_event_decoding(logs) %}

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

{% endmacro %}
