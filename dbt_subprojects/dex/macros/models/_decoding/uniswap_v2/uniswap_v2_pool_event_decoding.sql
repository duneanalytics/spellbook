{% macro uniswap_v2_pool_event_decoding(logs) %}

{% set abi = '{
    "anonymous": false,
    "inputs": [
        {
            "indexed": true,
            "internalType": "address",
            "name": "sender",
            "type": "address"
        },
        {
            "indexed": false,
            "internalType": "uint256",
            "name": "amount0In",
            "type": "uint256"
        },
        {
            "indexed": false,
            "internalType": "uint256",
            "name": "amount1In",
            "type": "uint256"
        },
        {
            "indexed": false,
            "internalType": "uint256",
            "name": "amount0Out",
            "type": "uint256"
        },
        {
            "indexed": false,
            "internalType": "uint256",
            "name": "amount1Out",
            "type": "uint256"
        },
        {
            "indexed": true,
            "internalType": "address",
            "name": "to",
            "type": "address"
        }
    ],
    "name": "Swap",
    "type": "event"
}' %}

{% set topic0 = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822' %}

{{ evm_event_decoding_base(logs, abi, topic0) }}
{% if is_incremental()  %}
WHERE {{ incremental_predicate('block_time') }}
{% endif %}
{% endmacro %}
