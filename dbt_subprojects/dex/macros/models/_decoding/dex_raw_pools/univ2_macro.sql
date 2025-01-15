{% macro univ2_macro(blockchain, logs) %}

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

with t as (
{% for topic0, abi in abi_config.items() %}
    select 
        '{{ blockchain }}' as blockchain
        , 'uniswap_compatible' as type
        , '2' as version
        , pair as pool
        , token0
        , token1
        , {% if 'swapFee' in abi %} cast(swapFee as uint256) {% else %} uint256 '3000' {% endif %} as fee
        , block_number
        , block_time
        , contract_address
        , tx_hash
    from ({{ evm_event_decoding_base(logs, abi, topic0) }})
    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}
)

select * from t 
limit 2000

{% endmacro %}