{% macro uniswap_v3_pool_mass_decoding(
    logs = null
    )
%}


SELECT 
block_time 
,block_number
,to
,contract_address
,amount0Out
,amount0In
,amount1Out
,amount1In
,block_date
,tx_hash
,index
FROM TABLE (
    decode_evm_event (
      abi => '{
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
    }',
      input => TABLE (
        SELECT l.* 
        FROM {{logs}} l
        WHERE topic0 = 0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822
        and block_date > (Select min(block_date) from {{logs}} where topic0 = 0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822)
      )
    )
  )

{% endmacro %}