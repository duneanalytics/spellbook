{% macro uniswap_v3_factory_mass_decoding(
    logs = null
    )
%}


SELECT
token0
,token1
,pair
,contract_address
,block_time
,block_number
,block_date
,tx_hash
,index

-- could grab abi from our database instead to make this more dynamic
FROM TABLE (
    decode_evm_event (
      abi => '{
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
      input => TABLE (
        SELECT l.* 
        FROM {{logs}} l
        WHERE topic0 = 0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9
        and block_date > (Select min(block_date) from {{logs}} where topic0 = 0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9) -- take out limit if you want to use in prod
      )
    )
  )

{% endmacro %}