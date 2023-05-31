{{ config(
        alias ='blocks',
        unique_key=['blockchain', 'number'],
        post_hook='{{ expose_spells_hide_trino(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum"]\',
                                    "sector",
                                    "evms",
                                    \'["hildobby"]\') }}'
        )
}}

SELECT 'ethereum' AS blockchain
, hash
, miner
, nonce
, parent_hash
, size
, time
, CAST(total_difficulty AS DECIMAL(38, 0)) AS total_difficulty
, number
, base_fee_per_gas
, CAST(difficulty AS DECIMAL(38,0)) AS difficulty
, gas_limit
, gas_used
FROM {{source('ethereum', 'blocks')}}

UNION ALL

SELECT 'polygon' AS blockchain
, hash
, miner
, nonce
, parent_hash
, size
, time
, CAST(total_difficulty AS DECIMAL(38, 0)) AS total_difficulty
, number
, base_fee_per_gas
, CAST(difficulty AS DECIMAL(38,0)) AS difficulty
, gas_limit
, gas_used
FROM {{ source('polygon', 'blocks') }}

UNION ALL

SELECT 'bnb' AS blockchain
, hash
, miner
, nonce
, parent_hash
, size
, time
, CAST(total_difficulty AS DECIMAL(38, 0)) AS total_difficulty
, number
, base_fee_per_gas
, CAST(difficulty AS DECIMAL(38,0)) AS difficulty
, gas_limit
, gas_used
FROM {{ source('bnb', 'blocks') }}

UNION ALL

SELECT 'avalanche_c' AS blockchain
, hash
, miner
, nonce
, parent_hash
, size
, time
, CAST(total_difficulty AS DECIMAL(38, 0)) AS total_difficulty
, number
, base_fee_per_gas
, CAST(difficulty AS DECIMAL(38,0)) AS difficulty
, gas_limit
, gas_used
FROM {{ source('avalanche_c', 'blocks') }}

UNION ALL

SELECT 'gnosis' AS blockchain
, hash
, miner
, nonce
, parent_hash
, size
, time
, CAST(total_difficulty AS DECIMAL(38, 0)) AS total_difficulty
, number
, base_fee_per_gas
, CAST(difficulty AS DECIMAL(38,0)) AS difficulty
, gas_limit
, gas_used
FROM {{ source('gnosis', 'blocks') }}

UNION ALL

SELECT 'fantom' AS blockchain
, hash
, miner
, nonce
, parent_hash
, size
, time
, CAST(total_difficulty AS DECIMAL(38, 0)) AS total_difficulty
, number
, base_fee_per_gas
, CAST(difficulty AS DECIMAL(38,0)) AS difficulty
, gas_limit
, gas_used
FROM {{ source('fantom', 'blocks') }}

UNION ALL

SELECT 'optimism' AS blockchain
, hash
, miner
, nonce
, parent_hash
, size
, time
, CAST(total_difficulty AS DECIMAL(38, 0)) AS total_difficulty
, number
, base_fee_per_gas
, CAST(difficulty AS DECIMAL(38,0)) AS difficulty
, gas_limit
, gas_used
FROM {{ source('optimism', 'blocks') }}

UNION ALL

SELECT 'arbitrum' AS blockchain
, hash
, miner
, nonce
, parent_hash
, size
, time
, CAST(total_difficulty AS DECIMAL(38, 0)) AS total_difficulty
, number
, base_fee_per_gas
, CAST(difficulty AS DECIMAL(38,0)) AS difficulty
, gas_limit
, gas_used
FROM {{ source('arbitrum', 'blocks') }}