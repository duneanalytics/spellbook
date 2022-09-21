{{ config( alias='nft',
        post_hook='{{ expose_spells(\'["avalanche_c","bnb","ethereum","optimism", "gnosis"]\',
                                    "sector",
                                    "tokens",
                                    \'["0xManny","hildobby","soispoke","dot2dotseurat"]\') }}')}}

SELECT 
'avalanche_c' as blockchain, 
contract_address, 
name, 
symbol,
standard, 
category 
FROM  {{ ref('tokens_avalanche_c_nft') }}
            UNION
SELECT
'ethereum' as blockchain, 
contract_address, 
name, 
symbol,
standard, 
category 
FROM  {{ ref('tokens_ethereum_nft') }}
            UNION
SELECT
'gnosis' as blockchain, 
contract_address, 
name, 
symbol,
standard, 
CAST(NULL as STRING) as category 
FROM  {{ ref('tokens_gnosis_nft') }}
            UNION
SELECT
'optimism' as blockchain, 
contract_address, 
name, 
CAST(NULL as STRING) as symbol,
CAST(NULL as STRING) as standard, 
CAST(NULL as STRING) as category 
FROM  {{ ref('tokens_optimism_nft') }}
            UNION
SELECT
'bnb' as blockchain, 
contract_address, 
name, 
CAST(NULL as STRING) as symbol,
standard, 
CAST(NULL as STRING) as category 
FROM  {{ ref('tokens_bnb_nft') }}
