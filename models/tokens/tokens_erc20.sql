{{ config( alias = 'erc20',
        tags=['static'],
        post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum","optimism", "gnosis", "fantom", "polygon","base", "celo"]\',
                                    "sector",
                                    "tokens",
                                    \'["0xManny","hildobby","soispoke","dot2dotseurat","mtitus6","wuligy"]\') }}')}}

SELECT 'arbitrum' as blockchain, contract_address, symbol, decimals FROM  {{ ref('tokens_arbitrum_erc20') }}
UNION ALL
SELECT 'avalanche_c' as blockchain, contract_address, symbol, decimals  FROM  {{ ref('tokens_avalanche_c_erc20') }}
UNION ALL
SELECT 'bnb' as blockchain, contract_address, symbol, decimals FROM  {{ ref('tokens_bnb_bep20') }}
UNION ALL
SELECT 'ethereum' as blockchain, contract_address, symbol, decimals FROM  {{ ref('tokens_ethereum_erc20') }}
UNION ALL
SELECT 'gnosis' as blockchain, contract_address, symbol, decimals FROM  {{ ref('tokens_gnosis_erc20') }}
UNION ALL
-- Optimism adds extra fields and pulls in all ERC20 tokens (regardless of if mapping is known). So we curate here to match other chains.
SELECT 'optimism' as blockchain, contract_address, symbol, decimals FROM  {{ ref('tokens_optimism_erc20') }}
        WHERE symbol IS NOT NULL --This can be removed if/when all other chains show all ERC20 tokens, rather than only mapped ones.
UNION ALL
SELECT 'polygon' as blockchain, contract_address, symbol, decimals FROM  {{ ref('tokens_polygon_erc20') }}
UNION ALL
SELECT 'fantom' as blockchain, contract_address, symbol, decimals FROM {{ ref('tokens_fantom_erc20') }}
UNION ALL
SELECT 'base' as blockchain, contract_address, symbol, decimals FROM {{ ref('tokens_base_erc20') }}
UNION ALL
SELECT 'celo' as blockchain, contract_address, symbol, decimals FROM {{ ref('tokens_celo_erc20') }}