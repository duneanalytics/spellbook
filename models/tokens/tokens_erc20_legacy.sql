{{ config( alias = alias('erc20', legacy_model=True),
        tags=['static'],
        post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum","optimism", "gnosis", "fantom", "polygon"]\',
                                    "sector",
                                    "tokens",
                                    \'["0xManny","hildobby","soispoke","dot2dotseurat","mtitus6"]\') }}')}}

SELECT 'arbitrum' as blockchain, * FROM  {{ ref('tokens_arbitrum_erc20_legacy') }}
UNION ALL
SELECT 'avalanche_c' as blockchain, * FROM  {{ ref('tokens_avalanche_c_erc20_legacy') }}
UNION ALL
SELECT 'bnb' as blockchain, * FROM  {{ ref('tokens_bnb_bep20_legacy') }}
UNION ALL
SELECT 'ethereum' as blockchain, * FROM  {{ ref('tokens_ethereum_erc20_legacy') }}
UNION ALL
SELECT 'gnosis' as blockchain, * FROM  {{ ref('tokens_gnosis_erc20_legacy') }}
UNION ALL
-- Optimism adds extra fields and pulls in all ERC20 tokens (regardless of if mapping is known). So we curate here to match other chains.
SELECT 'optimism' as blockchain, contract_address, symbol, decimals FROM  {{ ref('tokens_optimism_erc20_legacy') }}
        WHERE symbol IS NOT NULL --This can be removed if/when all other chains show all ERC20 tokens, rather than only mapped ones.
UNION ALL
SELECT 'polygon' as blockchain, * FROM  {{ ref('tokens_polygon_erc20_legacy') }}
UNION ALL
SELECT 'fantom' as blockchain, * FROM {{ ref('tokens_fantom_erc20_legacy') }}
