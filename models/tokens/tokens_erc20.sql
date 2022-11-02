{{ config( alias='erc20',
        post_hook='{{ expose_spells(\'["arbitrum","avalanche_c","bnb","ethereum","optimism", "gnosis"]\',
                                    "sector",
                                    "tokens",
                                    \'["0xManny","hildobby","soispoke","dot2dotseurat","mtitus6"]\') }}')}}

SELECT 'arbitrum' as blockchain, * FROM  {{ ref('tokens_arbitrum_erc20') }}
UNION ALL
SELECT 'avalanche_c' as blockchain, * FROM  {{ ref('tokens_avalanche_c_erc20') }}
UNION ALL
SELECT 'bnb' as blockchain, * FROM  {{ ref('tokens_bnb_bep20') }}
UNION ALL
SELECT 'ethereum' as blockchain, * FROM  {{ ref('tokens_ethereum_erc20') }}
UNION ALL
SELECT 'gnosis' as blockchain, * FROM  {{ ref('tokens_gnosis_erc20') }}
UNION ALL
SELECT 'optimism' as blockchain, * FROM  {{ ref('tokens_optimism_erc20') }}