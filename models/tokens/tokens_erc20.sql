{{ config( alias='erc20')}}

SELECT 'avalanche_c' as blockchain, * FROM  {{ ref('tokens_avalanche_c_erc20') }}
UNION
SELECT 'bnb' as blockchain, * FROM  {{ ref('tokens_bnb_bep20') }}
UNION
SELECT 'ethereum' as blockchain, * FROM  {{ ref('tokens_ethereum_erc20') }}
UNION
SELECT 'gnosis' as blockchain, * FROM  {{ ref('tokens_gnosis_erc20') }}
UNION
SELECT 'optimism' as blockchain, * FROM  {{ ref('tokens_optimism_erc20') }}