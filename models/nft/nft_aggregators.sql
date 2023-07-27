{{ config(
	tags=['legacy'],
        alias = alias('aggregators', legacy_model=True),
        post_hook='{{ expose_spells(\'["avalanche_c","bnb","ethereum","polygon", "optimism"]\',
                                    "sector",
                                    "nft",
                                    \'["soispoke","hildobby", "chuxin"]\') }}')
}}

SELECT 'avalanche_c' as blockchain, * FROM  {{ ref('nft_avalanche_c_aggregators_legacy') }}
UNION ALL
SELECT 'bnb' as blockchain, * FROM  {{ ref('nft_bnb_aggregators_legacy') }}
UNION ALL
SELECT 'ethereum' as blockchain, * FROM  {{ ref('nft_ethereum_aggregators_legacy') }}
UNION ALL
SELECT 'polygon' as blockchain, * FROM  {{ ref('nft_polygon_aggregators_legacy') }}
UNION ALL
SELECT 'optimism' as blockchain, * FROM  {{ ref('nft_optimism_aggregators_legacy') }}
UNION ALL
SELECT 'arbitrum' as blockchain, * FROM  {{ ref('nft_arbitrum_aggregators_legacy') }}
