{{ config(
        alias ='aggregators'
)
}}

SELECT 'avalanche_c' as blockchain, * FROM  {{ ref('nft_avalanche_c_aggregators') }}
UNION
SELECT 'bnb' as blockchain, * FROM  {{ ref('nft_bnb_aggregators') }}
UNION
SELECT 'ethereum' as blockchain, * FROM  {{ ref('nft_ethereum_aggregators') }}
UNION
SELECT 'polygon' as blockchain, * FROM  {{ ref('nft_polygon_aggregators') }}
