{{ config(
        alias ='aggregators'
)
}}

SELECT * FROM  {{ ref('nft_avalanche_c_aggregators') }}
UNION
SELECT * FROM  {{ ref('nft_bnb_aggregators') }}
UNION
SELECT * FROM  {{ ref('nft_ethereum_aggregators') }}
UNION
SELECT * FROM  {{ ref('nft_polygon_aggregators') }}
