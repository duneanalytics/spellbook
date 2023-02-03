{{ config(
      alias = 'top_minters',
      post_hook='{{ expose_spells(\'["ethereum"]\',
                                  "sector",
                                  "nft",
                                  \'["Henrystats"]\') }}'
      )
  }}

WITH 

src_data as (
    SELECT 
        nft_contract_address,
        buyer as minter, 
        SUM(amount_original) as eth_spent, 
        COUNT(*) as no_minted
    FROM 
    {{ ref('nft_mints') }}
    WHERE blockchain = 'ethereum'
    AND currency_symbol IN ('WETH', 'ETH')
    AND amount_original IS NOT NULL
    GROUP BY 1, 2
)

SELECT 
    ROW_NUMBER() OVER (PARTITION BY nft_contract_address ORDER BY no_minted DESC) as rank_,
    * 
FROM 
src_data
QUALIFY rank_ <= 50 