{{ config(
     alias = 'top_sales',
     post_hook='{{ expose_spells(\'["ethereum"]\',
                                 "sector",
                                 "nft",
                                 \'["Henrystats"]\') }}'
     )
 }}

WITH 

sales as (
    SELECT 
        nft_contract_address, 
        nft_token_id, 
        seller, 
        amount_original as price, 
        tx_hash, 
        ROW_NUMBER() OVER (PARTITION BY nft_contract_address ORDER BY amount_original DESC) as rn 
    FROM 
    {{ ref('nft_trades') }}
    WHERE blockchain = 'ethereum'
    AND currency_symbol IN ('ETH', 'WETH')
    QUALIFY rn <= 50 
)

SELECT 
    nft_contract_address, 
    nft_token_id,
    seller, 
    price, 
    tx_hash, 
    rn 
FROM 
sales 
ORDER BY rn DESC 