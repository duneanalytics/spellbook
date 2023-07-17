{{ config(
       alias = alias('top_erc1155_holders'),
       materialized='table',
       post_hook='{{ expose_spells(\'["ethereum"]\',
                                   "sector",
                                   "nft",
                                   \'["Henrystats"]\') }}'
       )
   }}

WITH 

erc1155_balances as (
    SELECT 
        wallet_address,
        token_address as nft_contract_address,
        COUNT(tokenId) as balance 
    FROM 
    {{ ref('balances_ethereum_erc1155_latest') }}
    GROUP BY 1, 2
), 

total_supply as (
    SELECT 
        wallet_address, 
        nft_contract_address, 
        balance, 
        SUM(balance) OVER (PARTITION BY nft_contract_address) as total_supply
    FROM 
    erc1155_balances
)

SELECT 
    * 
FROM 
(
 SELECT 
     wallet_address,
     nft_contract_address, 
     balance, 
     balance/total_supply as supply_share,
     total_supply, 
    ROW_NUMBER() OVER (PARTITION BY nft_contract_address ORDER BY balance DESC) as rn
 FROM 
 total_supply
) x 
WHERE rn <= 50 