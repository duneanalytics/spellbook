{{ config(
	    tags=['legacy'],
        alias = alias('top_erc721_holders', legacy_model=True),
        materialized='view',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                   "sector",
                                   "nft",
                                   \'["Henrystats"]\') }}'
       )
   }}

WITH 

erc721_balances as (
    SELECT 
        wallet_address,
        token_address as nft_contract_address,
        COUNT(tokenId) as balance 
    FROM 
    {{ ref('balances_ethereum_erc721_latest_legacy') }}
    GROUP BY 1, 2
), 

total_supply as (
    SELECT 
        wallet_address, 
        nft_contract_address, 
        balance, 
        SUM(balance) OVER (PARTITION BY nft_contract_address) as total_supply
    FROM 
    erc721_balances
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