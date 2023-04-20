{{ config(
       alias = 'top_erc721_holders',
       materialized='table',
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
    {{ ref('balances_ethereum_erc721_latest') }}
    GROUP BY 1, 2
), 

erc721_ranked as (
    SELECT 
        wallet_address, 
        nft_contract_address, 
        balance, 
        SUM(balance) OVER (PARTITION BY nft_contract_address) as total_supply, 
        ROW_NUMBER() OVER (PARTITION BY nft_contract_address ORDER BY balance DESC) as rn 
    FROM 
    erc721_balances
    QUALIFY rn <= 50 
)

 SELECT 
     wallet_address,
     nft_contract_address, 
     balance, 
     balance/total_supply as supply_share,
     total_supply, 
     rn 
 FROM 
 erc721_ranked