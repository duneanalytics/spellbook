{{ config(
      alias = 'top_holders',
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
        SUM(amount) as balance 
    FROM 
    {{ ref('balances_ethereum_erc1155_latest') }}
    GROUP BY 1, 2
), 

erc1155_ranked as (
    SELECT 
        wallet_address, 
        nft_contract_address, 
        balance, 
        SUM(balance) OVER (PARTITION BY nft_contract_address) as total_supply, 
        ROW_NUMBER() OVER (PARTITION BY nft_contract_address ORDER BY balance) as rn 
    FROM 
    erc1155_balances
    QUALIFY rn <= 50 
), 

erc721_balances as (
    SELECT 
        wallet_address,
        token_address as nft_contract_address,
        COUNT(*) as balance 
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
        ROW_NUMBER() OVER (PARTITION BY nft_contract_address ORDER BY balance) as rn 
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

UNION ALL 

SELECT 
    wallet_address,
    nft_contract_address, 
    balance, 
    balance/total_supply as supply_share,
    total_supply, 
    rn 
FROM 
erc1155_ranked
