{{config(alias='balances_erc20')}}

WITH erc20_balances AS 
(SELECT
    'ethereum' as blockchain,
    wallet_address as address,
    amount_usd as latest_balance_usd
FROM {{ ref('balances_ethereum_erc20_latest') }})

SELECT 
    collect_set(blockchain) as blockchain,
    address,
    latest_balance_usd,
    'balances' AS category,
    'soispoke' AS contributor,
    'query' AS source,
    timestamp('2022-10-04') as created_at,
    now() as updated_at
FROM erc20_balances
GROUP BY address, latest_balance_usd