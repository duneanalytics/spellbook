{{ config(
    alias = 'wallet_pnl',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "nft",
                                \'["Henrystats"]\') }}'
    )
}}

WITH 

trades as (
    SELECT 
        seller as wallet, 
        nft_contract_address, 
        'Sells' as trade_type,
        SUM(amount_original) as eth_amount, 
        COUNT(*) as trades 
    FROM 
    {{ ref('nft_trades') }}
    WHERE currency_symbol IN ('ETH', 'WETH')
    AND blockchain = 'ethereum'
    AND buyer != seller 
    AND trade_type = 'Single Item Trade'
    AND amount_original IS NOT NULL 
    GROUP BY 1, 2, 3

    UNION ALL 

    SELECT 
        buyer as wallet, 
        nft_contract_address, 
        'Buys' as trade_type,
        -1 * SUM(amount_original) as eth_amount,
        COUNT(*) as trades 
    FROM 
    {{ ref('nft_trades') }}
    WHERE currency_symbol IN ('ETH', 'WETH')
    AND blockchain = 'ethereum'
    AND buyer != seller 
    AND trade_type = 'Single Item Trade'
    AND amount_original IS NOT NULL 
    GROUP BY 1, 2, 3 
), 

combined as (
    SELECT 
        wallet, 
        nft_contract_address, 
        COALESCE(SUM(CASE 
            WHEN trade_type = 'Buys'
            THEN ABS(eth_amount) 
            ELSE 0 
        END), 0) as eth_spent, 
        COALESCE(SUM(CASE 
            WHEN trade_type = 'Sells'
            THEN eth_amount
            ELSE 0 
        END), 0) as eth_received, 
        SUM(eth_amount) as pnl, 
        SUM(trades) as trades 
    FROM 
    trades 
    GROUP BY 1, 2
)

SELECT 
    ROW_NUMBER() OVER (PARTITION BY nft_contract_address ORDER BY pnl DESC) as index, 
    * 
FROM 
combined
QUALIFY index <= 50
;