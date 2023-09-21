{{ config(
    tags = ['dunesql'],
    schema = 'nft_ethereum',
    alias = alias('wallet_pnl'),
    materialized='incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['wallet', 'nft_contract_address'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "nft",
                                \'["Henrystats"]\') }}'
    )
}}

{% if is_incremental() %}
WITH weekly_unique_wallet_address as 
(
    SELECT DISTINCT
        nft_contract_address,
        buyer as wallet 
    FROM 
        {{ ref('nft_trades') }}
    WHERE 
        block_time >= date_trunc('day', now() - interval '7' Day)
        AND blockchain = 'ethereum'
        AND currency_symbol IN ('WETH', 'ETH')
        AND amount_original IS NOT NULL
        AND number_of_items = UINT256 '1'
        AND buyer != seller 
    
    UNION 

    SELECT DISTINCT
        nft_contract_address,
        seller as wallet 
    FROM 
        {{ ref('nft_trades') }}
    WHERE 
        block_time >= date_trunc('day', now() - interval '7' Day)
        AND blockchain = 'ethereum'
        AND currency_symbol IN ('WETH', 'ETH')
        AND amount_original IS NOT NULL
        AND number_of_items = UINT256 '1'
        AND buyer != seller 
)
, trades as
(
    --sells
    SELECT
        src.seller as wallet, 
        src.nft_contract_address, 
        'Sells' as trade_type,
        SUM(src.amount_original) as eth_amount, 
        COUNT(*) as trades, 
        MAX(src.block_time) as last_updated
    FROM 
        {{ ref('nft_trades') }} src 
    INNER JOIN 
        weekly_unique_wallet_address
        ON src.nft_contract_address = weekly_unique_wallet_address.nft_contract_address
        AND src.seller = weekly_unique_wallet_address.wallet
    WHERE
        src.currency_symbol IN ('ETH', 'WETH')
        AND src.blockchain = 'ethereum'
        AND src.buyer != src.seller 
        AND src.number_of_items = UINT256 '1'
        AND src.amount_original IS NOT NULL 
    GROUP BY
        1, 2, 3

    UNION ALL 

    --buys
    SELECT 
        src.buyer as wallet, 
        src.nft_contract_address, 
        'Buys' as trade_type,
        -1 * SUM(src.amount_original) as eth_amount,
        COUNT(*) as trades, 
        MAX(src.block_time) as last_updated
    FROM 
        {{ ref('nft_trades') }} src
    INNER JOIN 
        weekly_unique_wallet_address
        ON src.nft_contract_address = weekly_unique_wallet_address.nft_contract_address
        AND src.buyer = weekly_unique_wallet_address.wallet
    WHERE
        src.currency_symbol IN ('ETH', 'WETH')
        AND src.blockchain = 'ethereum'
        AND src.buyer != src.seller 
        AND src.number_of_items = UINT256 '1'
        AND src.amount_original IS NOT NULL 
    GROUP BY
        1, 2, 3
)
SELECT
    wallet, 
    nft_contract_address, 
    MAX(last_updated) as last_updated,
    COALESCE
    (
        SUM
        (
            CASE 
                WHEN trade_type = 'Buys'
                THEN ABS(eth_amount) 
                ELSE 0 
            END
        )
    , 0
    ) as eth_spent, 
    COALESCE
    (
        SUM
        (
            CASE 
                WHEN trade_type = 'Sells'
                THEN eth_amount
                ELSE 0 
            END
        )
    , 0
    ) as eth_received, 
    SUM(eth_amount) as pnl, 
    SUM(trades) as trades 
FROM 
    trades 
GROUP BY
    1, 2
{% else %}
WITH trades as
(
    --sells
    SELECT
        src.seller as wallet, 
        src.nft_contract_address, 
        'Sells' as trade_type,
        SUM(src.amount_original) as eth_amount, 
        COUNT(*) as trades, 
        MAX(src.block_time) as last_updated
    FROM 
        {{ ref('nft_trades') }} src 
    WHERE
        src.currency_symbol IN ('ETH', 'WETH')
        AND src.blockchain = 'ethereum'
        AND src.buyer != src.seller 
        AND src.number_of_items = UINT256 '1'
        AND src.amount_original IS NOT NULL 
    GROUP BY
        1, 2, 3

    UNION ALL 

    --buys
    SELECT 
        src.buyer as wallet, 
        src.nft_contract_address, 
        'Buys' as trade_type,
        -1 * SUM(src.amount_original) as eth_amount,
        COUNT(*) as trades, 
        MAX(src.block_time) as last_updated
    FROM 
        {{ ref('nft_trades') }} src
    WHERE
        src.currency_symbol IN ('ETH', 'WETH')
        AND src.blockchain = 'ethereum'
        AND src.buyer != src.seller 
        AND src.number_of_items = UINT256 '1'
        AND src.amount_original IS NOT NULL 
    GROUP BY
        1, 2, 3
)
SELECT
    wallet, 
    nft_contract_address, 
    MAX(last_updated) as last_updated,
    COALESCE
    (
        SUM
        (
            CASE 
                WHEN trade_type = 'Buys'
                THEN ABS(eth_amount) 
                ELSE 0 
            END
        )
    , 0
    ) as eth_spent, 
    COALESCE
    (
        SUM
        (
            CASE 
                WHEN trade_type = 'Sells'
                THEN eth_amount
                ELSE 0 
            END
        )
    , 0
    ) as eth_received, 
    SUM(eth_amount) as pnl, 
    SUM(trades) as trades 
FROM 
    trades 
GROUP BY
    1, 2
{% endif %}